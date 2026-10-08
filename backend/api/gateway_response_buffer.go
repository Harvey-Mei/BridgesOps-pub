package api

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	// ErrGatewayUpstreamResponseTooLarge means an upstream response crossed the
	// caller's explicit hard limit. It is deliberately distinguishable from a
	// transport or temporary-file failure so a dispatched request can be
	// finalized without attempting another upstream request.
	ErrGatewayUpstreamResponseTooLarge = errors.New("gateway upstream response exceeds configured limit")
	errGatewayResponseBufferInvalid    = errors.New("invalid gateway upstream response buffer options")
	errGatewayResponseSpoolCapacity    = errors.New("gateway upstream response spool capacity is exhausted")
	errGatewayResponseSpoolQueueFull   = errors.New("gateway upstream response spool queue is full")
	// errGatewayResponseSpoolQueueWaitCanceled preserves that a request was
	// already physically dispatched and then stopped while waiting for a local
	// spool-file admission slot. The wrapped context cause can be the ordinary
	// first-byte deadline, but it must not be mistaken for an upstream timeout:
	// a retry here could duplicate work that is still executing upstream.
	errGatewayResponseSpoolQueueWaitCanceled = errors.New("gateway upstream response spool queue wait was canceled")
	errGatewayResponseSpoolReleased          = errors.New("gateway upstream response spool admission lease is released")
)

const (
	gatewayUpstreamResponseSpoolMaxPrefixBytes int64 = 1 << 20
	gatewayUpstreamResponseSpoolFilePrefix           = "bridgeops-upstream-response-"
)

// readGatewayUpstreamResponseLimited reads small error and model-list payloads
// with a strict limit+1 check. A response exactly at limit is accepted; the
// extra byte exists only to prove that the limit was exceeded.
func readGatewayUpstreamResponseLimited(reader io.Reader, limit int64) ([]byte, error) {
	if reader == nil || limit < 0 {
		return nil, errGatewayResponseBufferInvalid
	}
	if limit == int64(^uint64(0)>>1) {
		return nil, fmt.Errorf("%w: limit is too large for limit+1", errGatewayResponseBufferInvalid)
	}
	payload, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(payload)) > limit {
		return nil, fmt.Errorf("%w: response exceeds %d bytes", ErrGatewayUpstreamResponseTooLarge, limit)
	}
	return payload, nil
}

// gatewayUpstreamResponseSpoolOptions controls a disk-backed successful
// response buffer. MaxBytes is mandatory and includes every persisted byte.
// PrefixBytes only controls the in-memory metadata window; response data is
// never logged by this package.
type gatewayUpstreamResponseSpoolOptions struct {
	AdmissionTimeout time.Duration
	TempDir          string
	MaxBytes         int64
	PrefixBytes      int64
	OnFirstByte      func()
	Admission        *gatewayUpstreamResponseSpoolAdmission
}

// gatewayUpstreamResponseSpool owns a private 0600 temporary file. It keeps at
// most PrefixBytes in memory, supports replay through Seek/Read or Open, and
// removes the file exactly once on Close. It is intentionally package-private:
// callers must decide which protocol fields may be extracted from Prefix.
type gatewayUpstreamResponseSpool struct {
	file   *os.File
	path   string
	size   int64
	prefix []byte
	lease  *gatewayUpstreamResponseSpoolAdmissionLease

	mu              sync.Mutex
	closed          bool
	unlinked        bool
	openReaders     int64
	removeErr       error
	admissionClosed bool
}

// gatewayUpstreamResponseSpoolAdmissionLimits bound disk-backed responses for
// one process. File acquisition is FIFO and may queue. Byte growth deliberately
// never queues: a partially written response already holds a file and some
// bytes, so waiting for another active response to release bytes can deadlock
// every upstream reader. A failed GrowTo terminates that response and Release
// immediately makes its reservation available to other requests.
type gatewayUpstreamResponseSpoolAdmissionLimits struct {
	MaxInFlightFiles int64
	MaxInFlightBytes int64
	MaxQueuedFiles   int64
}

type gatewayUpstreamResponseSpoolAdmission struct {
	mu sync.Mutex

	limits gatewayUpstreamResponseSpoolAdmissionLimits

	inFlightFiles int64
	inFlightBytes int64
	queuedFiles   int64
	waiters       list.List
}

type gatewayUpstreamResponseSpoolAdmissionWaiter struct {
	lease   *gatewayUpstreamResponseSpoolAdmissionLease
	ready   chan struct{}
	element *list.Element
	granted bool
}

type gatewayUpstreamResponseSpoolAdmissionLease struct {
	mu sync.Mutex

	admission *gatewayUpstreamResponseSpoolAdmission
	maxBytes  int64
	bytes     int64
	released  bool
}

type gatewayUpstreamResponseSpoolAdmissionSnapshot struct {
	InFlightFiles int64
	InFlightBytes int64
	QueuedFiles   int64
}

func newGatewayUpstreamResponseSpoolAdmission(limits gatewayUpstreamResponseSpoolAdmissionLimits) (*gatewayUpstreamResponseSpoolAdmission, error) {
	if limits.MaxInFlightFiles <= 0 || limits.MaxInFlightBytes <= 0 || limits.MaxQueuedFiles < 0 {
		return nil, fmt.Errorf("%w: spool file and byte limits must be positive and queue limit must be non-negative", errGatewayResponseBufferInvalid)
	}
	return &gatewayUpstreamResponseSpoolAdmission{limits: limits}, nil
}

// Acquire reserves one spool file slot. The declared per-response maximum must
// fit inside the process byte limit, but bytes are charged only as the lease
// grows so small responses do not reserve their complete hard maximum.
func (a *gatewayUpstreamResponseSpoolAdmission) Acquire(ctx context.Context, maxBytes int64) (*gatewayUpstreamResponseSpoolAdmissionLease, error) {
	if a == nil || ctx == nil || maxBytes < 0 {
		return nil, errGatewayResponseBufferInvalid
	}
	if maxBytes > a.limits.MaxInFlightBytes {
		return nil, fmt.Errorf("%w: response maximum %d exceeds process byte limit %d", errGatewayResponseSpoolCapacity, maxBytes, a.limits.MaxInFlightBytes)
	}
	if err := gatewayUpstreamResponseSpoolContextError(ctx); err != nil {
		return nil, err
	}

	lease := &gatewayUpstreamResponseSpoolAdmissionLease{admission: a, maxBytes: maxBytes}
	a.mu.Lock()
	if a.waiters.Len() == 0 && a.inFlightFiles < a.limits.MaxInFlightFiles {
		a.inFlightFiles++
		a.mu.Unlock()
		return lease, nil
	}
	if a.limits.MaxQueuedFiles == 0 || a.queuedFiles >= a.limits.MaxQueuedFiles {
		a.mu.Unlock()
		return nil, errGatewayResponseSpoolQueueFull
	}
	waiter := &gatewayUpstreamResponseSpoolAdmissionWaiter{lease: lease, ready: make(chan struct{})}
	waiter.element = a.waiters.PushBack(waiter)
	a.queuedFiles++
	a.drainLocked()
	a.mu.Unlock()

	select {
	case <-waiter.ready:
		if err := gatewayUpstreamResponseSpoolContextError(ctx); err != nil {
			lease.Release()
			return nil, gatewayResponseSpoolQueueWaitError(err)
		}
		return lease, nil
	case <-ctx.Done():
		if a.cancelWaiter(waiter) {
			lease.Release()
		}
		return nil, gatewayResponseSpoolQueueWaitError(gatewayUpstreamResponseSpoolContextError(ctx))
	}
}

func gatewayResponseSpoolQueueWaitError(cause error) error {
	if cause == nil {
		cause = context.Canceled
	}
	return fmt.Errorf("%w: %w", errGatewayResponseSpoolQueueWaitCanceled, cause)
}

// GrowTo charges the lease up to targetBytes. Growth is monotonic and
// non-blocking; callers must stop and release the spool on a capacity error.
func (l *gatewayUpstreamResponseSpoolAdmissionLease) GrowTo(ctx context.Context, targetBytes int64) error {
	if l == nil || l.admission == nil || ctx == nil || targetBytes < 0 {
		return errGatewayResponseBufferInvalid
	}
	if err := gatewayUpstreamResponseSpoolContextError(ctx); err != nil {
		return err
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return errGatewayResponseSpoolReleased
	}
	if targetBytes <= l.bytes {
		return nil
	}
	if targetBytes > l.maxBytes {
		return fmt.Errorf("%w: response exceeds %d bytes", ErrGatewayUpstreamResponseTooLarge, l.maxBytes)
	}
	delta := targetBytes - l.bytes
	a := l.admission
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := gatewayUpstreamResponseSpoolContextError(ctx); err != nil {
		return err
	}
	if delta > a.limits.MaxInFlightBytes-a.inFlightBytes {
		return fmt.Errorf("%w: growing response to %d bytes would exceed process limit %d", errGatewayResponseSpoolCapacity, targetBytes, a.limits.MaxInFlightBytes)
	}
	a.inFlightBytes += delta
	l.bytes = targetBytes
	return nil
}

// Release is idempotent. It releases both the file slot and every byte charged
// by GrowTo, then admits queued file requests in FIFO order.
func (l *gatewayUpstreamResponseSpoolAdmissionLease) Release() {
	if l == nil || l.admission == nil {
		return
	}
	l.mu.Lock()
	if l.released {
		l.mu.Unlock()
		return
	}
	l.released = true
	bytes := l.bytes
	l.bytes = 0
	l.mu.Unlock()

	a := l.admission
	a.mu.Lock()
	a.inFlightFiles--
	a.inFlightBytes -= bytes
	a.drainLocked()
	a.mu.Unlock()
}

func (a *gatewayUpstreamResponseSpoolAdmission) cancelWaiter(waiter *gatewayUpstreamResponseSpoolAdmissionWaiter) bool {
	if a == nil || waiter == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if waiter.granted {
		return true
	}
	if waiter.element != nil {
		a.waiters.Remove(waiter.element)
		waiter.element = nil
		a.queuedFiles--
	}
	return false
}

func (a *gatewayUpstreamResponseSpoolAdmission) drainLocked() {
	for a.inFlightFiles < a.limits.MaxInFlightFiles && a.waiters.Len() > 0 {
		element := a.waiters.Front()
		waiter := element.Value.(*gatewayUpstreamResponseSpoolAdmissionWaiter)
		a.waiters.Remove(element)
		waiter.element = nil
		a.queuedFiles--
		a.inFlightFiles++
		waiter.granted = true
		close(waiter.ready)
	}
}

func (a *gatewayUpstreamResponseSpoolAdmission) snapshot() gatewayUpstreamResponseSpoolAdmissionSnapshot {
	if a == nil {
		return gatewayUpstreamResponseSpoolAdmissionSnapshot{}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return gatewayUpstreamResponseSpoolAdmissionSnapshot{
		InFlightFiles: a.inFlightFiles,
		InFlightBytes: a.inFlightBytes,
		QueuedFiles:   a.queuedFiles,
	}
}

// spoolGatewayUpstreamResponse copies reader into a private temporary file with
// a fixed-size copy buffer. TempDir must be a pre-created dedicated 0700
// directory. It checks context around reads, writes, EOF and the final rewind,
// so cancellation never returns a replayable payload.
func spoolGatewayUpstreamResponse(ctx context.Context, reader io.Reader, options gatewayUpstreamResponseSpoolOptions) (*gatewayUpstreamResponseSpool, error) {
	if ctx == nil || reader == nil || options.MaxBytes < 0 || options.PrefixBytes < 0 ||
		options.PrefixBytes > options.MaxBytes || options.PrefixBytes > gatewayUpstreamResponseSpoolMaxPrefixBytes {
		return nil, errGatewayResponseBufferInvalid
	}
	if err := gatewayUpstreamResponseSpoolContextError(ctx); err != nil {
		return nil, err
	}
	directory, err := validateGatewayUpstreamResponseSpoolDirectory(options.TempDir)
	if err != nil {
		return nil, err
	}

	var admissionLease *gatewayUpstreamResponseSpoolAdmissionLease
	if options.Admission != nil {
		admissionCtx, cancelAdmission := ctx, func() {}
		if options.AdmissionTimeout > 0 {
			admissionCtx, cancelAdmission = context.WithTimeout(ctx, options.AdmissionTimeout)
		}
		admissionLease, err = options.Admission.Acquire(admissionCtx, options.MaxBytes)
		cancelAdmission()
		if err != nil {
			return nil, err
		}
		// A queued acquisition may outlive a directory replacement or permission
		// change. Revalidate before opening a product-owned file.
		directory, err = validateGatewayUpstreamResponseSpoolDirectory(directory)
		if err != nil {
			admissionLease.Release()
			return nil, err
		}
	}
	file, err := os.CreateTemp(directory, gatewayUpstreamResponseSpoolFilePrefix+"*")
	if err != nil {
		if admissionLease != nil {
			admissionLease.Release()
		}
		return nil, err
	}
	// os.CreateTemp creates 0600 files on supported systems. Chmod makes that
	// invariant explicit even when a platform's implementation changes.
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		if admissionLease != nil {
			admissionLease.Release()
		}
		return nil, err
	}
	spool := &gatewayUpstreamResponseSpool{
		file:   file,
		path:   file.Name(),
		prefix: make([]byte, 0, options.PrefixBytes),
		lease:  admissionLease,
	}
	if err := spool.copyFrom(ctx, reader, options.MaxBytes, options.OnFirstByte); err != nil {
		return nil, errors.Join(err, spool.Close())
	}
	if err := gatewayUpstreamResponseSpoolContextError(ctx); err != nil {
		return nil, errors.Join(err, spool.Close())
	}
	if _, err := spool.file.Seek(0, io.SeekStart); err != nil {
		return nil, errors.Join(err, spool.Close())
	}
	if err := gatewayUpstreamResponseSpoolContextError(ctx); err != nil {
		return nil, errors.Join(err, spool.Close())
	}
	return spool, nil
}

func (s *gatewayUpstreamResponseSpool) copyFrom(ctx context.Context, reader io.Reader, maxBytes int64, onFirstByte func()) error {
	buffer := make([]byte, 32<<10)
	firstByte := false
	for {
		if err := gatewayUpstreamResponseSpoolContextError(ctx); err != nil {
			return err
		}
		remaining := maxBytes - s.size
		readSize := len(buffer)
		if remaining < int64(readSize) {
			// Read exactly one byte beyond the allowed payload after the
			// persistent file reaches its maximum size.
			readSize = int(remaining + 1)
		}
		if readSize <= 0 {
			return fmt.Errorf("%w: response exceeds %d bytes", ErrGatewayUpstreamResponseTooLarge, maxBytes)
		}
		read, readErr := reader.Read(buffer[:readSize])
		if read > 0 {
			if !firstByte {
				firstByte = true
				if onFirstByte != nil {
					onFirstByte()
				}
			}
			if err := gatewayUpstreamResponseSpoolContextError(ctx); err != nil {
				return err
			}
			if int64(read) > remaining {
				return fmt.Errorf("%w: response exceeds %d bytes", ErrGatewayUpstreamResponseTooLarge, maxBytes)
			}
			targetSize := s.size + int64(read)
			if s.lease != nil {
				if err := s.lease.GrowTo(ctx, targetSize); err != nil {
					return err
				}
			}
			written, err := s.file.Write(buffer[:read])
			if err != nil {
				return err
			}
			if written != read {
				return io.ErrShortWrite
			}
			s.size = targetSize
			s.appendPrefix(buffer[:read])
			if err := gatewayUpstreamResponseSpoolContextError(ctx); err != nil {
				return err
			}
		}
		if readErr == io.EOF {
			if err := gatewayUpstreamResponseSpoolContextError(ctx); err != nil {
				return err
			}
			return nil
		}
		if readErr != nil {
			return readErr
		}
		if read == 0 {
			return io.ErrNoProgress
		}
	}
}

func (s *gatewayUpstreamResponseSpool) appendPrefix(chunk []byte) {
	remaining := cap(s.prefix) - len(s.prefix)
	if remaining <= 0 {
		return
	}
	if len(chunk) > remaining {
		chunk = chunk[:remaining]
	}
	s.prefix = append(s.prefix, chunk...)
}

// Prefix returns a copy of the bounded metadata window. It never exposes the
// mutable backing buffer held by the spool.
func (s *gatewayUpstreamResponseSpool) Prefix() []byte {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.prefix...)
}

func (s *gatewayUpstreamResponseSpool) Size() int64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size
}

// Read and Seek make the spool directly replayable after construction. A
// caller that needs an independent cursor may use Open instead.
func (s *gatewayUpstreamResponseSpool) Read(payload []byte) (int, error) {
	if s == nil || s.file == nil {
		return 0, os.ErrClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, os.ErrClosed
	}
	return s.file.Read(payload)
}

func (s *gatewayUpstreamResponseSpool) Seek(offset int64, whence int) (int64, error) {
	if s == nil || s.file == nil {
		return 0, os.ErrClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, os.ErrClosed
	}
	return s.file.Seek(offset, whence)
}

type gatewayUpstreamResponseSpoolReader struct {
	*os.File

	spool *gatewayUpstreamResponseSpool
	once  sync.Once
	err   error
}

// Open supplies an independent replay cursor. The returned reader is valid
// until its own Close even if the spool is subsequently closed. The spool keeps
// its admission reservation until every independent cursor is closed, because
// an unlinked file still consumes disk space while an open descriptor exists.
func (s *gatewayUpstreamResponseSpool) Open() (*gatewayUpstreamResponseSpoolReader, error) {
	if s == nil {
		return nil, os.ErrClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, os.ErrClosed
	}
	file, err := os.Open(s.path)
	if err != nil {
		return nil, err
	}
	s.openReaders++
	return &gatewayUpstreamResponseSpoolReader{File: file, spool: s}, nil
}

func (r *gatewayUpstreamResponseSpoolReader) Close() error {
	if r == nil {
		return nil
	}
	r.once.Do(func() {
		if r.File != nil {
			r.err = r.File.Close()
		}
		if r.spool != nil {
			r.spool.closeReader()
		}
	})
	return r.err
}

func (s *gatewayUpstreamResponseSpool) closeReader() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.openReaders > 0 {
		s.openReaders--
	}
	s.releaseAdmissionLocked()
}

// Close closes and removes the temporary file. Repeated calls are idempotent
// and return the original cleanup result, making defer safe on every path.
func (s *gatewayUpstreamResponseSpool) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.removeErr
	}
	s.closed = true
	var closeErr error
	if s.file != nil {
		closeErr = s.file.Close()
	}
	removeErr := os.Remove(s.path)
	if errors.Is(removeErr, os.ErrNotExist) {
		removeErr = nil
	}
	s.unlinked = removeErr == nil
	s.removeErr = errors.Join(closeErr, removeErr)
	s.releaseAdmissionLocked()
	return s.removeErr
}

func (s *gatewayUpstreamResponseSpool) releaseAdmissionLocked() {
	if s == nil || s.admissionClosed || !s.closed || !s.unlinked || s.openReaders != 0 || s.lease == nil {
		return
	}
	s.admissionClosed = true
	s.lease.Release()
}

func gatewayUpstreamResponseSpoolContextError(ctx context.Context) error {
	if ctx == nil {
		return errGatewayResponseBufferInvalid
	}
	if ctx.Err() == nil {
		return nil
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	return ctx.Err()
}

// prepareGatewayUpstreamResponseSpoolDirectory creates or tightens the
// dedicated spool directory. It refuses relative paths, the filesystem root,
// symlinks and non-directories. Callers run this once during startup before
// accepting gateway traffic.
func prepareGatewayUpstreamResponseSpoolDirectory(directory string) (string, error) {
	directory = strings.TrimSpace(directory)
	if directory == "" || !filepath.IsAbs(directory) {
		return "", fmt.Errorf("%w: spool directory must be an absolute path", errGatewayResponseBufferInvalid)
	}
	directory = filepath.Clean(directory)
	if directory == string(filepath.Separator) {
		return "", fmt.Errorf("%w: filesystem root cannot be used as spool directory", errGatewayResponseBufferInvalid)
	}
	if info, err := os.Lstat(directory); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return "", fmt.Errorf("%w: spool path is not a dedicated directory", errGatewayResponseBufferInvalid)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return "", err
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return "", err
	}
	return validateGatewayUpstreamResponseSpoolDirectory(directory)
}

func validateGatewayUpstreamResponseSpoolDirectory(directory string) (string, error) {
	directory = strings.TrimSpace(directory)
	if directory == "" || !filepath.IsAbs(directory) {
		return "", fmt.Errorf("%w: spool directory must be an absolute path", errGatewayResponseBufferInvalid)
	}
	directory = filepath.Clean(directory)
	if directory == string(filepath.Separator) {
		return "", fmt.Errorf("%w: filesystem root cannot be used as spool directory", errGatewayResponseBufferInvalid)
	}
	info, err := os.Lstat(directory)
	if err != nil {
		return "", err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm() != 0o700 {
		return "", fmt.Errorf("%w: spool directory must be a non-symlink directory with mode 0700", errGatewayResponseBufferInvalid)
	}
	return directory, nil
}

type gatewayUpstreamResponseSpoolOrphan struct {
	path string
	info os.FileInfo
}

// cleanupGatewayUpstreamResponseSpoolOrphans removes only old regular files
// created with this package's fixed prefix. It preflights every matching entry
// before deleting anything; a matching symlink, directory or special file makes
// the cleanup fail closed. Unrelated entries are never inspected or removed.
func cleanupGatewayUpstreamResponseSpoolOrphans(directory string, now time.Time, minimumAge time.Duration) (int, error) {
	directory, err := validateGatewayUpstreamResponseSpoolDirectory(directory)
	if err != nil {
		return 0, err
	}
	if now.IsZero() || minimumAge <= 0 {
		return 0, fmt.Errorf("%w: orphan cleanup requires current time and a positive minimum age", errGatewayResponseBufferInvalid)
	}
	cutoff := now.Add(-minimumAge)
	entries, err := os.ReadDir(directory)
	if err != nil {
		return 0, err
	}
	orphans := make([]gatewayUpstreamResponseSpoolOrphan, 0)
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), gatewayUpstreamResponseSpoolFilePrefix) {
			continue
		}
		path := filepath.Join(directory, entry.Name())
		info, err := os.Lstat(path)
		if err != nil {
			return 0, err
		}
		if !info.Mode().IsRegular() {
			return 0, fmt.Errorf("%w: matching spool orphan is not a regular file: %s", errGatewayResponseBufferInvalid, entry.Name())
		}
		if !info.ModTime().After(cutoff) {
			orphans = append(orphans, gatewayUpstreamResponseSpoolOrphan{path: path, info: info})
		}
	}

	removed := 0
	for _, orphan := range orphans {
		current, err := os.Lstat(orphan.path)
		if err != nil {
			return removed, err
		}
		if !current.Mode().IsRegular() || !os.SameFile(orphan.info, current) {
			return removed, fmt.Errorf("%w: matching spool orphan changed during cleanup: %s", errGatewayResponseBufferInvalid, filepath.Base(orphan.path))
		}
		if err := os.Remove(orphan.path); err != nil {
			return removed, err
		}
		removed++
	}
	return removed, nil
}
