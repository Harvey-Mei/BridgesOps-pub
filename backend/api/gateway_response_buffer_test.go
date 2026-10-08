package api

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

func gatewayResponseSpoolTestDirectory(t *testing.T) string {
	t.Helper()
	directory, err := prepareGatewayUpstreamResponseSpoolDirectory(filepath.Join(t.TempDir(), "spool"))
	if err != nil {
		t.Fatalf("prepare response spool test directory: %v", err)
	}
	return directory
}

func TestReadGatewayUpstreamResponseLimitedUsesLimitPlusOne(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		payload string
		limit   int64
		wantErr error
	}{
		{name: "zero", payload: "", limit: 0},
		{name: "exact", payload: "four", limit: 4},
		{name: "one byte over", payload: "five!", limit: 4, wantErr: ErrGatewayUpstreamResponseTooLarge},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := readGatewayUpstreamResponseLimited(bytes.NewBufferString(testCase.payload), testCase.limit)
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("read error = %v, want errors.Is(%v)", err, testCase.wantErr)
			}
			if testCase.wantErr == nil && string(got) != testCase.payload {
				t.Fatalf("payload = %q, want %q", got, testCase.payload)
			}
		})
	}
}

func TestSpoolGatewayUpstreamResponseBoundsMemoryAndReplays(t *testing.T) {
	payload := bytes.Repeat([]byte("0123456789abcdef"), 1<<16)
	spool, err := spoolGatewayUpstreamResponse(context.Background(), bytes.NewReader(payload), gatewayUpstreamResponseSpoolOptions{
		TempDir: gatewayResponseSpoolTestDirectory(t), MaxBytes: int64(len(payload)), PrefixBytes: 37,
	})
	if err != nil {
		t.Fatalf("spool response: %v", err)
	}
	path := spool.path
	defer func() {
		if err := spool.Close(); err != nil {
			t.Fatalf("close spool: %v", err)
		}
	}()
	if spool.Size() != int64(len(payload)) || len(spool.Prefix()) != 37 || cap(spool.prefix) != 37 {
		t.Fatalf("spool metadata = size %d prefix %d cap %d", spool.Size(), len(spool.Prefix()), cap(spool.prefix))
	}
	// The only retained response bytes are the bounded prefix; the full payload
	// is verified from disk instead of comparing another in-memory copy.
	info, err := os.Stat(path)
	if err != nil || info.Size() != int64(len(payload)) || info.Mode().Perm() != 0o600 {
		t.Fatalf("spool file info = %+v err=%v", info, err)
	}
	first, err := io.ReadAll(spool)
	if err != nil || !bytes.Equal(first, payload) {
		t.Fatalf("first replay = %d bytes err=%v", len(first), err)
	}
	if _, err := spool.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("rewind spool: %v", err)
	}
	second, err := io.ReadAll(spool)
	if err != nil || !bytes.Equal(second, payload) {
		t.Fatalf("second replay = %d bytes err=%v", len(second), err)
	}
	reader, err := spool.Open()
	if err != nil {
		t.Fatalf("open independent replay: %v", err)
	}
	defer reader.Close()
	if got, err := reader.Read(make([]byte, 16)); err != nil || got != 16 {
		t.Fatalf("independent replay first read = %d err=%v", got, err)
	}
}

func TestSpoolGatewayUpstreamResponseRejectsLimitPlusOneAndCleansUp(t *testing.T) {
	directory := gatewayResponseSpoolTestDirectory(t)
	spool, err := spoolGatewayUpstreamResponse(context.Background(), bytes.NewBufferString("abcde"), gatewayUpstreamResponseSpoolOptions{
		TempDir: directory, MaxBytes: 4,
	})
	if spool != nil || !errors.Is(err, ErrGatewayUpstreamResponseTooLarge) {
		t.Fatalf("spool result = %v err=%v", spool, err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 0 {
		t.Fatalf("temporary files after rejected response = %v err=%v", entries, err)
	}
}

func TestSpoolGatewayUpstreamResponseCancellationAndWriteFailureCleanUp(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	spool, err := spoolGatewayUpstreamResponse(canceled, bytes.NewBufferString("body"), gatewayUpstreamResponseSpoolOptions{TempDir: t.TempDir(), MaxBytes: 4})
	if spool != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled spool = %v err=%v", spool, err)
	}

	file := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	spool, err = spoolGatewayUpstreamResponse(context.Background(), bytes.NewBufferString("body"), gatewayUpstreamResponseSpoolOptions{TempDir: file, MaxBytes: 4})
	if spool != nil || err == nil {
		t.Fatalf("write setup failure spool = %v err=%v", spool, err)
	}
	spool, err = spoolGatewayUpstreamResponse(context.Background(), bytes.NewBufferString("body"), gatewayUpstreamResponseSpoolOptions{
		TempDir: t.TempDir(), MaxBytes: 4, PrefixBytes: 5,
	})
	if spool != nil || !errors.Is(err, errGatewayResponseBufferInvalid) {
		t.Fatalf("prefix larger than response maximum spool = %v err=%v", spool, err)
	}
	spool, err = spoolGatewayUpstreamResponse(context.Background(), bytes.NewBufferString("body"), gatewayUpstreamResponseSpoolOptions{
		TempDir: t.TempDir(), MaxBytes: gatewayUpstreamResponseSpoolMaxPrefixBytes + 1, PrefixBytes: gatewayUpstreamResponseSpoolMaxPrefixBytes + 1,
	})
	if spool != nil || !errors.Is(err, errGatewayResponseBufferInvalid) {
		t.Fatalf("prefix above process memory cap spool = %v err=%v", spool, err)
	}
}

func TestGatewayUpstreamResponseSpoolStopsAfterReadCancellationAndWriteFailure(t *testing.T) {
	directory := gatewayResponseSpoolTestDirectory(t)
	ctx, cancel := context.WithCancel(context.Background())
	spool, err := spoolGatewayUpstreamResponse(ctx, &gatewayResponseBufferCancelAfterFirstRead{cancel: cancel}, gatewayUpstreamResponseSpoolOptions{
		TempDir: directory, MaxBytes: 16,
	})
	if spool != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("mid-read cancellation spool=%v err=%v", spool, err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 0 {
		t.Fatalf("temporary files after cancellation = %v err=%v", entries, err)
	}

	file, err := os.CreateTemp(directory, "read-only-spool-*")
	if err != nil {
		t.Fatal(err)
	}
	path := file.Name()
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	readOnly, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	failedSpool := &gatewayUpstreamResponseSpool{file: readOnly, path: path}
	if err := failedSpool.copyFrom(context.Background(), bytes.NewBufferString("body"), 4, nil); err == nil {
		t.Fatal("copy to read-only temporary file unexpectedly succeeded")
	}
	if err := failedSpool.Close(); err != nil {
		t.Fatalf("clean up failed write spool: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed write temporary file remains: %v", err)
	}
}

func TestSpoolGatewayUpstreamResponseFirstByteAndCloseAreIdempotent(t *testing.T) {
	called := 0
	spool, err := spoolGatewayUpstreamResponse(context.Background(), bytes.NewBufferString("body"), gatewayUpstreamResponseSpoolOptions{
		TempDir: gatewayResponseSpoolTestDirectory(t), MaxBytes: 4,
		OnFirstByte: func() {
			called++
		},
	})
	if err != nil || called != 1 {
		t.Fatalf("spool error=%v first-byte calls=%d", err, called)
	}
	path := spool.path
	if err := spool.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := spool.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary file remains after close: %v", err)
	}
}

func TestSpoolGatewayUpstreamResponseRejectsCancellationReturnedWithEOF(t *testing.T) {
	directory := gatewayResponseSpoolTestDirectory(t)
	ctx, cancel := context.WithCancel(context.Background())
	spool, err := spoolGatewayUpstreamResponse(ctx, gatewayResponseBufferCancelOnEOFReader{cancel: cancel}, gatewayUpstreamResponseSpoolOptions{
		TempDir: directory, MaxBytes: 4,
	})
	if spool != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("EOF cancellation spool=%v err=%v", spool, err)
	}
	entries, readErr := os.ReadDir(directory)
	if readErr != nil || len(entries) != 0 {
		t.Fatalf("temporary files after EOF cancellation=%v err=%v", entries, readErr)
	}
}

func TestGatewayUpstreamResponseSpoolAdmissionGrowReleaseAndCapacity(t *testing.T) {
	admission, err := newGatewayUpstreamResponseSpoolAdmission(gatewayUpstreamResponseSpoolAdmissionLimits{
		MaxInFlightFiles: 2,
		MaxInFlightBytes: 10,
		MaxQueuedFiles:   1,
	})
	if err != nil {
		t.Fatalf("new spool admission: %v", err)
	}
	if _, err := admission.Acquire(context.Background(), 11); !errors.Is(err, errGatewayResponseSpoolCapacity) {
		t.Fatalf("acquire response maximum above process capacity error=%v", err)
	}
	first, err := admission.Acquire(context.Background(), 8)
	if err != nil {
		t.Fatalf("acquire first spool: %v", err)
	}
	second, err := admission.Acquire(context.Background(), 8)
	if err != nil {
		t.Fatalf("acquire second spool: %v", err)
	}
	if err := first.GrowTo(context.Background(), 6); err != nil {
		t.Fatalf("grow first spool: %v", err)
	}
	if err := second.GrowTo(context.Background(), 5); !errors.Is(err, errGatewayResponseSpoolCapacity) {
		t.Fatalf("grow beyond process capacity error=%v", err)
	}
	if err := second.GrowTo(context.Background(), 4); err != nil {
		t.Fatalf("grow second spool to remaining capacity: %v", err)
	}
	canceled, cancel := context.WithCancelCause(context.Background())
	cancelCause := errors.New("test spool growth cancellation")
	cancel(cancelCause)
	if err := second.GrowTo(canceled, 5); !errors.Is(err, cancelCause) {
		t.Fatalf("canceled spool growth error=%v", err)
	}
	if got := admission.snapshot(); got != (gatewayUpstreamResponseSpoolAdmissionSnapshot{InFlightFiles: 2, InFlightBytes: 10}) {
		t.Fatalf("spool admission snapshot=%+v", got)
	}
	if err := first.GrowTo(context.Background(), 9); !errors.Is(err, ErrGatewayUpstreamResponseTooLarge) {
		t.Fatalf("per-response maximum error=%v", err)
	}
	first.Release()
	first.Release()
	second.Release()
	if got := admission.snapshot(); got != (gatewayUpstreamResponseSpoolAdmissionSnapshot{}) {
		t.Fatalf("released spool admission snapshot=%+v", got)
	}
	if err := first.GrowTo(context.Background(), 7); !errors.Is(err, errGatewayResponseSpoolReleased) {
		t.Fatalf("grow released lease error=%v", err)
	}
}

func TestGatewayUpstreamResponseSpoolAdmissionRejectsInvalidLimits(t *testing.T) {
	for _, limits := range []gatewayUpstreamResponseSpoolAdmissionLimits{
		{},
		{MaxInFlightFiles: 1, MaxInFlightBytes: 0},
		{MaxInFlightFiles: 1, MaxInFlightBytes: 1, MaxQueuedFiles: -1},
	} {
		if admission, err := newGatewayUpstreamResponseSpoolAdmission(limits); admission != nil || !errors.Is(err, errGatewayResponseBufferInvalid) {
			t.Fatalf("invalid spool admission limits=%+v admission=%v err=%v", limits, admission, err)
		}
	}
}

func TestGatewayUpstreamResponseSpoolAdmissionQueueIsFIFO(t *testing.T) {
	admission, err := newGatewayUpstreamResponseSpoolAdmission(gatewayUpstreamResponseSpoolAdmissionLimits{
		MaxInFlightFiles: 1,
		MaxInFlightBytes: 16,
		MaxQueuedFiles:   2,
	})
	if err != nil {
		t.Fatalf("new spool admission: %v", err)
	}
	holder, err := admission.Acquire(context.Background(), 8)
	if err != nil {
		t.Fatalf("acquire holder: %v", err)
	}
	firstResult := make(chan gatewayResponseSpoolAdmissionResult, 1)
	go acquireGatewayResponseSpoolAdmission(context.Background(), admission, 8, firstResult)
	waitForGatewayResponseSpoolAdmission(t, admission, func(snapshot gatewayUpstreamResponseSpoolAdmissionSnapshot) bool {
		return snapshot.QueuedFiles == 1
	})
	secondResult := make(chan gatewayResponseSpoolAdmissionResult, 1)
	go acquireGatewayResponseSpoolAdmission(context.Background(), admission, 8, secondResult)
	waitForGatewayResponseSpoolAdmission(t, admission, func(snapshot gatewayUpstreamResponseSpoolAdmissionSnapshot) bool {
		return snapshot.QueuedFiles == 2
	})

	holder.Release()
	first := receiveGatewayResponseSpoolAdmission(t, firstResult)
	if first.err != nil || first.lease == nil {
		t.Fatalf("first FIFO acquisition=%+v", first)
	}
	select {
	case second := <-secondResult:
		if second.lease != nil {
			second.lease.Release()
		}
		t.Fatalf("second FIFO acquisition completed before first release: %+v", second)
	default:
	}
	if got := admission.snapshot(); got.InFlightFiles != 1 || got.QueuedFiles != 1 {
		t.Fatalf("FIFO queue snapshot=%+v", got)
	}
	first.lease.Release()
	second := receiveGatewayResponseSpoolAdmission(t, secondResult)
	if second.err != nil || second.lease == nil {
		t.Fatalf("second FIFO acquisition=%+v", second)
	}
	second.lease.Release()
}

func TestGatewayUpstreamResponseSpoolAdmissionQueueCancellationWakesNext(t *testing.T) {
	admission, err := newGatewayUpstreamResponseSpoolAdmission(gatewayUpstreamResponseSpoolAdmissionLimits{
		MaxInFlightFiles: 1,
		MaxInFlightBytes: 16,
		MaxQueuedFiles:   2,
	})
	if err != nil {
		t.Fatalf("new spool admission: %v", err)
	}
	holder, err := admission.Acquire(context.Background(), 8)
	if err != nil {
		t.Fatalf("acquire holder: %v", err)
	}

	firstContext, cancelFirst := context.WithCancelCause(context.Background())
	firstResult := make(chan gatewayResponseSpoolAdmissionResult, 1)
	go acquireGatewayResponseSpoolAdmission(firstContext, admission, 8, firstResult)
	waitForGatewayResponseSpoolAdmission(t, admission, func(snapshot gatewayUpstreamResponseSpoolAdmissionSnapshot) bool {
		return snapshot.QueuedFiles == 1
	})

	secondResult := make(chan gatewayResponseSpoolAdmissionResult, 1)
	go acquireGatewayResponseSpoolAdmission(context.Background(), admission, 8, secondResult)
	waitForGatewayResponseSpoolAdmission(t, admission, func(snapshot gatewayUpstreamResponseSpoolAdmissionSnapshot) bool {
		return snapshot.QueuedFiles == 2
	})
	if _, err := admission.Acquire(context.Background(), 8); !errors.Is(err, errGatewayResponseSpoolQueueFull) {
		t.Fatalf("full spool queue error=%v", err)
	}

	cancelCause := errors.New("test spool queue cancellation")
	cancelFirst(cancelCause)
	first := receiveGatewayResponseSpoolAdmission(t, firstResult)
	if first.lease != nil || !errors.Is(first.err, cancelCause) {
		t.Fatalf("canceled queued acquisition=%+v", first)
	}
	waitForGatewayResponseSpoolAdmission(t, admission, func(snapshot gatewayUpstreamResponseSpoolAdmissionSnapshot) bool {
		return snapshot.QueuedFiles == 1
	})

	holder.Release()
	second := receiveGatewayResponseSpoolAdmission(t, secondResult)
	if second.err != nil || second.lease == nil {
		t.Fatalf("second queued acquisition=%+v", second)
	}
	second.lease.Release()
	if got := admission.snapshot(); got != (gatewayUpstreamResponseSpoolAdmissionSnapshot{}) {
		t.Fatalf("queue release snapshot=%+v", got)
	}
}

func TestGatewayUpstreamResponseSpoolAdmissionQueuedCancellationPreservesLocalProvenance(t *testing.T) {
	admission, err := newGatewayUpstreamResponseSpoolAdmission(gatewayUpstreamResponseSpoolAdmissionLimits{
		MaxInFlightFiles: 1,
		MaxInFlightBytes: 16,
		MaxQueuedFiles:   1,
	})
	if err != nil {
		t.Fatalf("new spool admission: %v", err)
	}
	holder, err := admission.Acquire(context.Background(), 8)
	if err != nil {
		t.Fatalf("acquire holder: %v", err)
	}
	defer holder.Release()

	queuedContext, cancelQueued := context.WithCancelCause(context.Background())
	defer cancelQueued(nil)
	result := make(chan error, 1)
	go func() {
		lease, acquireErr := admission.Acquire(queuedContext, 8)
		if lease != nil {
			lease.Release()
		}
		result <- acquireErr
	}()
	waitForGatewayResponseSpoolAdmission(t, admission, func(snapshot gatewayUpstreamResponseSpoolAdmissionSnapshot) bool {
		return snapshot.QueuedFiles == 1
	})
	cause := errors.New("first-byte deadline elapsed while waiting for local spool admission")
	cancelQueued(cause)
	if acquireErr := <-result; !errors.Is(acquireErr, errGatewayResponseSpoolQueueWaitCanceled) || !errors.Is(acquireErr, cause) {
		t.Fatalf("queued acquire error=%v, want local queue marker and original cause", acquireErr)
	}
}

func TestGatewayUpstreamResponseSpoolAdmissionConcurrentGrowthIsBounded(t *testing.T) {
	admission, err := newGatewayUpstreamResponseSpoolAdmission(gatewayUpstreamResponseSpoolAdmissionLimits{
		MaxInFlightFiles: 2,
		MaxInFlightBytes: 10,
		MaxQueuedFiles:   0,
	})
	if err != nil {
		t.Fatalf("new spool admission: %v", err)
	}
	leases := make([]*gatewayUpstreamResponseSpoolAdmissionLease, 2)
	for index := range leases {
		leases[index], err = admission.Acquire(context.Background(), 10)
		if err != nil {
			t.Fatalf("acquire concurrent lease %d: %v", index, err)
		}
	}
	start := make(chan struct{})
	results := make(chan error, len(leases))
	var workers sync.WaitGroup
	for _, lease := range leases {
		workers.Add(1)
		go func(lease *gatewayUpstreamResponseSpoolAdmissionLease) {
			defer workers.Done()
			<-start
			results <- lease.GrowTo(context.Background(), 6)
		}(lease)
	}
	close(start)
	workers.Wait()
	close(results)
	successes, capacityFailures := 0, 0
	for result := range results {
		switch {
		case result == nil:
			successes++
		case errors.Is(result, errGatewayResponseSpoolCapacity):
			capacityFailures++
		default:
			t.Fatalf("unexpected concurrent growth error=%v", result)
		}
	}
	if successes != 1 || capacityFailures != 1 {
		t.Fatalf("concurrent growth successes=%d capacity_failures=%d", successes, capacityFailures)
	}
	if got := admission.snapshot(); got.InFlightFiles != 2 || got.InFlightBytes != 6 {
		t.Fatalf("concurrent growth snapshot=%+v", got)
	}
	for _, lease := range leases {
		lease.Release()
	}
}

func TestSpoolGatewayUpstreamResponseAdmissionTracksOpenReplayReader(t *testing.T) {
	admission, err := newGatewayUpstreamResponseSpoolAdmission(gatewayUpstreamResponseSpoolAdmissionLimits{
		MaxInFlightFiles: 1,
		MaxInFlightBytes: 4,
		MaxQueuedFiles:   0,
	})
	if err != nil {
		t.Fatalf("new spool admission: %v", err)
	}
	spool, err := spoolGatewayUpstreamResponse(context.Background(), bytes.NewBufferString("body"), gatewayUpstreamResponseSpoolOptions{
		TempDir: gatewayResponseSpoolTestDirectory(t), MaxBytes: 4, Admission: admission,
	})
	if err != nil {
		t.Fatalf("spool admitted response: %v", err)
	}
	reader, err := spool.Open()
	if err != nil {
		t.Fatalf("open replay reader: %v", err)
	}
	if err := spool.Close(); err != nil {
		t.Fatalf("close spool with replay reader: %v", err)
	}
	if got := admission.snapshot(); got.InFlightFiles != 1 || got.InFlightBytes != 4 {
		t.Fatalf("admission released before replay reader close: %+v", got)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close replay reader: %v", err)
	}
	if got := admission.snapshot(); got != (gatewayUpstreamResponseSpoolAdmissionSnapshot{}) {
		t.Fatalf("admission after replay reader close=%+v", got)
	}
}

func TestPrepareGatewayUpstreamResponseSpoolDirectoryIsPrivateAndRejectsSymlink(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "nested", "spool")
	prepared, err := prepareGatewayUpstreamResponseSpoolDirectory(directory)
	if err != nil || prepared != directory {
		t.Fatalf("prepare spool directory=%q err=%v", prepared, err)
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("prepared spool directory info=%+v err=%v", info, err)
	}
	if err := os.Chmod(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareGatewayUpstreamResponseSpoolDirectory(directory); err != nil {
		t.Fatalf("tighten existing spool directory: %v", err)
	}
	info, err = os.Lstat(directory)
	if err != nil {
		t.Fatalf("stat tightened spool directory: %v", err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("tightened spool directory mode=%v", info.Mode().Perm())
	}

	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	symlink := filepath.Join(root, "spool-link")
	if err := os.Symlink(target, symlink); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareGatewayUpstreamResponseSpoolDirectory(symlink); !errors.Is(err, errGatewayResponseBufferInvalid) {
		t.Fatalf("prepare symlink spool directory error=%v", err)
	}
}

func TestCleanupGatewayUpstreamResponseSpoolOrphansOnlyRemovesOldOwnedRegularFiles(t *testing.T) {
	directory, err := prepareGatewayUpstreamResponseSpoolDirectory(filepath.Join(t.TempDir(), "spool"))
	if err != nil {
		t.Fatalf("prepare spool directory: %v", err)
	}
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	old := writeGatewayResponseSpoolCleanupFile(t, directory, gatewayUpstreamResponseSpoolFilePrefix+"old", now.Add(-2*time.Hour))
	recent := writeGatewayResponseSpoolCleanupFile(t, directory, gatewayUpstreamResponseSpoolFilePrefix+"recent", now.Add(-30*time.Minute))
	unrelated := writeGatewayResponseSpoolCleanupFile(t, directory, "unrelated-old", now.Add(-2*time.Hour))
	removed, err := cleanupGatewayUpstreamResponseSpoolOrphans(directory, now, time.Hour)
	if err != nil || removed != 1 {
		t.Fatalf("cleanup removed=%d err=%v", removed, err)
	}
	if _, err := os.Stat(old); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old owned spool remains: %v", err)
	}
	for _, path := range []string{recent, unrelated} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("cleanup removed protected file %s: %v", path, err)
		}
	}
}

func TestCleanupGatewayUpstreamResponseSpoolOrphansFailsClosedOnMatchingNonRegularFile(t *testing.T) {
	directory, err := prepareGatewayUpstreamResponseSpoolDirectory(filepath.Join(t.TempDir(), "spool"))
	if err != nil {
		t.Fatalf("prepare spool directory: %v", err)
	}
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	old := writeGatewayResponseSpoolCleanupFile(t, directory, gatewayUpstreamResponseSpoolFilePrefix+"old", now.Add(-2*time.Hour))
	suspicious := filepath.Join(directory, gatewayUpstreamResponseSpoolFilePrefix+"directory")
	if err := os.Mkdir(suspicious, 0o700); err != nil {
		t.Fatal(err)
	}
	removed, err := cleanupGatewayUpstreamResponseSpoolOrphans(directory, now, time.Hour)
	if removed != 0 || !errors.Is(err, errGatewayResponseBufferInvalid) {
		t.Fatalf("fail-closed cleanup removed=%d err=%v", removed, err)
	}
	if _, err := os.Stat(old); err != nil {
		t.Fatalf("preflight failure removed old regular spool: %v", err)
	}
	if info, err := os.Stat(suspicious); err != nil || !info.IsDir() {
		t.Fatalf("preflight failure changed suspicious entry info=%+v err=%v", info, err)
	}
}

type gatewayResponseSpoolAdmissionResult struct {
	lease *gatewayUpstreamResponseSpoolAdmissionLease
	err   error
}

func acquireGatewayResponseSpoolAdmission(ctx context.Context, admission *gatewayUpstreamResponseSpoolAdmission, maxBytes int64, result chan<- gatewayResponseSpoolAdmissionResult) {
	lease, err := admission.Acquire(ctx, maxBytes)
	result <- gatewayResponseSpoolAdmissionResult{lease: lease, err: err}
}

func waitForGatewayResponseSpoolAdmission(t *testing.T, admission *gatewayUpstreamResponseSpoolAdmission, ready func(gatewayUpstreamResponseSpoolAdmissionSnapshot) bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if ready(admission.snapshot()) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for spool admission state: %+v", admission.snapshot())
		}
		runtime.Gosched()
	}
}

func receiveGatewayResponseSpoolAdmission(t *testing.T, results <-chan gatewayResponseSpoolAdmissionResult) gatewayResponseSpoolAdmissionResult {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for spool admission result")
		return gatewayResponseSpoolAdmissionResult{}
	}
}

func writeGatewayResponseSpoolCleanupFile(t *testing.T, directory, name string, modifiedAt time.Time) string {
	t.Helper()
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, []byte("orphan"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, modifiedAt, modifiedAt); err != nil {
		t.Fatal(err)
	}
	return path
}

type gatewayResponseBufferCancelAfterFirstRead struct {
	cancel context.CancelFunc
	done   bool
}

type gatewayResponseBufferCancelOnEOFReader struct {
	cancel context.CancelFunc
}

func (r gatewayResponseBufferCancelOnEOFReader) Read(_ []byte) (int, error) {
	r.cancel()
	return 0, io.EOF
}

func (r *gatewayResponseBufferCancelAfterFirstRead) Read(payload []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	r.done = true
	copy(payload, "x")
	r.cancel()
	return 1, nil
}
