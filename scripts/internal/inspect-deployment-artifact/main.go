package main

import (
	"debug/buildinfo"
	"debug/elf"
	"errors"
	"fmt"
	"os"
)

const serverMainPath = "github.com/Harvey-Mei/BridgeOps/cmd/server"

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: inspect-deployment-artifact ARTIFACT EXPECTED_REVISION")
		os.Exit(2)
	}
	if err := inspect(os.Args[1], os.Args[2]); err != nil {
		fmt.Fprintf(os.Stderr, "invalid deployment artifact: %v\n", err)
		os.Exit(1)
	}
}

func inspect(path, expectedRevision string) error {
	elfFile, err := elf.Open(path)
	if err != nil {
		return fmt.Errorf("open ELF: %w", err)
	}
	defer elfFile.Close()

	if elfFile.Class != elf.ELFCLASS64 || elfFile.Data != elf.ELFDATA2LSB || elfFile.Machine != elf.EM_X86_64 {
		return errors.New("artifact must be a 64-bit little-endian x86-64 ELF")
	}
	if elfFile.Type != elf.ET_EXEC && elfFile.Type != elf.ET_DYN {
		return errors.New("artifact must be an executable ELF")
	}
	for _, program := range elfFile.Progs {
		if program.Type == elf.PT_INTERP {
			return errors.New("artifact must be static and contain no PT_INTERP segment")
		}
	}

	build, err := buildinfo.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read Go build metadata: %w", err)
	}
	if build.Path != serverMainPath {
		return fmt.Errorf("unexpected Go main package %q", build.Path)
	}
	settings := make(map[string]string, len(build.Settings))
	for _, setting := range build.Settings {
		settings[setting.Key] = setting.Value
	}
	if settings["GOOS"] != "linux" || settings["GOARCH"] != "amd64" {
		return errors.New("Go build target must be linux/amd64")
	}
	if settings["vcs.revision"] != expectedRevision {
		return errors.New("artifact VCS revision does not match Git HEAD")
	}
	if settings["vcs.modified"] != "false" {
		return errors.New("artifact VCS metadata is missing or modified")
	}
	return nil
}
