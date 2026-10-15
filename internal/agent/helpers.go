package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const runtimeMaxOutput = 32 * 1024
const maxAgentConfigFileBytes = 16 << 20
const maxAgentStateFileBytes = 1 << 20

type runtimeOutputBuffer struct {
	mu    sync.Mutex
	data  []byte
	start int
	size  int
}

func (b *runtimeOutputBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(p) == 0 {
		return 0, nil
	}
	if b.data == nil {
		b.data = make([]byte, runtimeMaxOutput)
	}
	if len(p) >= runtimeMaxOutput {
		copy(b.data, p[len(p)-runtimeMaxOutput:])
		b.start = 0
		b.size = runtimeMaxOutput
	} else {
		end := (b.start + b.size) % runtimeMaxOutput
		written := copy(b.data[end:], p)
		copy(b.data, p[written:])
		overflow := max(0, b.size+len(p)-runtimeMaxOutput)
		b.start = (b.start + overflow) % runtimeMaxOutput
		b.size = min(runtimeMaxOutput, b.size+len(p))
	}
	return len(p), nil
}

func (b *runtimeOutputBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.size == 0 {
		return ""
	}
	end := b.start + b.size
	if end <= runtimeMaxOutput {
		return string(b.data[b.start:end])
	}
	var output strings.Builder
	output.Grow(b.size)
	output.Write(b.data[b.start:])
	output.Write(b.data[:end-runtimeMaxOutput])
	return output.String()
}

func runRuntimeCommand(timeout time.Duration, name string, args ...string) (string, int, error) {
	return runRuntimeCommandContext(context.Background(), timeout, name, args...)
}

func runRuntimeCommandContext(parent context.Context, timeout time.Duration, name string, args ...string) (string, int, error) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	command := exec.CommandContext(ctx, name, args...)
	command.WaitDelay = 2 * time.Second
	command.Env = append(command.Environ(), "LC_ALL=C", "LANG=C")
	var output runtimeOutputBuffer
	command.Stdout = &output
	command.Stderr = &output
	err := command.Run()
	exitCode := 0
	if err != nil {
		exitCode = -1
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			exitCode = exitError.ExitCode()
		}
	}
	if ctx.Err() == context.DeadlineExceeded {
		return output.String(), exitCode, fmt.Errorf("command timed out")
	}
	if ctx.Err() != nil {
		return output.String(), exitCode, ctx.Err()
	}
	return output.String(), exitCode, err
}

func commandErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	return strings.TrimSpace(err.Error())
}

func tailOutput(output string, limit int) string {
	if len(output) <= limit {
		return strings.TrimSpace(output)
	}
	return strings.TrimSpace(output[len(output)-limit:])
}

func readBoundedAgentFile(path string, limit int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("agent state must be a regular file")
	}
	if info.Size() > limit {
		return nil, fmt.Errorf("agent state file exceeds %d bytes", limit)
	}
	// One extra byte detects growth without ReadAll's expanding intermediate buffers.
	data := make([]byte, info.Size()+1)
	n, err := io.ReadFull(file, data)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, err
	}
	if n == len(data) {
		return nil, fmt.Errorf("state changed while reading")
	}
	return data[:n], nil
}

func atomicWrite(path string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".ravel-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}
