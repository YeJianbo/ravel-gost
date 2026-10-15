package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"

	"github.com/YeJianbo/ravel-gost/internal/agent"
)

var version = "dev"

func main() {
	if len(os.Args) == 2 && os.Args[1] == "version" {
		fmt.Printf("ravel %s (gost-only)\n", version)
		return
	}
	if len(os.Args) < 2 || os.Args[1] != "run" {
		fmt.Fprintln(os.Stderr, "Usage: ravel version | ravel run --state /etc/ravel-gost/state.json")
		os.Exit(2)
	}
	flags := flag.NewFlagSet("run", flag.ExitOnError)
	statePath := flags.String("state", "/etc/ravel-gost/state.json", "machine credentials file")
	directory := flags.String("data", "/var/lib/ravel-gost", "private runtime state directory")
	gost := flags.String("gost", "/usr/local/ravel-gost/gost", "trusted GOST executable")
	flags.Parse(os.Args[2:])
	if runtime.GOOS != "linux" {
		slog.Error("Ravel GOST machine service requires Linux")
		os.Exit(1)
	}
	state, err := agent.LoadState(*statePath)
	if err != nil {
		slog.Error("load machine state failed", "error", err)
		os.Exit(1)
	}
	client, err := agent.NewClient(state)
	if err != nil {
		slog.Error("initialize panel connection failed", "error", err)
		os.Exit(1)
	}
	controller := &agent.Controller{Client: client, Directory: *directory, Relay: agent.NewRelayManager(*gost, filepath.Join(*directory, "relay.json"))}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	defer controller.Relay.Shutdown()
	if err := controller.Run(ctx, state, version); err != nil {
		slog.Error("Ravel stopped", "error", err)
		os.Exit(1)
	}
}
