// Command rasdaemon-exporter provides the collector's process boundary.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"rasdaemon-exporter/internal/collector"
)

func main() {
	// Keep os.Exit outside run so connection and signal cleanup can finish.
	os.Exit(run(os.Args[1:], os.Stderr))
}

func run(args []string, stderr io.Writer) int {
	cfg, err := parseConfig(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}

	// Help and invalid arguments never open the source database. Signals cancel
	// database work through context rather than interrupting cleanup with Exit.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := collector.Run(ctx, cfg); err != nil {
		if onlyContextCanceled(err) && ctx.Err() != nil {
			return 0
		}
		fmt.Fprintf(stderr, "rasdaemon-exporter: %v\n", err)
		return 1
	}
	return 0
}

// onlyContextCanceled accepts cancellation-only wrapped or joined errors.
// Cleanup errors remain visible instead of being hidden by a canceled context.
func onlyContextCanceled(err error) bool {
	if err == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !onlyContextCanceled(cause) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return onlyContextCanceled(wrapped.Unwrap())
	}
	return err == context.Canceled
}

// parseConfig owns CLI diagnostics, including flag's help/error output.
// Callers use the returned error for exit status and must not print it again.
func parseConfig(args []string, stderr io.Writer) (collector.Config, error) {
	cfg := collector.DefaultConfig()
	flags := flag.NewFlagSet("rasdaemon-exporter", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&cfg.DatabasePath, "db", cfg.DatabasePath,
		"absolute path to an existing rasdaemon SQLite database (read-only)")
	flags.StringVar(&cfg.StateDir, "state-dir", cfg.StateDir,
		"absolute path to the private collector state directory")
	flags.StringVar(&cfg.SnapshotPath, "output", cfg.SnapshotPath,
		"absolute path to the Prometheus textfile output")
	flags.DurationVar(&cfg.Interval, "interval", cfg.Interval,
		"delay between completed collection rounds")
	flags.Usage = func() {
		fmt.Fprintln(stderr, "Usage: rasdaemon-exporter [--db PATH] [--state-dir PATH] [--output PATH] [--interval DURATION]")
		fmt.Fprintln(stderr, "Collects rasdaemon events and publishes Prometheus textfile metrics.")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		return collector.Config{}, err
	}
	if flags.NArg() != 0 {
		err := errors.New("unexpected positional arguments; use --db PATH")
		fmt.Fprintf(stderr, "rasdaemon-exporter: %v\n", err)
		return collector.Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(stderr, "rasdaemon-exporter: configuration: %v\n", err)
		return collector.Config{}, err
	}
	return cfg, nil
}
