package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"log2sql/internal/importer"
	"log2sql/internal/store"
	"log2sql/internal/web"
)

const (
	defaultListenHost = "127.0.0.1"
	defaultPort       = 8090
)

func main() {
	log.SetFlags(0)

	if len(os.Args) < 2 {
		if err := runDefault(); err != nil {
			log.Fatal(err)
		}
		return
	}

	switch os.Args[1] {
	case "ingest":
		if err := runIngest(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
	case "serve":
		if err := runServe(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
	case "help", "-h", "--help":
		printHelpCommand(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "Unknown command %q.\n\n", os.Args[1])
		printMainUsage(os.Stderr)
		os.Exit(2)
	}
}

func runIngest(args []string) error {
	fs := newFlagSet("ingest")
	input := fs.String("input", "", "Path to input file")
	dbPath := fs.String("db", "", "Path to SQLite database")
	format := fs.String("format", "auto", "Input format: auto, csv, plain")
	listen := fs.String("listen", defaultListenHost, "Listen host")
	port := fs.Int("port", defaultPort, "Listen port")
	noServe := fs.Bool("no-serve", false, "Import only; do not start the web UI")
	fs.Usage = func() {
		printIngestUsage(fs.Output())
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *input == "" || *dbPath == "" {
		printIngestUsage(os.Stderr)
		return fmt.Errorf("both --input and --db are required")
	}

	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}

	progress := newCLIProgressPrinter(os.Stderr)
	imp := importer.New(st).WithProgress(progress.Update)
	result, err := imp.ImportFile(context.Background(), *input, *format)
	progress.Finish()
	closeErr := st.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}

	fmt.Printf("Imported %d rows from %s into %s (format=%s, import_id=%d)\n", result.RowsInserted, *input, *dbPath, result.Format, result.ImportID)

	if *noServe {
		return nil
	}

	manager, err := newWebManager(*dbPath)
	if err != nil {
		return err
	}
	defer manager.Close()

	addr := fmt.Sprintf("%s:%d", *listen, *port)
	fmt.Printf("Starting web UI at http://%s\n", addr)
	return web.ListenAndServe(addr, manager)
}

func runServe(args []string) error {
	fs := newFlagSet("serve")
	dbPath := fs.String("db", "", "Path to SQLite database")
	listen := fs.String("listen", defaultListenHost, "Listen host")
	port := fs.Int("port", defaultPort, "Listen port")
	fs.Usage = func() {
		printServeUsage(fs.Output())
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dbPath != "" {
		if err := validateServeDBPath(*dbPath); err != nil {
			return err
		}
	}

	manager, err := newWebManager(*dbPath)
	if err != nil {
		return err
	}
	defer manager.Close()

	addr := fmt.Sprintf("%s:%d", *listen, *port)
	fmt.Printf("Starting web UI at http://%s\n", addr)
	return web.ListenAndServe(addr, manager)
}

func runDefault() error {
	manager, err := newWebManager("")
	if err != nil {
		return err
	}
	defer manager.Close()

	addr := fmt.Sprintf("%s:%d", defaultListenHost, defaultPort)
	fmt.Printf("Starting web UI at http://%s\n", addr)
	return web.ListenAndServe(addr, manager)
}

func newFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	return fs
}

func printHelpCommand(args []string) {
	if len(args) == 0 {
		printMainUsage(os.Stdout)
		return
	}

	switch args[0] {
	case "ingest":
		printIngestUsage(os.Stdout)
	case "serve":
		printServeUsage(os.Stdout)
	default:
		fmt.Fprintf(os.Stderr, "Unknown help topic %q.\n\n", args[0])
		printMainUsage(os.Stderr)
		os.Exit(2)
	}
}

func printMainUsage(w io.Writer) {
	bin := filepath.Base(os.Args[0])
	fmt.Fprintf(w, "Log2SQL imports Graylog-style logs into SQLite and serves a local query UI.\n\n")
	fmt.Fprintf(w, "Regular user flow:\n")
	fmt.Fprintf(w, "  1. Run %s with no arguments.\n", bin)
	fmt.Fprintf(w, "  2. Use the web home screen to import a log file or open an existing DB.\n")
	fmt.Fprintf(w, "  3. Reuse the same database later with the serve command.\n\n")
	fmt.Fprintf(w, "Usage:\n")
	fmt.Fprintf(w, "  %s\n", bin)
	fmt.Fprintf(w, "  %s ingest --input /path/to/logs.csv --db /path/to/logs.sqlite\n", bin)
	fmt.Fprintf(w, "  %s serve [--db /path/to/logs.sqlite]\n", bin)
	fmt.Fprintf(w, "  %s help [ingest|serve]\n\n", bin)
	fmt.Fprintf(w, "Examples:\n")
	fmt.Fprintf(w, "  %s\n", bin)
	fmt.Fprintf(w, "  %s ingest --input ./graylog.csv --db ./logs.sqlite\n", bin)
	fmt.Fprintf(w, "  %s ingest --input ./app.log --format plain --db ./logs.sqlite --no-serve\n", bin)
	fmt.Fprintf(w, "  %s serve --db ./logs.sqlite --port %d\n\n", bin, defaultPort)
	fmt.Fprintf(w, "Run '%s help ingest' or '%s help serve' for detailed command help.\n", bin, bin)
}

func printIngestUsage(w io.Writer) {
	bin := filepath.Base(os.Args[0])
	fmt.Fprintf(w, "Usage:\n")
	fmt.Fprintf(w, "  %s ingest --input /path/to/logs.csv --db /path/to/logs.sqlite [options]\n\n", bin)
	fmt.Fprintf(w, "What it does:\n")
	fmt.Fprintf(w, "  - reads CSV or plain-text log files\n")
	fmt.Fprintf(w, "  - parses Graylog-style prefixes and JSON messages\n")
	fmt.Fprintf(w, "  - appends parsed rows into the SQLite database\n")
	fmt.Fprintf(w, "  - starts the web UI unless --no-serve is set\n\n")
	fmt.Fprintf(w, "Options:\n")
	fmt.Fprintf(w, "  --input <path>      Input log file to import (required)\n")
	fmt.Fprintf(w, "  --db <path>         SQLite database path (required)\n")
	fmt.Fprintf(w, "  --format <value>    auto, csv, or plain (default: auto)\n")
	fmt.Fprintf(w, "  --listen <host>     Web UI listen host (default: %s)\n", defaultListenHost)
	fmt.Fprintf(w, "  --port <port>       Web UI port (default: %d)\n", defaultPort)
	fmt.Fprintf(w, "  --no-serve          Import only; do not start the UI\n\n")
	fmt.Fprintf(w, "Examples:\n")
	fmt.Fprintf(w, "  %s ingest --input ./graylog.csv --db ./logs.sqlite\n", bin)
	fmt.Fprintf(w, "  %s ingest --input ./service.log --format plain --db ./logs.sqlite --no-serve\n", bin)
}

func printServeUsage(w io.Writer) {
	bin := filepath.Base(os.Args[0])
	fmt.Fprintf(w, "Usage:\n")
	fmt.Fprintf(w, "  %s serve [--db /path/to/logs.sqlite] [options]\n\n", bin)
	fmt.Fprintf(w, "What it does:\n")
	fmt.Fprintf(w, "  - starts the local web UI home screen\n")
	fmt.Fprintf(w, "  - optionally opens an existing SQLite database immediately\n\n")
	fmt.Fprintf(w, "Options:\n")
	fmt.Fprintf(w, "  --db <path>         SQLite database path to open immediately (optional)\n")
	fmt.Fprintf(w, "  --listen <host>     Web UI listen host (default: %s)\n", defaultListenHost)
	fmt.Fprintf(w, "  --port <port>       Web UI port (default: %d)\n\n", defaultPort)
	fmt.Fprintf(w, "Example:\n")
	fmt.Fprintf(w, "  %s serve --db ./logs.sqlite --port %d\n", bin, defaultPort)
}

func validateServeDBPath(dbPath string) error {
	info, err := os.Stat(dbPath)
	if err == nil {
		if info.IsDir() {
			return fmt.Errorf("database path %q is a directory, not a SQLite file", dbPath)
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to access database %q: %w", dbPath, err)
	}

	suggestions := findAvailableDBs(dbPath)
	if len(suggestions) == 0 {
		return fmt.Errorf("database %q does not exist", dbPath)
	}
	return fmt.Errorf("database %q does not exist\navailable databases:\n  - %s", dbPath, strings.Join(suggestions, "\n  - "))
}

func findAvailableDBs(dbPath string) []string {
	dir := filepath.Dir(dbPath)
	if dir == "." || dir == "" {
		dir = "."
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}

	suggestions := make([]string, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !looksLikeSQLiteFile(name) {
			continue
		}
		suggestions = append(suggestions, filepath.Join(dir, name))
	}
	sort.Strings(suggestions)
	return suggestions
}

func looksLikeSQLiteFile(name string) bool {
	lower := strings.ToLower(name)
	return strings.HasSuffix(lower, ".sqlite") || strings.HasSuffix(lower, ".sqlite3") || strings.HasSuffix(lower, ".db")
}

func newWebManager(initialDBPath string) (*web.Manager, error) {
	return web.NewManager(managedDatabaseDir(), initialDBPath)
}

func managedDatabaseDir() string {
	return filepath.Join(".", "log2sql-data", "databases")
}

type cliProgressPrinter struct {
	w         io.Writer
	lastWidth int
	hasOutput bool
}

func newCLIProgressPrinter(w io.Writer) *cliProgressPrinter {
	return &cliProgressPrinter{w: w}
}

func (p *cliProgressPrinter) Update(progress importer.Progress) {
	if progress.Done {
		return
	}

	line := fmt.Sprintf(
		"\rIngesting %s: %s (%d rows)",
		progress.Format,
		formatBytesProgress(progress.BytesRead, progress.TotalBytes),
		progress.RowsInserted,
	)
	if progress.RowsTotal > progress.RowsInserted {
		line = fmt.Sprintf(
			"\rIngesting %s: %s (%d/%d rows)",
			progress.Format,
			formatBytesProgress(progress.BytesRead, progress.TotalBytes),
			progress.RowsInserted,
			progress.RowsTotal,
		)
	}

	padding := ""
	visibleWidth := len(stripANSI(line))
	if p.lastWidth > visibleWidth {
		padding = strings.Repeat(" ", p.lastWidth-visibleWidth)
	}
	fmt.Fprint(p.w, line, padding)
	p.lastWidth = visibleWidth
	p.hasOutput = true
}

func (p *cliProgressPrinter) Finish() {
	if !p.hasOutput {
		return
	}
	fmt.Fprintln(p.w)
	p.hasOutput = false
	p.lastWidth = 0
}

func formatBytesProgress(read, total int64) string {
	if total <= 0 {
		return fmt.Sprintf("%s read", humanBytes(read))
	}
	percent := float64(read) / float64(total) * 100
	if percent > 100 {
		percent = 100
	}
	return fmt.Sprintf("%.0f%% (%s / %s)", percent, humanBytes(read), humanBytes(total))
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}

	value := float64(n)
	suffixes := []string{"KB", "MB", "GB", "TB"}
	suffix := suffixes[0]
	for _, candidate := range suffixes {
		value /= unit
		suffix = candidate
		if value < unit {
			break
		}
	}
	if value >= 10 {
		return fmt.Sprintf("%.0f %s", value, suffix)
	}
	return fmt.Sprintf("%.1f %s", value, suffix)
}

func stripANSI(input string) string {
	return strings.ReplaceAll(input, "\r", "")
}
