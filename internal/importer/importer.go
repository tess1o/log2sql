package importer

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"log2sql/internal/parser"
	"log2sql/internal/store"
)

type Progress struct {
	Format       string
	RowsTotal    int
	RowsInserted int
	BytesRead    int64
	TotalBytes   int64
	Done         bool
}

type Result struct {
	ImportID     int64
	Format       string
	RowsTotal    int
	RowsInserted int
	ParseErrors  int
}

type Importer struct {
	store      *store.Store
	onProgress func(Progress)
}

func New(st *store.Store) *Importer {
	return &Importer{store: st}
}

func (i *Importer) WithProgress(fn func(Progress)) *Importer {
	i.onProgress = fn
	return i
}

func (i *Importer) ImportFile(ctx context.Context, inputPath, format string) (Result, error) {
	format = strings.ToLower(strings.TrimSpace(format))
	if format == "" {
		format = "auto"
	}
	if format == "auto" {
		detected, err := detectFormat(inputPath)
		if err != nil {
			return Result{}, err
		}
		format = detected
	}
	if format != "csv" && format != "plain" {
		return Result{}, fmt.Errorf("unsupported format %q", format)
	}

	importID, err := i.store.CreateImport(ctx, inputPath, format)
	if err != nil {
		return Result{}, err
	}

	result := Result{ImportID: importID, Format: format}
	tx, err := i.store.Begin(ctx)
	if err != nil {
		return Result{}, err
	}

	commit := false
	defer func() {
		if !commit {
			_ = tx.Rollback()
		}
	}()

	switch format {
	case "csv":
		err = i.importCSV(ctx, tx, importID, inputPath, &result)
	case "plain":
		err = i.importPlain(ctx, tx, importID, inputPath, &result)
	}
	if err != nil {
		_ = i.store.FinishImport(ctx, importID, result.RowsTotal, result.RowsInserted, result.ParseErrors)
		return result, err
	}

	if err := tx.Commit(); err != nil {
		_ = i.store.FinishImport(ctx, importID, result.RowsTotal, result.RowsInserted, result.ParseErrors)
		return result, err
	}
	commit = true

	if err := i.store.FinishImport(ctx, importID, result.RowsTotal, result.RowsInserted, result.ParseErrors); err != nil {
		return result, err
	}

	i.emitProgress(Progress{
		Format:       result.Format,
		RowsTotal:    result.RowsTotal,
		RowsInserted: result.RowsInserted,
		Done:         true,
	})

	return result, nil
}

func (i *Importer) importCSV(ctx context.Context, tx *store.Tx, importID int64, inputPath string, result *Result) error {
	file, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer file.Close()

	totalBytes := fileSize(file)
	counter := &countingReader{reader: file}
	progress := newProgressReporter(i.emitProgress, "csv", totalBytes)

	reader := csv.NewReader(counter)

	header, err := reader.Read()
	if err != nil {
		return err
	}
	header = append([]string(nil), header...)
	reader.ReuseRecord = true

	messageIndex := -1
	for idx, name := range header {
		if strings.EqualFold(strings.TrimSpace(name), "message") {
			messageIndex = idx
			break
		}
	}
	if messageIndex == -1 {
		return errors.New(`CSV input must contain a "message" column`)
	}

	for rowNumber := 1; ; rowNumber++ {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		result.RowsTotal++

		csvFields := make(map[string]any)
		fieldSources := make(map[string]string)
		for idx, column := range header {
			if idx >= len(record) || idx == messageIndex {
				continue
			}
			csvFields[column] = parser.NormalizeStringValue(record[idx])
			fieldSources[column] = "csv"
		}

		rawMessage := ""
		if messageIndex < len(record) {
			rawMessage = record[messageIndex]
		}

		parsed := parser.ParseMessage(rawMessage)
		if err := i.insertRow(ctx, tx, importID, rowNumber, "csv", inputPath, rawMessage, parsed, csvFields, fieldSources); err != nil {
			return err
		}
		result.RowsInserted++
		progress.MaybeReport(*result, counter.BytesRead())
	}
}

func (i *Importer) importPlain(ctx context.Context, tx *store.Tx, importID int64, inputPath string, result *Result) error {
	file, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer file.Close()

	totalBytes := fileSize(file)
	counter := &countingReader{reader: file}
	progress := newProgressReporter(i.emitProgress, "plain", totalBytes)

	return readPlainEntries(counter, func(rowNumber int, rawMessage string) error {
		result.RowsTotal++
		parsed := parser.ParseMessage(rawMessage)
		if err := i.insertRow(ctx, tx, importID, rowNumber, "plain", inputPath, rawMessage, parsed, nil, nil); err != nil {
			return err
		}
		result.RowsInserted++
		progress.MaybeReport(*result, counter.BytesRead())
		return nil
	})
}

func (i *Importer) insertRow(ctx context.Context, tx *store.Tx, importID int64, rowNumber int, sourceFormat, filename, rawMessage string, parsed parser.ParsedRecord, preferredFields map[string]any, preferredSources map[string]string) error {
	mergedFields := make(map[string]any)
	fieldSources := make(map[string]string)

	for key, value := range parsed.PrefixFields {
		mergedFields[key] = value
		fieldSources[key] = "prefix"
	}

	for key, value := range parsed.JSONFields {
		if _, exists := mergedFields[key]; !exists {
			mergedFields[key] = value
			fieldSources[key] = "json"
		}
	}

	for key, value := range preferredFields {
		mergedFields[key] = value
		fieldSources[key] = preferredSources[key]
	}

	fingerprint := parser.ComputeFingerprint(parsed.MessageText, parsed.Level, mergedFields)
	entry := store.LogEntry{
		ImportID:           importID,
		Filename:           filename,
		RowNumber:          rowNumber,
		SourceFormat:       sourceFormat,
		RawMessage:         rawMessage,
		MessageText:        parsed.MessageText,
		MessageFingerprint: fingerprint,
		ParsedTimestamp:    parsed.ParsedTimestamp,
		Level:              parsed.Level,
		DynamicFields:      mergedFields,
		FieldSources:       fieldSources,
	}
	return i.store.InsertLog(ctx, tx, entry)
}

func detectFormat(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024), 10*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		rowReader := csv.NewReader(strings.NewReader(line))
		row, err := rowReader.Read()
		if err == nil {
			for _, field := range row {
				if strings.EqualFold(strings.TrimSpace(field), "message") {
					return "csv", nil
				}
			}
		}
		return "plain", nil
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}
	return "", errors.New("input file is empty")
}

func readPlainEntries(reader io.Reader, fn func(rowNumber int, rawMessage string) error) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 1024), 10*1024*1024)

	var builder strings.Builder
	rowNumber := 0
	flush := func() error {
		if builder.Len() == 0 {
			return nil
		}
		rowNumber++
		entry := builder.String()
		builder.Reset()
		return fn(rowNumber, entry)
	}

	for scanner.Scan() {
		line := scanner.Text()
		if parser.LooksLikeLogStart(line) && builder.Len() > 0 {
			if err := flush(); err != nil {
				return err
			}
		}

		if builder.Len() > 0 {
			builder.WriteByte('\n')
		}
		builder.WriteString(line)
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return flush()
}

func (i *Importer) emitProgress(progress Progress) {
	if i.onProgress != nil {
		i.onProgress(progress)
	}
}

type countingReader struct {
	reader io.Reader
	read   int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.read += int64(n)
	return n, err
}

func (r *countingReader) BytesRead() int64 {
	return r.read
}

type progressReporter struct {
	callback   func(Progress)
	format     string
	totalBytes int64
	lastRows   int
	lastAt     time.Time
}

func newProgressReporter(callback func(Progress), format string, totalBytes int64) *progressReporter {
	return &progressReporter{
		callback:   callback,
		format:     format,
		totalBytes: totalBytes,
		lastAt:     time.Now(),
	}
}

func (p *progressReporter) MaybeReport(result Result, bytesRead int64) {
	if p.callback == nil {
		return
	}
	now := time.Now()
	if result.RowsInserted < 1 {
		return
	}
	if result.RowsInserted-p.lastRows < 200 && now.Sub(p.lastAt) < 250*time.Millisecond {
		return
	}
	p.lastRows = result.RowsInserted
	p.lastAt = now
	p.callback(Progress{
		Format:       p.format,
		RowsTotal:    result.RowsTotal,
		RowsInserted: result.RowsInserted,
		BytesRead:    bytesRead,
		TotalBytes:   p.totalBytes,
	})
}

func fileSize(file *os.File) int64 {
	info, err := file.Stat()
	if err != nil {
		return 0
	}
	return info.Size()
}
