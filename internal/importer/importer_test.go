package importer

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"log2sql/internal/store"
)

func TestImportCSVPrefersCSVColumns(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "logs.sqlite")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	inputPath := filepath.Join(t.TempDir(), "sample.csv")
	content := "\"timestamp\",\"request_id\",\"message\"\n" +
		"\"2026-05-01T02:16:21.449Z\",\"csv-req\",\"[2026-05-01T02:16:21.449][WARN ] [request_id=log-req] [tenant_id=tenant-1] warning text\"\n"
	if err := os.WriteFile(inputPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	imp := New(st)
	if _, err := imp.ImportFile(context.Background(), inputPath, "csv"); err != nil {
		t.Fatal(err)
	}

	result, err := st.QuerySQL(context.Background(), `SELECT request_id, tenant_id, message_text FROM logs`)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected one row, got %d", len(result.Rows))
	}
	if got := result.Rows[0]["request_id"]; got != "csv-req" {
		t.Fatalf("expected CSV request_id, got %#v", got)
	}
	if got := result.Rows[0]["tenant_id"]; got != "tenant-1" {
		t.Fatalf("expected tenant_id, got %#v", got)
	}
	if got := result.Rows[0]["message_text"]; got != "warning text" {
		t.Fatalf("unexpected message_text: %#v", got)
	}
}

func TestImportPlainKeepsMultilineEntriesTogether(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "logs.sqlite")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	inputPath := filepath.Join(t.TempDir(), "sample.log")
	content := "[2026-05-01T02:16:21.449][WARN ] [request_id=-] first line\n" +
		"stack line 1\n" +
		"stack line 2\n" +
		"[2026-05-01T02:16:22.449][ERROR] [request_id=-] second entry\n"
	if err := os.WriteFile(inputPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	imp := New(st)
	result, err := imp.ImportFile(context.Background(), inputPath, "plain")
	if err != nil {
		t.Fatal(err)
	}
	if result.RowsInserted != 2 {
		t.Fatalf("expected 2 inserted rows, got %d", result.RowsInserted)
	}

	sqlResult, err := st.QuerySQL(context.Background(), `SELECT message_text FROM logs ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if got := sqlResult.Rows[0]["message_text"]; got != "first line\nstack line 1\nstack line 2" {
		t.Fatalf("unexpected multiline message: %#v", got)
	}
}
