package web

import (
	"bytes"
	"context"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"log2sql/internal/store"
)

func TestHTTPHandlersWithActiveDB(t *testing.T) {
	root := t.TempDir()
	dbPath := filepath.Join(root, "logs.sqlite")
	manager, err := NewManager(filepath.Join(root, "managed"), dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	if err := seedTestDB(t, dbPath); err != nil {
		t.Fatal(err)
	}
	if err := manager.OpenDatabase(dbPath); err != nil {
		t.Fatal(err)
	}

	handler := NewHandler(manager)

	req := httptest.NewRequest(http.MethodGet, "/api/session", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("session status: %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/schema", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("schema status: %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/logs?columns=level,message_text,request_id", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("logs status: %d", rec.Code)
	}

	body, _ := json.Marshal(map[string]string{"query": "SELECT level, request_id FROM logs"})
	req = httptest.NewRequest(http.MethodPost, "/api/sql", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("sql status: %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestHTTPHandlersWithoutActiveDB(t *testing.T) {
	root := t.TempDir()
	manager, err := NewManager(filepath.Join(root, "managed"), "")
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	handler := NewHandler(manager)
	req := httptest.NewRequest(http.MethodGet, "/api/schema", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected conflict without active db, got %d", rec.Code)
	}
}

func TestUploadAndOpenFlows(t *testing.T) {
	root := t.TempDir()
	managedDir := filepath.Join(root, "managed")
	manager, err := NewManager(managedDir, "")
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	handler := NewHandler(manager)

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("db_name", "prod errors"); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteField("format", "csv"); err != nil {
		t.Fatal(err)
	}
	part, err := writer.CreateFormFile("file", "logs.csv")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = part.Write([]byte("\"timestamp\",\"message\"\n\"2026-05-01T02:16:21.449Z\",\"[2026-05-01T02:16:21.449][WARN ] [request_id=req-1] hello\"\n"))
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/upload", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("upload status: %d body=%s", rec.Code, rec.Body.String())
	}

	managedDBPath := filepath.Join(managedDir, "prod_errors.sqlite")
	if _, err := os.Stat(managedDBPath); err != nil {
		t.Fatalf("expected managed DB to exist: %v", err)
	}

	manualDBPath := filepath.Join(root, "manual.sqlite")
	if err := seedTestDB(t, manualDBPath); err != nil {
		t.Fatal(err)
	}

	openBody, _ := json.Marshal(map[string]string{"path": manualDBPath})
	req = httptest.NewRequest(http.MethodPost, "/api/open-db", bytes.NewReader(openBody))
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("open manual db status: %d body=%s", rec.Code, rec.Body.String())
	}

	session, err := manager.SessionInfo()
	if err != nil {
		t.Fatal(err)
	}
	if session.CurrentDBPath != manualDBPath {
		t.Fatalf("expected manual db to become active, got %q", session.CurrentDBPath)
	}
	if len(session.KnownDatabases) != 1 || !strings.HasSuffix(session.KnownDatabases[0].Path, "prod_errors.sqlite") {
		t.Fatalf("expected only managed db in known list, got %#v", session.KnownDatabases)
	}
}

func seedTestDB(t *testing.T, dbPath string) error {
	t.Helper()
	st, err := store.Open(dbPath)
	if err != nil {
		return err
	}
	defer st.Close()

	ctx := context.Background()
	importID, err := st.CreateImport(ctx, "sample.csv", "csv")
	if err != nil {
		return err
	}
	tx, err := st.Begin(ctx)
	if err != nil {
		return err
	}
	if err := st.InsertLog(ctx, tx, store.LogEntry{
		ImportID:           importID,
		Filename:           "sample.csv",
		RowNumber:          1,
		SourceFormat:       "csv",
		RawMessage:         "raw",
		MessageText:        "boom",
		MessageFingerprint: "fp1",
		ParsedTimestamp:    "2026-05-01T02:16:21.449Z",
		Level:              "ERROR",
		DynamicFields: map[string]any{
			"request_id": "req-1",
			"tenant_id":  "tenant-1",
		},
		FieldSources: map[string]string{
			"request_id": "csv",
			"tenant_id":  "csv",
		},
	}); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return st.FinishImport(ctx, importID, 1, 1, 0)
}
