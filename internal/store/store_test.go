package store

import (
	"context"
	"path/filepath"
	"testing"
)

func TestQuerySQLReadOnlyValidation(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "logs.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	ctx := context.Background()
	importID, err := st.CreateImport(ctx, "sample.log", "plain")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := st.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.InsertLog(ctx, tx, LogEntry{
		ImportID:           importID,
		Filename:           "sample.log",
		RowNumber:          1,
		SourceFormat:       "plain",
		RawMessage:         "raw",
		MessageText:        "boom",
		MessageFingerprint: "fp1",
		Level:              "ERROR",
		DynamicFields: map[string]any{
			"request_id": "req-1",
		},
		FieldSources: map[string]string{
			"request_id": "csv",
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := st.FinishImport(ctx, importID, 1, 1, 0); err != nil {
		t.Fatal(err)
	}

	if _, err := st.QuerySQL(ctx, `SELECT level, COUNT(*) AS total FROM logs GROUP BY level`); err != nil {
		t.Fatalf("expected SELECT to succeed: %v", err)
	}
	if _, err := st.QuerySQL(ctx, `UPDATE logs SET level = 'INFO'`); err == nil {
		t.Fatal("expected UPDATE to be blocked")
	}
	if _, err := st.QuerySQL(ctx, `SELECT 1; SELECT 2`); err == nil {
		t.Fatal("expected multiple statements to be blocked")
	}
}
