package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateServeDBPathReturnsSuggestions(t *testing.T) {
	root := t.TempDir()
	dbA := filepath.Join(root, "logs.sqlite")
	dbB := filepath.Join(root, "archive.db")
	writeTestFile(t, dbA)
	writeTestFile(t, dbB)

	err := validateServeDBPath(filepath.Join(root, "missing.sqlite"))
	if err == nil {
		t.Fatal("expected an error for missing database")
	}

	message := err.Error()
	if !strings.Contains(message, "available databases:") {
		t.Fatalf("expected suggestions in error, got %q", message)
	}
	if !strings.Contains(message, dbA) || !strings.Contains(message, dbB) {
		t.Fatalf("expected both database suggestions, got %q", message)
	}
}

func TestValidateServeDBPathAcceptsExistingFile(t *testing.T) {
	root := t.TempDir()
	dbPath := filepath.Join(root, "logs.sqlite")
	writeTestFile(t, dbPath)

	if err := validateServeDBPath(dbPath); err != nil {
		t.Fatalf("expected existing db to be accepted, got %v", err)
	}
}

func writeTestFile(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("test"), 0o644); err != nil {
		t.Fatalf("write file %s: %v", path, err)
	}
}
