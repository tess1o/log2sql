package web

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"log2sql/internal/store"
)

func TestManagerNoActiveDB(t *testing.T) {
	root := t.TempDir()
	manager, err := NewManager(filepath.Join(root, "managed"), "")
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	err = manager.WithStore(func(_ *store.Store) error { return nil })
	if !errors.Is(err, errNoActiveDatabase) {
		t.Fatalf("expected no active db error, got %v", err)
	}
}

func TestManagerListsManagedDatabases(t *testing.T) {
	root := t.TempDir()
	managedDir := filepath.Join(root, "managed")
	manager, err := NewManager(managedDir, "")
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	if err := os.WriteFile(filepath.Join(managedDir, "a.sqlite"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(managedDir, "b.db"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(managedDir, "note.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	list, err := manager.ListManagedDatabases()
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 managed dbs, got %d", len(list))
	}
	for _, item := range list {
		if item.HasData {
			t.Fatalf("expected empty managed db metadata, got has_data=true for %#v", item)
		}
	}
}

func TestSanitizeDBName(t *testing.T) {
	name, err := sanitizeDBName(" Prod Errors!.sqlite ")
	if err != nil {
		t.Fatal(err)
	}
	if name != "prod_errors.sqlite" {
		t.Fatalf("unexpected sanitized name: %q", name)
	}
}
