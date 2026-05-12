package web

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"log2sql/internal/importer"
	"log2sql/internal/store"
)

var errNoActiveDatabase = errors.New("no active database selected")

type DBInfo struct {
	Name        string `json:"name"`
	Path        string `json:"path"`
	HasData     bool   `json:"has_data"`
	ImportCount int    `json:"import_count"`
	RowCount    int    `json:"row_count"`
}

type SessionInfo struct {
	HasActiveDB    bool     `json:"has_active_db"`
	CurrentDBPath  string   `json:"current_db_path"`
	ManagedDBDir   string   `json:"managed_db_dir"`
	KnownDatabases []DBInfo `json:"known_databases"`
}

type Manager struct {
	mu           sync.RWMutex
	activePath   string
	activeStore  *store.Store
	managedDBDir string
}

func NewManager(managedDBDir string, initialDBPath string) (*Manager, error) {
	if err := os.MkdirAll(managedDBDir, 0o755); err != nil {
		return nil, err
	}

	manager := &Manager{managedDBDir: managedDBDir}
	if strings.TrimSpace(initialDBPath) != "" {
		if err := manager.OpenDatabase(initialDBPath); err != nil {
			return nil, err
		}
	}
	return manager, nil
}

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.activeStore == nil {
		return nil
	}
	err := m.activeStore.Close()
	m.activeStore = nil
	m.activePath = ""
	return err
}

func (m *Manager) WithStore(fn func(*store.Store) error) error {
	m.mu.RLock()
	active := m.activeStore
	m.mu.RUnlock()

	if active == nil {
		return errNoActiveDatabase
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.activeStore == nil {
		return errNoActiveDatabase
	}
	return fn(m.activeStore)
}

func (m *Manager) SessionInfo() (SessionInfo, error) {
	known, err := m.ListManagedDatabases()
	if err != nil {
		return SessionInfo{}, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	return SessionInfo{
		HasActiveDB:    m.activeStore != nil,
		CurrentDBPath:  m.activePath,
		ManagedDBDir:   m.managedDBDir,
		KnownDatabases: known,
	}, nil
}

func (m *Manager) ListManagedDatabases() ([]DBInfo, error) {
	entries, err := os.ReadDir(m.managedDBDir)
	if err != nil {
		return nil, err
	}

	var databases []DBInfo
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !looksLikeSQLiteFile(entry.Name()) {
			continue
		}
		fullPath := filepath.Join(m.managedDBDir, entry.Name())
		stats, err := inspectDatabase(fullPath)
		if err != nil {
			return nil, err
		}
		databases = append(databases, DBInfo{
			Name:        entry.Name(),
			Path:        fullPath,
			HasData:     stats.RowCount > 0 || stats.ImportCount > 0,
			ImportCount: stats.ImportCount,
			RowCount:    stats.RowCount,
		})
	}
	sort.Slice(databases, func(i, j int) bool {
		return databases[i].Name < databases[j].Name
	})
	return databases, nil
}

func (m *Manager) OpenManagedDatabase(name string) (string, error) {
	filename, err := sanitizeDBName(name)
	if err != nil {
		return "", err
	}
	target := filepath.Join(m.managedDBDir, filename)
	if _, err := os.Stat(target); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("managed database %q does not exist", filename)
		}
		return "", err
	}
	return target, m.OpenDatabase(target)
}

func (m *Manager) OpenManualDatabase(path string) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return errors.New("database path is required")
	}
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("database %q does not exist", path)
		}
		return fmt.Errorf("failed to access database %q: %w", path, err)
	}
	if info.IsDir() {
		return fmt.Errorf("database path %q is a directory, not a SQLite file", path)
	}
	return m.OpenDatabase(path)
}

func (m *Manager) OpenDatabase(path string) error {
	nextStore, err := store.Open(path)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	oldStore := m.activeStore
	m.activeStore = nextStore
	m.activePath = path
	if oldStore != nil {
		return oldStore.Close()
	}
	return nil
}

func (m *Manager) ManagedDBPath(name string) (string, error) {
	filename, err := sanitizeDBName(name)
	if err != nil {
		return "", err
	}
	return filepath.Join(m.managedDBDir, filename), nil
}

func sanitizeDBName(input string) (string, error) {
	name := strings.ToLower(strings.TrimSpace(input))
	name = strings.ReplaceAll(name, ".sqlite", "")
	name = strings.ReplaceAll(name, ".sqlite3", "")
	name = strings.ReplaceAll(name, ".db", "")
	name = strings.ReplaceAll(name, " ", "_")

	var builder strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z':
			builder.WriteRune(r)
		case r >= '0' && r <= '9':
			builder.WriteRune(r)
		case r == '-' || r == '_':
			builder.WriteRune(r)
		}
	}

	cleaned := strings.Trim(builder.String(), "-_")
	if cleaned == "" {
		return "", errors.New("database name is required")
	}
	return cleaned + ".sqlite", nil
}

func looksLikeSQLiteFile(name string) bool {
	lower := strings.ToLower(name)
	return strings.HasSuffix(lower, ".sqlite") || strings.HasSuffix(lower, ".sqlite3") || strings.HasSuffix(lower, ".db")
}

func importUploadedFile(ctx context.Context, manager *Manager, tempFilePath, dbPath, format string) (importResult, error) {
	st, err := store.Open(dbPath)
	if err != nil {
		return importResult{}, err
	}
	defer st.Close()

	imp := importer.New(st)
	result, err := imp.ImportFile(ctx, tempFilePath, format)
	if err != nil {
		return importResult{}, err
	}

	if err := manager.OpenDatabase(dbPath); err != nil {
		return importResult{}, err
	}

	return importResult{
		DBPath:       dbPath,
		RowsInserted: result.RowsInserted,
		Format:       result.Format,
		ImportID:     result.ImportID,
	}, nil
}

func inspectDatabase(path string) (store.DatabaseStats, error) {
	st, err := store.Open(path)
	if err != nil {
		return store.DatabaseStats{}, err
	}
	defer st.Close()
	return st.DatabaseStats(context.Background())
}

type importResult struct {
	DBPath       string `json:"db_path"`
	RowsInserted int    `json:"rows_inserted"`
	Format       string `json:"format"`
	ImportID     int64  `json:"import_id"`
}
