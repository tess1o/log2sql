package web

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"log2sql/internal/store"
)

//go:embed assets/*
var assets embed.FS

type Server struct {
	manager *Manager
}

func NewHandler(manager *Manager) http.Handler {
	server := &Server{manager: manager}
	staticFS, err := fs.Sub(assets, "assets")
	if err != nil {
		panic(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", server.handleIndex)
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.FS(staticFS))))
	mux.HandleFunc("/api/session", server.handleSession)
	mux.HandleFunc("/api/open-db", server.handleOpenDB)
	mux.HandleFunc("/api/upload", server.handleUpload)
	mux.HandleFunc("/api/schema", server.handleSchema)
	mux.HandleFunc("/api/imports", server.handleImports)
	mux.HandleFunc("/api/logs", server.handleLogs)
	mux.HandleFunc("/api/sql", server.handleSQL)
	return mux
}

func ListenAndServe(addr string, manager *Manager) error {
	return http.ListenAndServe(addr, NewHandler(manager))
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	data, err := assets.ReadFile("assets/index.html")
	if err != nil {
		http.Error(w, "failed to load UI", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(data)
}

func (s *Server) handleSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	info, err := s.manager.SessionInfo()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, info)
}

func (s *Server) handleOpenDB(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload struct {
		ManagedName string `json:"managed_name"`
		Path        string `json:"path"`
	}
	if err := decodeJSONBody(r, &payload); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	switch {
	case strings.TrimSpace(payload.ManagedName) != "":
		if _, err := s.manager.OpenManagedDatabase(payload.ManagedName); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
	case strings.TrimSpace(payload.Path) != "":
		if err := s.manager.OpenManualDatabase(payload.Path); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
	default:
		writeError(w, http.StatusBadRequest, errors.New("managed_name or path is required"))
		return
	}

	info, err := s.manager.SessionInfo()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, info)
}

func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("failed to parse upload form: %w", err))
		return
	}

	dbName := r.FormValue("db_name")
	format := r.FormValue("format")
	if strings.TrimSpace(format) == "" {
		format = "auto"
	}

	targetDBPath, err := s.manager.ManagedDBPath(dbName)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, errors.New("file upload is required"))
		return
	}
	defer file.Close()

	tempFile, err := os.CreateTemp("", "log2sql-upload-*"+filepath.Ext(header.Filename))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	tempPath := tempFile.Name()
	defer os.Remove(tempPath)

	if _, err := io.Copy(tempFile, file); err != nil {
		tempFile.Close()
		writeError(w, http.StatusInternalServerError, fmt.Errorf("failed to save upload: %w", err))
		return
	}
	if err := tempFile.Close(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	result, err := importUploadedFile(r.Context(), s.manager, tempPath, targetDBPath, format)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleSchema(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var schema store.SchemaInfo
	err := s.manager.WithStore(func(st *store.Store) error {
		var innerErr error
		schema, innerErr = st.Schema(r.Context())
		return innerErr
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, schema)
}

func (s *Server) handleImports(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var imports []store.ImportInfo
	err := s.manager.WithStore(func(st *store.Store) error {
		var innerErr error
		imports, innerErr = st.ListImports(r.Context())
		return innerErr
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"imports": imports})
}

func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	page := parseInt(r.URL.Query().Get("page"), 1)
	pageSize := parseInt(r.URL.Query().Get("page_size"), 50)
	sortColumn := r.URL.Query().Get("sort")
	sortOrder := r.URL.Query().Get("order")
	search := r.URL.Query().Get("q")
	filename := r.URL.Query().Get("filename")

	var columns []string
	if raw := strings.TrimSpace(r.URL.Query().Get("columns")); raw != "" {
		for _, item := range strings.Split(raw, ",") {
			item = strings.TrimSpace(item)
			if item != "" {
				columns = append(columns, item)
			}
		}
	}

	var pageData store.LogsPage
	err := s.manager.WithStore(func(st *store.Store) error {
		var innerErr error
		pageData, innerErr = st.QueryLogs(r.Context(), page, pageSize, sortColumn, sortOrder, search, filename, columns)
		return innerErr
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, pageData)
}

func (s *Server) handleSQL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload struct {
		Query string `json:"query"`
	}
	if err := decodeJSONBody(r, &payload); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	var result store.SQLResult
	err := s.manager.WithStore(func(st *store.Store) error {
		var innerErr error
		result, innerErr = st.QuerySQL(r.Context(), payload.Query)
		return innerErr
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func decodeJSONBody(r *http.Request, dest any) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, dest); err != nil {
		return fmt.Errorf("invalid JSON payload")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]any{"error": err.Error()})
}

func writeStoreError(w http.ResponseWriter, err error) {
	if errors.Is(err, errNoActiveDatabase) {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":   err.Error(),
			"code":    "NO_ACTIVE_DB",
			"message": "Open an existing database or import a log file first.",
		})
		return
	}
	writeError(w, http.StatusBadRequest, err)
}

func parseInt(value string, fallback int) int {
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}
