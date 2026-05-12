package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

var nonAlphaNumRE = regexp.MustCompile(`[^a-z0-9_]+`)

var defaultExplorerColumns = []string{
	"parsed_timestamp",
	"level",
	"message_text",
}

type Store struct {
	db *sql.DB

	mu            sync.RWMutex
	keyToColumn   map[string]string
	columnToKey   map[string]string
	dynamicColumn map[string]struct{}
}

type Tx = sql.Tx

type LogEntry struct {
	ImportID           int64
	Filename           string
	RowNumber          int
	SourceFormat       string
	RawMessage         string
	MessageText        string
	MessageFingerprint string
	ParsedTimestamp    string
	Level              string
	DynamicFields      map[string]any
	FieldSources       map[string]string
}

type ColumnInfo struct {
	Name       string `json:"name"`
	Original   string `json:"original"`
	SourceType string `json:"source_type"`
	TypeHint   string `json:"type_hint"`
}

type SchemaInfo struct {
	Tables                 []string     `json:"tables"`
	Columns                []ColumnInfo `json:"columns"`
	DefaultExplorerColumns []string     `json:"default_explorer_columns"`
}

type ImportInfo struct {
	ID           int64  `json:"id"`
	Filename     string `json:"filename"`
	SourceFormat string `json:"source_format"`
	StartedAt    string `json:"started_at"`
	FinishedAt   string `json:"finished_at"`
	RowsTotal    int    `json:"rows_total"`
	RowsInserted int    `json:"rows_inserted"`
	ParseErrors  int    `json:"parse_errors"`
}

type LogsPage struct {
	Page     int              `json:"page"`
	PageSize int              `json:"page_size"`
	Total    int              `json:"total"`
	Columns  []string         `json:"columns"`
	Rows     []map[string]any `json:"rows"`
}

type SQLResult struct {
	Columns   []string         `json:"columns"`
	Rows      []map[string]any `json:"rows"`
	RowCount  int              `json:"row_count"`
	ElapsedMS int64            `json:"elapsed_ms"`
}

type DatabaseStats struct {
	ImportCount int `json:"import_count"`
	RowCount    int `json:"row_count"`
}

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetConnMaxLifetime(0)

	st := &Store{
		db:            db,
		keyToColumn:   make(map[string]string),
		columnToKey:   make(map[string]string),
		dynamicColumn: make(map[string]struct{}),
	}

	if err := st.bootstrap(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	if err := st.loadSchemaCache(context.Background()); err != nil {
		db.Close()
		return nil, err
	}

	return st, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Begin(ctx context.Context) (*Tx, error) {
	return s.db.BeginTx(ctx, nil)
}

func (s *Store) CreateImport(ctx context.Context, filename, sourceFormat string) (int64, error) {
	result, err := s.db.ExecContext(ctx, `
		INSERT INTO imports (filename, source_format, started_at, rows_total, rows_inserted, parse_errors)
		VALUES (?, ?, ?, 0, 0, 0)
	`, filename, sourceFormat, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (s *Store) FinishImport(ctx context.Context, importID int64, rowsTotal, rowsInserted, parseErrors int) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE imports
		SET finished_at = ?, rows_total = ?, rows_inserted = ?, parse_errors = ?
		WHERE id = ?
	`, time.Now().UTC().Format(time.RFC3339Nano), rowsTotal, rowsInserted, parseErrors, importID)
	return err
}

func (s *Store) InsertLog(ctx context.Context, tx *Tx, entry LogEntry) error {
	canonicalFields, err := s.ensureDynamicColumns(ctx, tx, entry.ImportID, entry.DynamicFields, entry.FieldSources)
	if err != nil {
		return err
	}

	columns := []string{
		"import_id",
		"filename",
		"row_number",
		"source_format",
		"raw_message",
		"message_text",
		"message_fingerprint",
		"parsed_timestamp",
		"level",
	}
	args := []any{
		entry.ImportID,
		entry.Filename,
		entry.RowNumber,
		entry.SourceFormat,
		entry.RawMessage,
		nullableString(entry.MessageText),
		nullableString(entry.MessageFingerprint),
		nullableString(entry.ParsedTimestamp),
		nullableString(entry.Level),
	}

	dynamicColumns := make([]string, 0, len(canonicalFields))
	for column := range canonicalFields {
		dynamicColumns = append(dynamicColumns, column)
	}
	sort.Strings(dynamicColumns)

	for _, column := range dynamicColumns {
		columns = append(columns, column)
		args = append(args, canonicalFields[column])
	}

	placeholders := make([]string, len(columns))
	quoted := make([]string, len(columns))
	for idx, column := range columns {
		placeholders[idx] = "?"
		quoted[idx] = quoteIdent(column)
	}

	query := fmt.Sprintf("INSERT INTO logs (%s) VALUES (%s)", strings.Join(quoted, ", "), strings.Join(placeholders, ", "))
	_, err = tx.ExecContext(ctx, query, args...)
	return err
}

func (s *Store) Schema(ctx context.Context) (SchemaInfo, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT canonical_name, original_key, source_type, type_hint
		FROM schema_columns
		ORDER BY canonical_name
	`)
	if err != nil {
		return SchemaInfo{}, err
	}
	defer rows.Close()

	columns := make([]ColumnInfo, 0)
	for rows.Next() {
		var info ColumnInfo
		if err := rows.Scan(&info.Name, &info.Original, &info.SourceType, &info.TypeHint); err != nil {
			return SchemaInfo{}, err
		}
		columns = append(columns, info)
	}
	if err := rows.Err(); err != nil {
		return SchemaInfo{}, err
	}

	defaults := make([]string, 0, len(defaultExplorerColumns))
	for _, column := range defaultExplorerColumns {
		if s.columnExists(column) {
			defaults = append(defaults, column)
		}
	}

	return SchemaInfo{
		Tables:                 []string{"logs", "imports", "schema_columns"},
		Columns:                columns,
		DefaultExplorerColumns: defaults,
	}, nil
}

func (s *Store) ListImports(ctx context.Context) ([]ImportInfo, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, filename, source_format, started_at, COALESCE(finished_at, ''), rows_total, rows_inserted, parse_errors
		FROM imports
		ORDER BY id DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []ImportInfo
	for rows.Next() {
		var item ImportInfo
		if err := rows.Scan(&item.ID, &item.Filename, &item.SourceFormat, &item.StartedAt, &item.FinishedAt, &item.RowsTotal, &item.RowsInserted, &item.ParseErrors); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (s *Store) DatabaseStats(ctx context.Context) (DatabaseStats, error) {
	var stats DatabaseStats
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM imports`).Scan(&stats.ImportCount); err != nil {
		return DatabaseStats{}, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM logs`).Scan(&stats.RowCount); err != nil {
		return DatabaseStats{}, err
	}
	return stats, nil
}

func (s *Store) QueryLogs(ctx context.Context, page, pageSize int, sortColumn, sortOrder, search, filename string, requestedColumns []string) (LogsPage, error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 50
	}
	if pageSize > 500 {
		pageSize = 500
	}

	if !s.columnExists(sortColumn) {
		sortColumn = "parsed_timestamp"
		if !s.columnExists(sortColumn) {
			sortColumn = "id"
		}
	}
	sortOrder = strings.ToUpper(strings.TrimSpace(sortOrder))
	if sortOrder != "ASC" {
		sortOrder = "DESC"
	}

	selectedColumns := requestedColumns
	if len(selectedColumns) == 0 {
		selectedColumns = append([]string(nil), defaultExplorerColumns...)
	}

	filteredColumns := make([]string, 0, len(selectedColumns))
	for _, column := range selectedColumns {
		if s.columnExists(column) {
			filteredColumns = append(filteredColumns, column)
		}
	}
	if len(filteredColumns) == 0 {
		filteredColumns = []string{"id", "message_text"}
	}

	whereParts := make([]string, 0, 2)
	args := make([]any, 0, 8)

	if filename != "" {
		whereParts = append(whereParts, "filename = ?")
		args = append(args, filename)
	}

	if trimmed := strings.TrimSpace(search); trimmed != "" {
		searchColumns := s.allColumns()
		searchParts := make([]string, 0, len(searchColumns))
		for _, column := range searchColumns {
			searchParts = append(searchParts, fmt.Sprintf("COALESCE(CAST(%s AS TEXT), '') LIKE ?", quoteIdent(column)))
			args = append(args, "%"+trimmed+"%")
		}
		whereParts = append(whereParts, "("+strings.Join(searchParts, " OR ")+")")
	}

	whereClause := ""
	if len(whereParts) > 0 {
		whereClause = " WHERE " + strings.Join(whereParts, " AND ")
	}

	var total int
	countQuery := "SELECT COUNT(*) FROM logs" + whereClause
	if err := s.db.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return LogsPage{}, err
	}

	queryArgs := append([]any(nil), args...)
	queryArgs = append(queryArgs, pageSize, (page-1)*pageSize)

	quotedColumns := make([]string, len(filteredColumns))
	for idx, column := range filteredColumns {
		quotedColumns[idx] = quoteIdent(column)
	}

	query := fmt.Sprintf(
		"SELECT %s FROM logs%s ORDER BY %s %s LIMIT ? OFFSET ?",
		strings.Join(quotedColumns, ", "),
		whereClause,
		quoteIdent(sortColumn),
		sortOrder,
	)
	rows, err := s.db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return LogsPage{}, err
	}
	defer rows.Close()

	data, err := scanRows(rows)
	if err != nil {
		return LogsPage{}, err
	}

	return LogsPage{
		Page:     page,
		PageSize: pageSize,
		Total:    total,
		Columns:  filteredColumns,
		Rows:     data,
	}, nil
}

func (s *Store) QuerySQL(ctx context.Context, query string) (SQLResult, error) {
	if err := validateReadOnlySQL(query); err != nil {
		return SQLResult{}, err
	}

	start := time.Now()
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return SQLResult{}, err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "PRAGMA query_only = ON"); err != nil {
		return SQLResult{}, err
	}
	defer conn.ExecContext(context.Background(), "PRAGMA query_only = OFF")

	rows, err := conn.QueryContext(ctx, strings.TrimSpace(query))
	if err != nil {
		return SQLResult{}, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return SQLResult{}, err
	}

	data, err := scanRows(rows)
	if err != nil {
		return SQLResult{}, err
	}

	return SQLResult{
		Columns:   columns,
		Rows:      data,
		RowCount:  len(data),
		ElapsedMS: time.Since(start).Milliseconds(),
	}, nil
}

func (s *Store) bootstrap(ctx context.Context) error {
	statements := []string{
		`PRAGMA busy_timeout = 5000`,
		`PRAGMA journal_mode = WAL`,
		`CREATE TABLE IF NOT EXISTS logs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			import_id INTEGER NOT NULL,
			filename TEXT NOT NULL,
			row_number INTEGER NOT NULL,
			source_format TEXT NOT NULL,
			raw_message TEXT NOT NULL,
			message_text TEXT,
			message_fingerprint TEXT,
			parsed_timestamp TEXT,
			level TEXT,
			created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS imports (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			filename TEXT NOT NULL,
			source_format TEXT NOT NULL,
			started_at TEXT NOT NULL,
			finished_at TEXT,
			rows_total INTEGER NOT NULL DEFAULT 0,
			rows_inserted INTEGER NOT NULL DEFAULT 0,
			parse_errors INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS schema_columns (
			canonical_name TEXT PRIMARY KEY,
			original_key TEXT NOT NULL,
			source_type TEXT NOT NULL,
			type_hint TEXT NOT NULL,
			first_import_id INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_logs_import_id ON logs(import_id)`,
		`CREATE INDEX IF NOT EXISTS idx_logs_filename ON logs(filename)`,
		`CREATE INDEX IF NOT EXISTS idx_logs_level ON logs(level)`,
		`CREATE INDEX IF NOT EXISTS idx_logs_parsed_timestamp ON logs(parsed_timestamp)`,
		`CREATE INDEX IF NOT EXISTS idx_logs_message_fingerprint ON logs(message_fingerprint)`,
	}

	for _, stmt := range statements {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) loadSchemaCache(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, fixed := range fixedColumns() {
		s.dynamicColumn[fixed] = struct{}{}
	}

	rows, err := s.db.QueryContext(ctx, `SELECT canonical_name, original_key FROM schema_columns`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var canonical string
		var original string
		if err := rows.Scan(&canonical, &original); err != nil {
			return err
		}
		s.keyToColumn[original] = canonical
		s.columnToKey[canonical] = original
		s.dynamicColumn[canonical] = struct{}{}
	}
	return rows.Err()
}

func (s *Store) ensureDynamicColumns(ctx context.Context, tx *Tx, importID int64, fields map[string]any, sources map[string]string) (map[string]any, error) {
	if len(fields) == 0 {
		return nil, nil
	}

	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	result := make(map[string]any, len(fields))
	for _, originalKey := range keys {
		canonical, isNew := s.resolveColumnName(originalKey)
		if isNew {
			if _, err := tx.ExecContext(ctx, fmt.Sprintf("ALTER TABLE logs ADD COLUMN %s", quoteIdent(canonical))); err != nil {
				return nil, err
			}
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO schema_columns (canonical_name, original_key, source_type, type_hint, first_import_id)
				VALUES (?, ?, ?, ?, ?)
			`, canonical, originalKey, safeSourceType(sources[originalKey]), inferTypeHint(fields[originalKey]), importID); err != nil {
				return nil, err
			}
		}
		result[canonical] = normalizeDBValue(fields[originalKey])
	}
	return result, nil
}

func (s *Store) resolveColumnName(original string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if canonical, exists := s.keyToColumn[original]; exists {
		return canonical, false
	}

	base := sanitizeColumnName(original)
	if base == "" {
		base = "field"
	}

	candidate := base
	for idx := 2; ; idx++ {
		if _, fixed := s.dynamicColumn[candidate]; !fixed {
			break
		}
		if existingOriginal, exists := s.columnToKey[candidate]; exists && existingOriginal == original {
			break
		}
		candidate = fmt.Sprintf("%s__%d", base, idx)
	}

	s.keyToColumn[original] = candidate
	s.columnToKey[candidate] = original
	s.dynamicColumn[candidate] = struct{}{}
	return candidate, true
}

func (s *Store) columnExists(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, exists := s.dynamicColumn[name]
	return exists
}

func (s *Store) allColumns() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	columns := make([]string, 0, len(s.dynamicColumn))
	for column := range s.dynamicColumn {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	return columns
}

func fixedColumns() []string {
	return []string{
		"id",
		"import_id",
		"filename",
		"row_number",
		"source_format",
		"raw_message",
		"message_text",
		"message_fingerprint",
		"parsed_timestamp",
		"level",
		"created_at",
	}
}

func sanitizeColumnName(input string) string {
	normalized := strings.ToLower(strings.TrimSpace(input))
	normalized = strings.ReplaceAll(normalized, ".", "__")
	normalized = strings.ReplaceAll(normalized, "-", "_")
	normalized = nonAlphaNumRE.ReplaceAllString(normalized, "_")
	normalized = strings.Trim(normalized, "_")
	normalized = strings.ReplaceAll(normalized, "___", "__")
	normalized = strings.ReplaceAll(normalized, "____", "__")
	return normalized
}

func quoteIdent(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func inferTypeHint(value any) string {
	switch value.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case int, int8, int16, int32, int64, uint, uint64:
		return "integer"
	case float32, float64:
		return "real"
	default:
		return "text"
	}
}

func safeSourceType(value string) string {
	switch value {
	case "csv", "prefix", "json":
		return value
	default:
		return "prefix"
	}
}

func normalizeDBValue(value any) any {
	switch typed := value.(type) {
	case nil:
		return nil
	case string:
		return nullableString(typed)
	default:
		return typed
	}
}

func nullableString(value string) any {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return trimmed
}

func scanRows(rows *sql.Rows) ([]map[string]any, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var result []map[string]any
	for rows.Next() {
		raw := make([]any, len(columns))
		dest := make([]any, len(columns))
		for i := range raw {
			dest[i] = &raw[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}

		row := make(map[string]any, len(columns))
		for idx, column := range columns {
			row[column] = convertScannedValue(raw[idx])
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func convertScannedValue(value any) any {
	switch typed := value.(type) {
	case nil:
		return nil
	case []byte:
		return string(typed)
	case time.Time:
		return typed.Format(time.RFC3339Nano)
	default:
		return typed
	}
}

func validateReadOnlySQL(query string) error {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return errors.New("query is required")
	}
	if hasMultipleStatements(trimmed) {
		return errors.New("only one SQL statement is allowed")
	}

	firstKeyword := leadingKeyword(trimmed)
	switch firstKeyword {
	case "SELECT", "WITH", "EXPLAIN":
		return nil
	default:
		return errors.New("only SELECT, WITH, and EXPLAIN statements are allowed")
	}
}

func leadingKeyword(query string) string {
	var builder strings.Builder
	inLineComment := false
	inBlockComment := false

	for i := 0; i < len(query); i++ {
		ch := query[i]
		next := byte(0)
		if i+1 < len(query) {
			next = query[i+1]
		}

		if inLineComment {
			if ch == '\n' {
				inLineComment = false
			}
			continue
		}
		if inBlockComment {
			if ch == '*' && next == '/' {
				inBlockComment = false
				i++
			}
			continue
		}
		if ch == '-' && next == '-' {
			inLineComment = true
			i++
			continue
		}
		if ch == '/' && next == '*' {
			inBlockComment = true
			i++
			continue
		}
		if builder.Len() == 0 && (ch == ' ' || ch == '\n' || ch == '\t' || ch == '\r') {
			continue
		}
		if ch == ' ' || ch == '\n' || ch == '\t' || ch == '\r' || ch == '(' {
			break
		}
		builder.WriteByte(ch)
	}
	return strings.ToUpper(builder.String())
}

func hasMultipleStatements(query string) bool {
	inSingle := false
	inDouble := false
	inBacktick := false
	inLineComment := false
	inBlockComment := false
	lastContentAfterSemicolon := false

	for i := 0; i < len(query); i++ {
		ch := query[i]
		next := byte(0)
		if i+1 < len(query) {
			next = query[i+1]
		}

		if inLineComment {
			if ch == '\n' {
				inLineComment = false
			}
			continue
		}
		if inBlockComment {
			if ch == '*' && next == '/' {
				inBlockComment = false
				i++
			}
			continue
		}
		if inSingle {
			if ch == '\'' {
				inSingle = false
			}
			continue
		}
		if inDouble {
			if ch == '"' {
				inDouble = false
			}
			continue
		}
		if inBacktick {
			if ch == '`' {
				inBacktick = false
			}
			continue
		}

		switch {
		case ch == '-' && next == '-':
			inLineComment = true
			i++
		case ch == '/' && next == '*':
			inBlockComment = true
			i++
		case ch == '\'':
			inSingle = true
		case ch == '"':
			inDouble = true
		case ch == '`':
			inBacktick = true
		case ch == ';':
			for j := i + 1; j < len(query); j++ {
				if !strings.ContainsRune(" \n\r\t;", rune(query[j])) {
					lastContentAfterSemicolon = true
					break
				}
			}
			if lastContentAfterSemicolon {
				return true
			}
		}
	}
	return false
}

func MustJSON(value any) string {
	bytes, _ := json.Marshal(value)
	return string(bytes)
}
