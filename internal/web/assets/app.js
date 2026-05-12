const state = {
  session: null,
  schema: null,
  imports: [],
  page: 1,
  pageSize: 50,
  sort: "parsed_timestamp",
  order: "DESC",
  search: "",
  filename: "",
  visibleColumns: [],
  sqlTables: [],
  sqlColumns: [],
  sqlEditor: null,
  sqlRows: [],
  sqlResultColumns: [],
  sqlPage: 1,
  sqlPageSize: 50,
};

const lastDBStorageKey = "log2sql.last_db_path";

const defaultSQL = `SELECT level, COUNT(*) AS total
FROM logs
GROUP BY level
ORDER BY total DESC;`;

document.addEventListener("DOMContentLoaded", async () => {
  bindTabs();
  bindExplorerControls();
  initSQLEditor();
  bindHomeControls();
  bindHeaderControls();
  await refreshApp();
});

async function refreshApp() {
  state.session = await fetchJSON("/api/session");

  if (!state.session.has_active_db) {
    const restored = await tryRestoreLastDatabase();
    if (restored) {
      state.session = await fetchJSON("/api/session");
    }
  }

  if (state.session.has_active_db) {
    saveLastDatabasePath(state.session.current_db_path);
  } else {
    clearLastDatabasePath();
  }

  renderSession();

  if (!state.session.has_active_db) {
    resetActiveDataState();
    return;
  }

  const [schema, imports] = await Promise.all([fetchJSON("/api/schema"), fetchJSON("/api/imports")]);
  state.schema = schema;
  state.imports = imports.imports;
  state.visibleColumns = schema.default_explorer_columns.length
    ? [...schema.default_explorer_columns]
    : ["id", "message_text"];
  const sqlCatalog = buildSQLCatalog(schema);
  state.sqlTables = sqlCatalog.tables;
  state.sqlColumns = sqlCatalog.columns;
  if (state.sqlEditor) {
    state.sqlEditor.setSchema({ tables: state.sqlTables, columns: state.sqlColumns });
  }
  renderImports();
  renderColumnOptions();
  renderSQLHelp();
  await loadLogs();
}

function resetActiveDataState() {
  state.schema = null;
  state.imports = [];
  state.visibleColumns = [];
  state.sqlTables = [];
  state.sqlColumns = [];
  state.sqlRows = [];
  state.sqlResultColumns = [];
  state.sqlPage = 1;
  if (state.sqlEditor) {
    state.sqlEditor.setSchema({ tables: [], columns: [] });
  }
  renderSQLResultsPage();
  renderManagedDBs();
}

function bindTabs() {
  document.querySelectorAll(".tab").forEach((button) => {
    button.addEventListener("click", () => {
      document.querySelectorAll(".tab").forEach((item) => item.classList.remove("active"));
      document.querySelectorAll(".panel[data-app-panel='true']").forEach((item) => item.classList.remove("active"));
      button.classList.add("active");
      document.getElementById(`tab-${button.dataset.tab}`).classList.add("active");
    });
  });
}

function bindHomeControls() {
  const uploadForm = document.getElementById("upload-form");
  const dbNameInput = document.getElementById("db-name");
  const managedSelect = document.getElementById("managed-db-select");

  dbNameInput.addEventListener("input", renderDBPreview);
  uploadForm.addEventListener("submit", handleUpload);

  document.getElementById("open-managed-db").addEventListener("click", async () => {
    const value = managedSelect.value;
    if (!value) {
      setStatus("open-db-status", "Select a managed database first.", true);
      return;
    }
    await openDatabase({ managed_name: value });
  });

  document.getElementById("open-manual-db").addEventListener("click", async () => {
    const value = document.getElementById("manual-db-path").value.trim();
    if (!value) {
      setStatus("open-db-status", "Enter a database path first.", true);
      return;
    }
    await openDatabase({ path: value });
  });
}

function bindHeaderControls() {
  document.getElementById("change-db-button").addEventListener("click", () => {
    showHomePanel();
  });

  document.getElementById("back-to-explorer-button").addEventListener("click", () => {
    showDataExplorer();
  });
}

function bindExplorerControls() {
  const searchInput = document.getElementById("search-input");
  const filenameFilter = document.getElementById("filename-filter");
  const pageSize = document.getElementById("page-size");
  const refreshButton = document.getElementById("refresh-table");
  const prevButton = document.getElementById("prev-page");
  const nextButton = document.getElementById("next-page");
  const resetColumns = document.getElementById("reset-columns");
  const sqlPageSize = document.getElementById("sql-page-size");
  const sqlPrevButton = document.getElementById("sql-prev-page");
  const sqlNextButton = document.getElementById("sql-next-page");

  searchInput.addEventListener("change", async () => {
    state.search = searchInput.value.trim();
    state.page = 1;
    await loadLogs();
  });

  filenameFilter.addEventListener("change", async () => {
    state.filename = filenameFilter.value;
    state.page = 1;
    await loadLogs();
  });

  pageSize.addEventListener("change", async () => {
    state.pageSize = Number(pageSize.value || 50);
    state.page = 1;
    await loadLogs();
  });

  refreshButton.addEventListener("click", async () => {
    await refreshApp();
  });

  prevButton.addEventListener("click", async () => {
    if (state.page > 1) {
      state.page -= 1;
      await loadLogs();
    }
  });

  nextButton.addEventListener("click", async () => {
    state.page += 1;
    await loadLogs();
  });

  resetColumns.addEventListener("click", async () => {
    if (!state.schema) return;
    state.visibleColumns = [...state.schema.default_explorer_columns];
    renderColumnOptions();
    await loadLogs();
  });

  sqlPageSize.addEventListener("change", () => {
    state.sqlPageSize = Number(sqlPageSize.value || 50);
    state.sqlPage = 1;
    renderSQLResultsPage();
  });

  sqlPrevButton.addEventListener("click", () => {
    if (state.sqlPage > 1) {
      state.sqlPage -= 1;
      renderSQLResultsPage();
    }
  });

  sqlNextButton.addEventListener("click", () => {
    const totalPages = Math.max(1, Math.ceil(state.sqlRows.length / state.sqlPageSize));
    if (state.sqlPage < totalPages) {
      state.sqlPage += 1;
      renderSQLResultsPage();
    }
  });
}

function initSQLEditor() {
  const editorRoot = document.getElementById("sql-editor");
  const runButton = document.getElementById("run-sql");

  state.sqlEditor = window.Log2SQL.createSQLEditor({
    parent: editorRoot,
    initialDoc: defaultSQL,
    schema: { tables: [], columns: [] },
  });

  runButton.addEventListener("click", runSQL);
}

async function handleUpload(event) {
  event.preventDefault();

  const form = event.currentTarget;
  const submit = document.getElementById("upload-submit");
  const formData = new FormData(form);
  const dbName = String(formData.get("db_name") || "").trim();
  const targetName = previewSanitizedDBName(dbName);
  const existingDB = (state.session?.known_databases || []).find((item) => item.name === targetName);

  if (existingDB?.has_data) {
    const confirmed = window.confirm(
      `${existingDB.name} already contains ${existingDB.row_count} rows from ${existingDB.import_count} import(s).\n\nImporting this file will append new rows to that database.\n\nDo you want to continue?`
    );
    if (!confirmed) {
      setStatus("upload-status", "Import cancelled.", false);
      return;
    }
  }

  submit.disabled = true;
  setStatus("upload-status", "Uploading and importing...", false);

  try {
    const response = await fetch("/api/upload", {
      method: "POST",
      body: formData,
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || "Upload failed");
    }
    saveLastDatabasePath(payload.db_path);
    setStatus("upload-status", `Imported ${payload.rows_inserted} rows into ${payload.db_path}`, false);
    await refreshApp();
    showDataExplorer();
  } catch (error) {
    setStatus("upload-status", error.message, true);
  } finally {
    submit.disabled = false;
  }
}

async function openDatabase(payload) {
  setStatus("open-db-status", "Opening database...", false);
  try {
    const response = await fetch("/api/open-db", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await response.json();
    if (!response.ok) {
      throw new Error(data.error || "Open database failed");
    }
    saveLastDatabasePath(data.current_db_path);
    setStatus("open-db-status", `Opened ${data.current_db_path}`, false);
    await refreshApp();
    showDataExplorer();
  } catch (error) {
    setStatus("open-db-status", error.message, true);
  }
}

async function loadLogs() {
  if (!state.session?.has_active_db) return;

  const params = new URLSearchParams({
    page: String(state.page),
    page_size: String(state.pageSize),
    sort: state.sort,
    order: state.order,
    q: state.search,
    filename: state.filename,
    columns: state.visibleColumns.join(","),
  });
  const payload = await fetchJSON(`/api/logs?${params.toString()}`);
  renderLogsTable(payload);
}

async function runSQL() {
  if (!state.session?.has_active_db) {
    setSQLMeta("Open a database first.", true);
    return;
  }

  setSQLMeta("Running query...", false);

  const response = await fetch("/api/sql", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query: state.sqlEditor.getValue() }),
  });
  const payload = await response.json();
  if (!response.ok) {
    setSQLMeta(payload.error || payload.message || "Query failed", true);
    state.sqlRows = [];
    state.sqlResultColumns = [];
    state.sqlPage = 1;
    renderSQLResultsPage();
    return;
  }

  state.sqlRows = payload.rows || [];
  state.sqlResultColumns = payload.columns || [];
  state.sqlPage = 1;
  setSQLMeta(`${payload.row_count} rows in ${payload.elapsed_ms} ms`, false);
  renderSQLResultsPage();
}

function renderSession() {
  const tabs = document.getElementById("app-tabs");
  const homePanel = document.getElementById("home-panel");
  const currentDB = document.getElementById("current-db");
  const changeDBButton = document.getElementById("change-db-button");
  const backButton = document.getElementById("back-to-explorer-button");

  renderManagedDBs();
  renderDBPreview();

  if (!state.session?.has_active_db) {
    tabs.classList.add("hidden");
    homePanel.classList.add("active");
    document.querySelectorAll(".panel[data-app-panel='true']").forEach((item) => item.classList.remove("active"));
    currentDB.classList.add("hidden");
    currentDB.textContent = "";
    changeDBButton.classList.add("hidden");
    backButton.classList.add("hidden");
    return;
  }

  currentDB.classList.remove("hidden");
  currentDB.textContent = `Current DB: ${state.session.current_db_path}`;
  changeDBButton.classList.remove("hidden");
  showDataExplorer();
}

function renderManagedDBs() {
  const select = document.getElementById("managed-db-select");
  select.innerHTML = '<option value="">Select a database</option>';
  const known = state.session?.known_databases || [];
  known.forEach((db) => {
    const option = document.createElement("option");
    option.value = db.name;
    const suffix = db.has_data ? ` (${db.row_count} rows, ${db.import_count} imports)` : " (empty)";
    option.textContent = `${db.name}${suffix} — ${db.path}`;
    select.appendChild(option);
  });
}

function renderDBPreview() {
  const preview = document.getElementById("db-preview");
  const warning = document.getElementById("db-warning");
  const rawName = document.getElementById("db-name").value.trim();
  const managedDir = state.session?.managed_db_dir || "./log2sql-data/databases";

  if (!rawName) {
    preview.textContent = `Managed databases are stored in ${managedDir}`;
    warning.classList.add("hidden");
    warning.textContent = "";
    return;
  }

  const targetName = previewSanitizedDBName(rawName);
  preview.textContent = `Will be stored in ${managedDir}/${targetName}`;

  const existingDB = (state.session?.known_databases || []).find((item) => item.name === targetName);
  if (existingDB?.has_data) {
    warning.classList.remove("hidden");
    warning.textContent = `Warning: ${existingDB.name} already contains ${existingDB.row_count} rows from ${existingDB.import_count} import(s). Importing will append new rows if you confirm.`;
    return;
  }

  warning.classList.add("hidden");
  warning.textContent = "";
}

function previewSanitizedDBName(input) {
  let name = input.trim().toLowerCase();
  name = name.replace(/\.sqlite3?$/g, "").replace(/\.db$/g, "");
  name = name.replace(/\s+/g, "_").replace(/[^a-z0-9_-]/g, "");
  name = name.replace(/^[-_]+|[-_]+$/g, "");
  return `${name || "database"}.sqlite`;
}

function setStatus(id, message, isError) {
  const node = document.getElementById(id);
  node.textContent = message;
  node.dataset.error = isError ? "true" : "false";
}

function setSQLMeta(message, isError) {
  const node = document.getElementById("sql-meta");
  node.textContent = message;
  node.dataset.error = isError ? "true" : "false";
}

function renderSQLResultsPage() {
  const totalRows = state.sqlRows.length;
  const totalPages = Math.max(1, Math.ceil(totalRows / state.sqlPageSize));
  if (state.sqlPage > totalPages) {
    state.sqlPage = totalPages;
  }

  const start = (state.sqlPage - 1) * state.sqlPageSize;
  const end = start + state.sqlPageSize;
  const pageRows = state.sqlRows.slice(start, end);

  renderGenericTable(document.getElementById("sql-results"), state.sqlResultColumns, pageRows);

  const status = document.getElementById("sql-page-status");
  const prev = document.getElementById("sql-prev-page");
  const next = document.getElementById("sql-next-page");

  if (!state.sqlResultColumns.length) {
    status.textContent = "No results";
  } else {
    status.textContent = `Page ${state.sqlPage} of ${totalPages} (${totalRows} rows)`;
  }

  prev.disabled = state.sqlPage <= 1 || !state.sqlResultColumns.length;
  next.disabled = state.sqlPage >= totalPages || !state.sqlResultColumns.length;
}

function showDataExplorer() {
  document.getElementById("home-panel").classList.remove("active");
  document.getElementById("app-tabs").classList.remove("hidden");
  document.getElementById("back-to-explorer-button").classList.add("hidden");
  document.querySelectorAll(".tab").forEach((item) => item.classList.remove("active"));
  document.querySelectorAll(".panel[data-app-panel='true']").forEach((item) => item.classList.remove("active"));
  document.querySelector('.tab[data-tab="explorer"]').classList.add("active");
  document.getElementById("tab-explorer").classList.add("active");
}

function showHomePanel() {
  resetImportForm();
  document.getElementById("home-panel").classList.add("active");
  document.getElementById("app-tabs").classList.add("hidden");
  document.getElementById("back-to-explorer-button").classList.remove("hidden");
  document.querySelectorAll(".panel[data-app-panel='true']").forEach((item) => item.classList.remove("active"));
  document.querySelectorAll(".tab").forEach((item) => item.classList.remove("active"));
}

function resetImportForm() {
  document.getElementById("upload-form").reset();
  document.getElementById("db-warning").classList.add("hidden");
  document.getElementById("db-warning").textContent = "";
  setStatus("upload-status", "", false);
  renderDBPreview();
}

function saveLastDatabasePath(path) {
  if (!path) return;
  try {
    window.localStorage.setItem(lastDBStorageKey, path);
  } catch (_error) {
    // Ignore storage failures so the app still works in restricted browsers.
  }
}

function loadLastDatabasePath() {
  try {
    return window.localStorage.getItem(lastDBStorageKey) || "";
  } catch (_error) {
    return "";
  }
}

function clearLastDatabasePath() {
  try {
    window.localStorage.removeItem(lastDBStorageKey);
  } catch (_error) {
    // Ignore storage failures so the app still works in restricted browsers.
  }
}

async function tryRestoreLastDatabase() {
  const lastPath = loadLastDatabasePath().trim();
  if (!lastPath) {
    return false;
  }

  try {
    const response = await fetch("/api/open-db", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ path: lastPath }),
    });
    if (!response.ok) {
      clearLastDatabasePath();
      return false;
    }
    return true;
  } catch (_error) {
    return false;
  }
}

function renderImports() {
  const filter = document.getElementById("filename-filter");
  filter.innerHTML = '<option value="">All files</option>';
  const seen = new Set();
  state.imports.forEach((item) => {
    if (seen.has(item.filename)) return;
    seen.add(item.filename);
    const option = document.createElement("option");
    option.value = item.filename;
    option.textContent = item.filename;
    filter.appendChild(option);
  });
}

function renderColumnOptions() {
  const wrap = document.getElementById("column-options");
  wrap.innerHTML = "";
  if (!state.schema) return;

  const fixedColumns = ["parsed_timestamp", "level", "request_id", "tenant_id", "class", "method", "filename", "message_text"];
  const allColumns = [...new Set([...fixedColumns, ...state.schema.columns.map((item) => item.name)])];

  allColumns.forEach((column) => {
    const label = document.createElement("label");
    label.className = "column-option";

    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.checked = state.visibleColumns.includes(column);
    checkbox.addEventListener("change", async () => {
      if (checkbox.checked) {
        if (!state.visibleColumns.includes(column)) {
          state.visibleColumns.push(column);
        }
      } else {
        state.visibleColumns = state.visibleColumns.filter((item) => item !== column);
      }
      await loadLogs();
    });

    const text = document.createElement("span");
    text.textContent = column;
    label.appendChild(checkbox);
    label.appendChild(text);
    wrap.appendChild(label);
  });
}

function renderLogsTable(payload) {
  renderGenericTable(document.getElementById("logs-table"), payload.columns, payload.rows, true);
  const totalPages = Math.max(1, Math.ceil(payload.total / payload.page_size));
  if (state.page > totalPages) {
    state.page = totalPages;
  }
  document.getElementById("page-status").textContent = `Page ${payload.page} of ${totalPages} (${payload.total} rows)`;
}

function renderGenericTable(table, columns, rows, sortable = false) {
  const thead = table.querySelector("thead");
  const tbody = table.querySelector("tbody");
  thead.innerHTML = "";
  tbody.innerHTML = "";

  const headerRow = document.createElement("tr");
  columns.forEach((column) => {
    const th = document.createElement("th");
    th.classList.add(columnClassName(column));
    th.textContent = column;
    if (sortable) {
      th.classList.add("sortable");
      if (state.sort === column) {
        th.dataset.active = state.order;
      }
      th.addEventListener("click", async () => {
        if (state.sort === column) {
          state.order = state.order === "ASC" ? "DESC" : "ASC";
        } else {
          state.sort = column;
          state.order = "ASC";
        }
        await loadLogs();
      });
    }
    headerRow.appendChild(th);
  });
  thead.appendChild(headerRow);

  rows.forEach((row) => {
    const tr = document.createElement("tr");
    columns.forEach((column) => {
      const td = document.createElement("td");
      td.classList.add(columnClassName(column));
      const value = row[column];
      td.textContent = value === null || value === undefined ? "" : String(value);
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
}

function columnClassName(column) {
  return `col-${column.replace(/[^a-zA-Z0-9_-]/g, "-")}`;
}

function buildSQLCatalog(schema) {
  const tables = [...new Set(schema.tables)];
  const columns = [...new Set([
    "id", "import_id", "filename", "row_number", "source_format", "raw_message",
    "message_text", "message_fingerprint", "parsed_timestamp", "level", "created_at",
    ...schema.columns.map((item) => item.name),
  ])].sort((left, right) => left.localeCompare(right));
  return { tables, columns };
}

function renderSQLHelp() {
  const wrap = document.getElementById("sql-help");
  if (!state.schema) {
    wrap.innerHTML = "";
    return;
  }

  const mainTable = state.sqlTables.includes("logs") ? "logs" : (state.sqlTables[0] || "logs");
  const metadataTables = state.sqlTables.filter((item) => item !== mainTable);
  const columns = state.sqlColumns;

  wrap.innerHTML = `
    <div class="sql-help-section">
      <div class="sql-help-label">Main table</div>
      <code class="sql-help-code">${escapeHTML(mainTable)}</code>
    </div>
    <div class="sql-help-section">
      <div class="sql-help-label">Other tables</div>
      <div class="sql-chip-row">
        ${metadataTables.length ? metadataTables.map((item) => `<code class="sql-chip">${escapeHTML(item)}</code>`).join("") : '<span class="sql-help-empty">None</span>'}
      </div>
    </div>
    <div class="sql-help-section">
      <div class="sql-help-label">Available columns</div>
      <div class="sql-chip-row">
        ${columns.map((item) => `<code class="sql-chip">${escapeHTML(item)}</code>`).join("")}
      </div>
    </div>
  `;
}

function escapeHTML(value) {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

async function fetchJSON(url) {
  const response = await fetch(url);
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error || payload.message || `Request failed: ${response.status}`);
  }
  return payload;
}
