const state = {
  connection: null,
  savedConnections: [],
  overview: null,
  objects: [],
  filter: "all",
  selectedObject: null,
  objectSummary: null,
  dataSlice: null,
  rowPage: 1,
  pageSize: 50,
  rowSearch: "",
  rowSort: "",
  rowDirection: "ASC",
  llmStatus: null,
  selectedModel: "qwen3:8b",
  chatMessages: [],
  chatBusy: false,
  semanticResults: [],
  semanticBuildBusy: false,
  semanticSearchBusy: false,
  objectCache: {},
  rowsLoading: false,
  sectionLoading: {
    schema: false,
    relationships: false,
    indexes: false,
    definition: false
  },
  panelState: {
    schema: false,
    relationships: false,
    indexes: false,
    definition: false
  },
  selectionToken: 0
};

const elements = {
  connectionStatus: document.querySelector("#connectionStatus"),
  connectForm: document.querySelector("#connectForm"),
  connectButton: document.querySelector("#connectButton"),
  focusConnectButton: document.querySelector("#focusConnectButton"),
  savedConnectionSelect: document.querySelector("#savedConnectionSelect"),
  savedConnectionHint: document.querySelector("#savedConnectionHint"),
  host: document.querySelector("#host"),
  port: document.querySelector("#port"),
  user: document.querySelector("#user"),
  password: document.querySelector("#password"),
  database: document.querySelector("#database"),
  objectSearch: document.querySelector("#objectSearch"),
  filterCountAll: document.querySelector("#filterCountAll"),
  filterCountTables: document.querySelector("#filterCountTables"),
  filterCountViews: document.querySelector("#filterCountViews"),
  filterCountInferred: document.querySelector("#filterCountInferred"),
  objectFilterSummary: document.querySelector("#objectFilterSummary"),
  globalSearch: document.querySelector("#globalSearch"),
  objectList: document.querySelector("#objectList"),
  searchResults: document.querySelector("#searchResults"),
  metricObjects: document.querySelector("#metricObjects"),
  metricRelationships: document.querySelector("#metricRelationships"),
  metricRows: document.querySelector("#metricRows"),
  metricSize: document.querySelector("#metricSize"),
  objectTypeLabel: document.querySelector("#objectTypeLabel"),
  objectName: document.querySelector("#objectName"),
  objectMeta: document.querySelector("#objectMeta"),
  objectStats: document.querySelector("#objectStats"),
  columnsTable: document.querySelector("#columnsTable"),
  indexesTable: document.querySelector("#indexesTable"),
  definitionBlock: document.querySelector("#definitionBlock"),
  relationshipMap: document.querySelector("#relationshipMap"),
  rowSearch: document.querySelector("#rowSearch"),
  pageSizeSelect: document.querySelector("#pageSizeSelect"),
  rowStatus: document.querySelector("#rowStatus"),
  pageLabel: document.querySelector("#pageLabel"),
  previousPageButton: document.querySelector("#previousPageButton"),
  nextPageButton: document.querySelector("#nextPageButton"),
  rowsTable: document.querySelector("#rowsTable"),
  exportCsvButton: document.querySelector("#exportCsvButton"),
  exportJsonButton: document.querySelector("#exportJsonButton"),
  profileColumnButton: document.querySelector("#profileColumnButton"),
  columnProfile: document.querySelector("#columnProfile"),
  sqlEditor: document.querySelector("#sqlEditor"),
  runQueryButton: document.querySelector("#runQueryButton"),
  loadSampleQueryButton: document.querySelector("#loadSampleQueryButton"),
  sqlStatus: document.querySelector("#sqlStatus"),
  sqlResultsTable: document.querySelector("#sqlResultsTable"),
  llmStatusPill: document.querySelector("#llmStatusPill"),
  refreshModelsButton: document.querySelector("#refreshModelsButton"),
  llmModelSelect: document.querySelector("#llmModelSelect"),
  llmContextHint: document.querySelector("#llmContextHint"),
  chatTranscript: document.querySelector("#chatTranscript"),
  chatForm: document.querySelector("#chatForm"),
  chatInput: document.querySelector("#chatInput"),
  chatSendButton: document.querySelector("#chatSendButton"),
  chatClearButton: document.querySelector("#chatClearButton"),
  semanticStatus: document.querySelector("#semanticStatus"),
  buildSemanticIndexButton: document.querySelector("#buildSemanticIndexButton"),
  semanticBuildInfo: document.querySelector("#semanticBuildInfo"),
  semanticSearchForm: document.querySelector("#semanticSearchForm"),
  semanticQuery: document.querySelector("#semanticQuery"),
  semanticSearchButton: document.querySelector("#semanticSearchButton"),
  semanticResults: document.querySelector("#semanticResults"),
  toast: document.querySelector("#toast")
};

let toastTimer = null;

async function request(path, options = {}) {
  const response = await fetch(path, {
    headers: {
      "Content-Type": "application/json"
    },
    ...options
  });

  if (!response.ok) {
    let message = `Request failed with status ${response.status}`;
    try {
      const payload = await response.json();
      message = payload.error || message;
    } catch {
      // Ignore JSON parsing failures.
    }
    throw new Error(message);
  }

  const contentType = response.headers.get("content-type") || "";
  if (contentType.includes("application/json")) {
    return response.json();
  }
  return response.text();
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function showToast(message) {
  elements.toast.textContent = message;
  elements.toast.classList.add("is-visible");
  window.clearTimeout(toastTimer);
  toastTimer = window.setTimeout(() => {
    elements.toast.classList.remove("is-visible");
  }, 3200);
}

function formatNumber(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "-";
  }
  return new Intl.NumberFormat().format(Number(value));
}

function formatSize(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "-";
  }
  return `${Number(value).toFixed(2)} MB`;
}

function formatValue(value) {
  if (value === null || value === undefined || value === "") {
    return "null";
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

function setConnectionStatus(connection) {
  state.connection = connection;
  elements.connectionStatus.textContent = connection.connected
    ? `${connection.user}@${connection.host}:${connection.port}`
    : "Disconnected";
  elements.connectionStatus.classList.toggle("connected", Boolean(connection.connected));
  elements.connectButton.textContent = connection.connected ? "Refresh Schema View" : "Connect To Schema";
}

function setConnectButtonBusy(isBusy, label) {
  elements.connectButton.disabled = isBusy;
  elements.connectButton.textContent = label;
}

function fillConnectionForm(connection) {
  elements.host.value = connection.host || "127.0.0.1";
  elements.port.value = connection.port || 3306;
  elements.user.value = connection.user || "";
  elements.database.value = connection.database || "";
}

function renderSavedConnections(connections) {
  if (!connections.length) {
    elements.savedConnectionSelect.innerHTML = `<option value="">No saved MySQL Shell connections found</option>`;
    elements.savedConnectionSelect.disabled = true;
    elements.savedConnectionHint.textContent = "No saved MySQL Shell connection metadata was found on this machine.";
    return;
  }

  elements.savedConnectionSelect.disabled = false;
  elements.savedConnectionSelect.innerHTML = [
    `<option value="">Load a saved connection preset...</option>`,
    ...connections.map(
      (connection) => `
        <option value="${escapeHtml(connection.id)}" ${connection.preferred ? "selected" : ""}>
          ${escapeHtml(connection.caption)} | ${escapeHtml(connection.user)}@${escapeHtml(connection.host)}:${escapeHtml(connection.port)}
        </option>
      `
    )
  ].join("");

  const preferred = connections.find((connection) => connection.preferred) || connections[0];
  if (preferred) {
    fillConnectionForm(preferred);
    elements.savedConnectionHint.textContent = `${preferred.caption} loaded from MySQL Shell for VS Code metadata. Enter the password, then connect.`;
  }
}

function renderMetrics() {
  const totals = state.overview?.totals;
  if (!totals) {
    elements.metricObjects.textContent = "-";
    elements.metricRelationships.textContent = "-";
    elements.metricRows.textContent = "-";
    elements.metricSize.textContent = "-";
    elements.filterCountAll.textContent = "-";
    elements.filterCountTables.textContent = "-";
    elements.filterCountViews.textContent = "-";
    elements.filterCountInferred.textContent = "-";
    renderObjectFilterSummary();
    return;
  }

  elements.metricObjects.textContent = formatNumber(totals.tables + totals.views);
  elements.metricRelationships.textContent = formatNumber(totals.relationships);
  elements.metricRows.textContent = formatNumber(totals.estimatedRows);
  elements.metricSize.textContent = formatSize(totals.sizeMb);
  elements.filterCountAll.textContent = formatNumber(totals.tables + totals.views);
  elements.filterCountTables.textContent = formatNumber(totals.tables);
  elements.filterCountViews.textContent = formatNumber(totals.views);
  elements.filterCountInferred.textContent = formatNumber(totals.inferredViews || 0);
  renderObjectFilterSummary();
}

function renderObjectFilterSummary(filteredCount = null, searchTerm = "") {
  const totals = state.overview?.totals;
  if (!totals) {
    elements.objectFilterSummary.textContent = "Connect to load the schema explorer.";
    return;
  }

  const visibleObjects = filteredCount ?? state.objects.length;
  const visibleCount = formatNumber(visibleObjects);
  const summaries = {
    all: `${formatNumber(totals.tables + totals.views)} database objects in the connected schema. ${formatNumber(totals.inferredViews || 0)} placeholder views also have Schema Atlas inferred versions.`,
    tables: `${formatNumber(totals.tables)} base tables in the connected schema. ${visibleCount} shown in the explorer.`,
    views: `${formatNumber(totals.views)} raw database views in the connected schema. ${visibleCount} shown in the explorer.`,
    inferred: `${formatNumber(totals.inferredViews || 0)} Schema Atlas inferred views built from placeholder database views. ${visibleCount} shown in the explorer.`
  };

  const suffix = searchTerm ? ` Filtered by "${searchTerm}".` : "";
  elements.objectFilterSummary.textContent = `${summaries[state.filter] || summaries.all}${suffix}`;
}

function renderChatContent(value) {
  return escapeHtml(value).replaceAll("\n", "<br>");
}

function renderSemanticStatus() {
  const semantic = state.llmStatus?.semanticSearch;
  if (!semantic) {
    elements.semanticStatus.textContent =
      "Semantic search is currently disabled. Schema Atlas is using direct tools, inferred views, and live read-only SQL instead.";
    elements.semanticBuildInfo.innerHTML = `
      <strong>No local embedding index has been built yet.</strong>
      <span>Build the local index to enable semantic retrieval over the schema snapshot, inferred notes, and inferred view mappings.</span>
      <span><strong>Typical build time:</strong> Under a minute for schema-only content, or a few minutes if you also index notes and documentation.</span>
      <span><strong>Lifecycle:</strong> Usually a one-time local build, then rebuild only when the schema snapshot or notes change.</span>
      <span><strong>Storage:</strong> A local vector index on disk is enough. A graph database is optional and not required.</span>
    `;
    elements.buildSemanticIndexButton.textContent = state.semanticBuildBusy ? "Building Index..." : "Build Local Index";
    elements.buildSemanticIndexButton.disabled = state.semanticBuildBusy;
    elements.semanticSearchButton.disabled = true;
    return;
  }

  elements.semanticStatus.textContent = semantic.reason;
  elements.semanticBuildInfo.innerHTML = `
    <strong>${escapeHtml(semantic.indexed ? "Local embedding index is available." : "No local embedding index has been built yet.")}</strong>
    <span>${escapeHtml(semantic.indexed ? "Semantic retrieval is enabled for Ask Atlas and the search panel below." : "Build the local index to enable semantic retrieval for Ask Atlas and the search panel below.")}</span>
    <span><strong>Typical build time:</strong> ${escapeHtml(semantic.buildTimeEstimate || "Depends on the amount of schema notes and documentation being indexed.")}</span>
    <span><strong>Lifecycle:</strong> ${escapeHtml(semantic.lifecycle || "Usually a one-time build, then occasional rebuilds when source documents change.")}</span>
    <span><strong>Storage:</strong> ${escapeHtml(semantic.storage || "A local vector index on disk is typically enough; a graph database is not required.")}</span>
    <span><strong>Embedding model:</strong> ${escapeHtml(semantic.embedModelResolvedName || semantic.embedModel || "unknown")} ${semantic.embedModelInstalled ? "(installed)" : "(not installed)"}</span>
    <span><strong>Index path:</strong> ${escapeHtml(semantic.indexPath || "not set")}</span>
    <span><strong>Documents:</strong> ${escapeHtml(formatNumber(semantic.documentCount || 0))}${semantic.lastBuiltAt ? ` | <strong>Last built:</strong> ${escapeHtml(semantic.lastBuiltAt)}` : ""}${semantic.stale ? " | <strong>Status:</strong> stale, rebuild recommended" : ""}</span>
  `;
  elements.buildSemanticIndexButton.textContent = state.semanticBuildBusy
    ? "Building Index..."
    : semantic.indexed
      ? "Rebuild Local Index"
      : "Build Local Index";
  elements.buildSemanticIndexButton.disabled = Boolean(state.semanticBuildBusy);
  elements.semanticSearchButton.disabled = Boolean(state.semanticSearchBusy || !semantic.indexed);
}

function renderSemanticResults() {
  if (state.semanticSearchBusy) {
    elements.semanticResults.innerHTML = `<div class="empty-state">Searching the local semantic index...</div>`;
    return;
  }

  if (!state.semanticResults.length) {
    const semantic = state.llmStatus?.semanticSearch;
    elements.semanticResults.innerHTML = semantic?.indexed
      ? `<div class="empty-state">Run a semantic query to retrieve related schema notes, objects, and inferred view mappings.</div>`
      : `<div class="empty-state">Build the local index to enable semantic retrieval over schema notes and inferred mappings.</div>`;
    return;
  }

  elements.semanticResults.innerHTML = state.semanticResults
    .map(
      (result) => `
        <article class="semantic-result">
          <div class="semantic-result-header">
            <div>
              <strong>${escapeHtml(result.title || result.objectName || result.id || "Untitled result")}</strong>
              <div class="semantic-result-meta">
                ${escapeHtml(result.kind || "result")} | ${escapeHtml(result.sourceLabel || "semantic index")}
              </div>
            </div>
            <span class="semantic-score">${escapeHtml(Number(result.score || 0).toFixed(3))}</span>
          </div>
          <p>${escapeHtml(result.snippet || "")}</p>
          ${
            result.objectName
              ? `<button class="ghost-button semantic-open-button" data-object-name="${escapeHtml(result.objectName)}" type="button">Open ${escapeHtml(result.objectName)}</button>`
              : ""
          }
        </article>
      `
    )
    .join("");
}

function renderLlmStatus() {
  const status = state.llmStatus;
  const models = status?.models || [];
  const currentModel = state.selectedModel;

  elements.llmModelSelect.innerHTML = models.length
    ? models
        .map((model) => {
          const statusText = model.installed ? "installed" : "not installed";
          return `
            <option value="${escapeHtml(model.id)}" ${model.id === currentModel ? "selected" : ""}>
              ${escapeHtml(model.label)} · ${escapeHtml(statusText)}
            </option>
          `;
        })
        .join("")
    : `
        <option value="${escapeHtml(currentModel)}">
          ${escapeHtml(currentModel)} · status unknown
        </option>
      `;

  if (!models.some((model) => model.id === currentModel) && models[0]?.id) {
    state.selectedModel = models[0].id;
    elements.llmModelSelect.value = state.selectedModel;
  }

  if (!status) {
    elements.llmStatusPill.textContent = "Ollama status unknown";
    elements.llmStatusPill.classList.remove("connected");
    elements.chatSendButton.disabled = state.chatBusy;
    renderSemanticStatus();
    renderSemanticResults();
    return;
  }

  const selectedModelStatus = models.find((model) => model.id === state.selectedModel);
  if (status.ollamaReachable) {
    const installedCount = (status.models || []).filter((model) => model.installed).length;
    elements.llmStatusPill.textContent = `Ollama ready · ${installedCount} recommended models installed`;
    elements.llmStatusPill.classList.add("connected");
  } else {
    elements.llmStatusPill.textContent = "Ollama offline";
    elements.llmStatusPill.classList.remove("connected");
  }

  elements.chatSendButton.disabled = Boolean(
    state.chatBusy ||
      !status.ollamaReachable ||
      !models.length ||
      !selectedModelStatus ||
      !selectedModelStatus.installed
  );
  renderSemanticStatus();
  renderSemanticResults();
}

function renderLlmContext() {
  elements.llmContextHint.textContent = state.selectedObject
    ? `Current object context: ${state.selectedObject}`
    : "Current object context: none selected";
}

function renderChatTranscript() {
  if (!state.chatMessages.length) {
    elements.chatTranscript.innerHTML = `
        <article class="chat-message assistant">
        <div class="chat-role">Ask Atlas</div>
        <div class="chat-bubble">
          Ask a plain-English question about the connected database. This assistant is read-only and will use local tools to inspect tables, inferred views, column profiles, and likely joins.
        </div>
      </article>
    `;
    return;
  }

  elements.chatTranscript.innerHTML = state.chatMessages
    .map((message) => {
      const toolTrace = Array.isArray(message.toolTrace) && message.toolTrace.length
        ? `
            <div class="chat-tools">
              <div class="chat-tools-label">Tools used</div>
              ${message.toolTrace
                .map(
                  (tool) => `
                    <div class="chat-tool-pill" title="${escapeHtml(JSON.stringify(tool.arguments || {}))}">
                      ${escapeHtml(tool.name)}
                    </div>
                  `
                )
                .join("")}
            </div>
          `
        : "";

      return `
        <article class="chat-message ${escapeHtml(message.role)}">
          <div class="chat-role">${message.role === "user" ? "You" : "Ask Atlas"}</div>
          <div class="chat-bubble">
            ${renderChatContent(message.content)}
            ${toolTrace}
          </div>
        </article>
      `;
    })
    .join("");

  elements.chatTranscript.scrollTop = elements.chatTranscript.scrollHeight;
}

function getObjectCache(name) {
  if (!name) {
    return null;
  }
  if (!state.objectCache[name]) {
    state.objectCache[name] = {
      summary: null,
      schema: null,
      relationships: null,
      indexes: null,
      definition: null,
      dataSlices: {}
    };
  }
  return state.objectCache[name];
}

function getSelectedCache() {
  return getObjectCache(state.selectedObject);
}

function getObjectStub(name) {
  if (!name) {
    return null;
  }
  const object = state.objects.find((item) => item.name === name);
  if (!object) {
    return { name };
  }
  return {
    name: object.name,
    type: object.type,
    isInferred: object.isInferred,
    engine: object.engine,
    estimatedRows: object.estimatedRows,
    sizeMb: object.sizeMb,
    updatedAt: object.updatedAt,
    comment: object.comment
  };
}

function getDataSliceCacheKey() {
  return JSON.stringify({
    page: state.rowPage,
    pageSize: state.pageSize,
    search: state.rowSearch,
    sort: state.rowSort,
    direction: state.rowDirection
  });
}

function createTable(columns, rows, options = {}) {
  if (!columns.length) {
    return `<tbody><tr><td>${escapeHtml(options.emptyMessage || "No data available.")}</td></tr></tbody>`;
  }

  const header = columns
    .map((column) => {
      const classes = [];
      if (options.sortable) {
        classes.push("sortable");
      }
      if (column.headerTitle) {
        classes.push("has-tooltip");
      }
      const classAttr = classes.length ? ` class="${classes.join(" ")}"` : "";
      const sortableAttr = options.sortable ? ` data-sort-column="${escapeHtml(column.key)}"` : "";
      const titleAttr = column.headerTitle
        ? ` title="${escapeHtml(column.headerTitle)}" aria-label="${escapeHtml(column.headerTitle)}"`
        : "";
      const label = escapeHtml(column.label || column.key);
      return `<th${classAttr}${sortableAttr}${titleAttr}>${label}</th>`;
    })
    .join("");

  const body = rows.length
    ? rows
        .map(
          (row) => `
            <tr>
              ${columns.map((column) => `<td>${escapeHtml(formatValue(row[column.key]))}</td>`).join("")}
            </tr>
          `
        )
        .join("")
    : `<tr><td colspan="${columns.length}">${escapeHtml(options.emptyMessage || "No rows to display.")}</td></tr>`;

  return `<thead><tr>${header}</tr></thead><tbody>${body}</tbody>`;
}

function renderEmptyTable(table, message) {
  table.innerHTML = `<tbody><tr><td>${escapeHtml(message)}</td></tr></tbody>`;
}

function renderObjectList() {
  const search = elements.objectSearch.value.trim().toLowerCase();
  const filtered = state.objects.filter((item) => item.name.toLowerCase().includes(search));
  renderObjectFilterSummary(filtered.length, search);

  if (!filtered.length) {
    elements.objectList.innerHTML = `<div class="empty-state">No objects match the current filter.</div>`;
    return;
  }

  elements.objectList.innerHTML = filtered
    .map(
      (item) => `
        <button class="object-button ${state.selectedObject === item.name ? "is-active" : ""}" data-object-name="${escapeHtml(item.name)}" type="button">
          <div class="object-list-title">
            <span>${escapeHtml(item.name)}</span>
            <span class="object-tag">${item.isInferred ? "Inferred" : item.type === "VIEW" ? "View" : "Table"}</span>
          </div>
          <div class="object-list-meta">
            ${
              item.isInferred
                ? "built from base tables | row count on demand"
                : `est rows ${formatNumber(item.estimatedRows)} | size ${formatSize(item.sizeMb)}`
            }
          </div>
        </button>
      `
    )
    .join("");
}

function renderPanelState(section) {
  const card = document.querySelector(`[data-panel-card="${section}"]`);
  const button = document.querySelector(`[data-toggle-panel="${section}"]`);
  const body = document.querySelector(`#${section}PanelBody`);
  const isOpen = Boolean(state.panelState[section]);

  if (!card || !button || !body) {
    return;
  }

  card.classList.toggle("is-collapsed", !isOpen);
  body.hidden = !isOpen;
  button.textContent = isOpen ? "Hide" : "Show";
  button.setAttribute("aria-expanded", isOpen ? "true" : "false");
}

function renderPanelStates() {
  Object.keys(state.panelState).forEach(renderPanelState);
}

function renderObjectHero() {
  renderLlmContext();
  const cache = getSelectedCache();
  const summary = cache?.summary || state.objectSummary;

  if (!summary) {
    if (state.connection?.connected) {
      elements.objectTypeLabel.textContent = "Schema Connected";
      elements.objectName.textContent = "Select a table or view";
      elements.objectMeta.textContent =
        "The database connection is live. Choose an object from the left-hand explorer to inspect live rows first, then open metadata panels as needed.";
    } else {
      elements.objectTypeLabel.textContent = "Select a table or view";
      elements.objectName.textContent = "Waiting for connection";
      elements.objectMeta.textContent = "Connect to the database, then choose an object from the left-hand explorer.";
    }
    elements.objectStats.innerHTML = "";
    return;
  }

  const schemaColumns = cache?.schema?.columns || state.dataSlice?.columns || [];
  const indexesCount = summary.type === "VIEW" ? 0 : cache?.indexes?.indexes?.length;

  elements.objectTypeLabel.textContent = summary.isInferred ? "Inferred View" : summary.type === "VIEW" ? "View" : "Table";
  elements.objectName.textContent = summary.name || state.selectedObject;
  const meta = [];
  if (summary.isInferred) {
    meta.push("Built in Schema Atlas from base tables");
  }
  meta.push(`Engine ${summary.engine || "-"}`);
  if (summary.estimatedRows === null || summary.estimatedRows === undefined) {
    meta.push("Row count on demand");
  } else {
    meta.push(`Estimated rows ${formatNumber(summary.estimatedRows)}`);
  }
  if (summary.sizeMb !== null && summary.sizeMb !== undefined) {
    meta.push(`Approx size ${formatSize(summary.sizeMb)}`);
  }
  if (summary.comment && summary.isInferred) {
    meta.push(summary.comment);
  }
  elements.objectMeta.textContent = meta.join(" | ");

  const stats = [
    { label: "Updated", value: summary.updatedAt ? new Date(summary.updatedAt).toLocaleString() : "n/a" },
    { label: "Columns", value: schemaColumns.length ? formatNumber(schemaColumns.length) : "On demand" },
    { label: "Indexes", value: indexesCount === undefined ? "On demand" : formatNumber(indexesCount) },
    { label: "Rows", value: state.dataSlice ? formatNumber(state.dataSlice.rows.length) : "Loading" }
  ];

  elements.objectStats.innerHTML = stats
    .map(
      (item) => `
        <div class="mini-stat">
          <span>${escapeHtml(item.label)}</span>
          <strong>${escapeHtml(item.value)}</strong>
        </div>
      `
    )
    .join("");
}

function renderRelationshipMap(relationships) {
  const outgoing = relationships?.outgoing || [];
  const incoming = relationships?.incoming || [];

  if (!state.selectedObject) {
    elements.relationshipMap.className = "relationship-map empty-state";
    elements.relationshipMap.textContent = "No object selected yet.";
    return;
  }

  elements.relationshipMap.className = "relationship-map";
  elements.relationshipMap.innerHTML = `
    <div class="relationship-grid">
      <div class="relationship-column">
        ${
          incoming.length
            ? incoming
                .map(
                  (item) => `
                    <div class="relationship-card">
                      <strong>${escapeHtml(item.sourceTable)}</strong>
                      <div>${escapeHtml(item.sourceColumn)} -> ${escapeHtml(item.referencedColumn)}</div>
                    </div>
                  `
                )
                .join("")
            : `<div class="relationship-card">No incoming references found.</div>`
        }
      </div>
      <div class="relationship-center">
        <div class="eyebrow">Selected Object</div>
        <h3>${escapeHtml(state.selectedObject)}</h3>
        <p>${escapeHtml(String(incoming.length))} incoming | ${escapeHtml(String(outgoing.length))} outgoing</p>
      </div>
      <div class="relationship-column">
        ${
          outgoing.length
            ? outgoing
                .map(
                  (item) => `
                    <div class="relationship-card">
                      <strong>${escapeHtml(item.referencedTable)}</strong>
                      <div>${escapeHtml(item.columnName)} -> ${escapeHtml(item.referencedColumn)}</div>
                    </div>
                  `
                )
                .join("")
            : `<div class="relationship-card">No outgoing references found.</div>`
        }
      </div>
    </div>
  `;
}

function renderSchemaPanel() {
  const cache = getSelectedCache();
  const schema = cache?.schema;

  if (!state.selectedObject) {
    elements.columnProfile.innerHTML = "";
    elements.columnsTable.innerHTML = "";
    return;
  }

  if (state.sectionLoading.schema && !schema?.columns?.length) {
    renderEmptyTable(elements.columnsTable, "Loading column metadata...");
    return;
  }

  const columns = schema?.columns || [];
  elements.columnsTable.innerHTML = createTable(
    [
      { key: "name", label: "Column" },
      { key: "columnType", label: "Type" },
      { key: "isNullable", label: "Nullable" },
      { key: "columnDefault", label: "Default" },
      { key: "columnKey", label: "Key" },
      { key: "extra", label: "Extra" }
    ],
    columns,
    { emptyMessage: "No column metadata available yet." }
  );
}

function renderIndexesPanel() {
  const cache = getSelectedCache();
  const summary = cache?.summary || state.objectSummary;

  if (!state.selectedObject) {
    elements.indexesTable.innerHTML = "";
    return;
  }

  if (summary?.type === "VIEW") {
    renderEmptyTable(
      elements.indexesTable,
      summary?.isInferred ? "Inferred views are composed in Schema Atlas and do not expose database indexes." : "Views do not expose table indexes."
    );
    return;
  }

  if (state.sectionLoading.indexes && !cache?.indexes) {
    renderEmptyTable(elements.indexesTable, "Loading indexes...");
    return;
  }

  const indexes = cache?.indexes?.indexes || [];
  elements.indexesTable.innerHTML = createTable(
    [
      { key: "Key_name", label: "Index" },
      { key: "Column_name", label: "Column" },
      { key: "Seq_in_index", label: "Order" },
      { key: "Non_unique", label: "Non Unique" },
      { key: "Index_type", label: "Type" }
    ],
    indexes,
    { emptyMessage: "No indexes found for this object." }
  );
}

function renderDefinitionPanel() {
  const cache = getSelectedCache();

  if (!state.selectedObject) {
    elements.definitionBlock.textContent = "";
    return;
  }

  if (state.sectionLoading.definition && !cache?.definition) {
    elements.definitionBlock.textContent = "Loading definition...";
    return;
  }

  elements.definitionBlock.textContent = cache?.definition?.definition?.statement || "No definition loaded yet.";
}

function renderRelationshipsPanel() {
  const cache = getSelectedCache();

  if (!state.selectedObject) {
    renderRelationshipMap(null);
    return;
  }

  if (state.sectionLoading.relationships && !cache?.relationships) {
    elements.relationshipMap.className = "relationship-map empty-state";
    elements.relationshipMap.textContent = "Loading relationship map...";
    return;
  }

  renderRelationshipMap(cache?.relationships?.relationships || null);
}

function renderRowData() {
  const slice = state.dataSlice;
  const cache = getSelectedCache();
  const summary = cache?.summary || state.objectSummary;

  if (!state.selectedObject) {
    elements.rowsTable.innerHTML = "";
    elements.rowStatus.textContent = "Choose an object to browse its data.";
    elements.pageLabel.textContent = "Page 1";
    elements.previousPageButton.disabled = true;
    elements.nextPageButton.disabled = true;
    return;
  }

  if (state.rowsLoading && !slice) {
    renderEmptyTable(elements.rowsTable, `Loading rows for ${state.selectedObject}...`);
    elements.rowStatus.textContent = "Loading live data...";
    elements.pageLabel.textContent = `Page ${state.rowPage}`;
    elements.previousPageButton.disabled = state.rowPage <= 1;
    elements.nextPageButton.disabled = true;
    return;
  }

  const rows = slice?.rows || [];
  const dataColumns = slice?.columns?.length
    ? slice.columns.map((column) => ({
        key: column.name,
        label: column.name,
        headerTitle:
          summary?.isInferred && column.sourceSummary
            ? `Source: ${column.sourceSummary}`
            : ""
      }))
    : (rows[0] ? Object.keys(rows[0]).map((name) => ({ key: name, label: name })) : []);
  const columns = dataColumns;
  elements.rowsTable.innerHTML = createTable(columns, rows, {
    sortable: true,
    emptyMessage: `No rows returned for ${summary?.name || state.selectedObject}.`
  });

  if (!slice) {
    elements.rowStatus.textContent = "Choose an object to browse its data.";
    elements.pageLabel.textContent = "Page 1";
    elements.previousPageButton.disabled = true;
    elements.nextPageButton.disabled = true;
    return;
  }

  const totalLabel =
    slice.total === null || slice.total === undefined
      ? "row count on demand"
      : slice.totalIsEstimate
        ? `${formatNumber(slice.total)} estimated rows`
        : `${formatNumber(slice.total)} matching rows`;
  elements.rowStatus.textContent = `${totalLabel} | ${formatNumber(rows.length)} rows loaded on this page`;
  elements.pageLabel.textContent = `Page ${slice.page}`;
  elements.previousPageButton.disabled = slice.page <= 1;
  elements.nextPageButton.disabled = rows.length < slice.pageSize;
}

function renderSearchResults(payload) {
  if (!payload || (!payload.tables.length && !payload.columns.length)) {
    elements.searchResults.innerHTML = `<div class="empty-state">Search tables or columns to map the schema faster.</div>`;
    return;
  }

  const tableHits = payload.tables.map(
    (item) => `
      <button class="search-hit" data-object-name="${escapeHtml(item.objectName)}" type="button">
        <strong>${escapeHtml(item.objectName)}</strong>
        <small>${item.objectType === "VIEW" ? "View" : "Table"}</small>
      </button>
    `
  );
  const columnHits = payload.columns.map(
    (item) => `
      <button class="search-hit" data-object-name="${escapeHtml(item.objectName)}" type="button">
        <strong>${escapeHtml(item.objectName)}.${escapeHtml(item.columnName)}</strong>
        <small>${escapeHtml(item.columnType)}</small>
      </button>
    `
  );

  elements.searchResults.innerHTML = [...tableHits, ...columnHits].join("");
}

function renderPanels() {
  renderPanelStates();
  renderSchemaPanel();
  renderRelationshipsPanel();
  renderIndexesPanel();
  renderDefinitionPanel();
}

function togglePanel(section) {
  state.panelState[section] = !state.panelState[section];
  renderPanelState(section);
  if (state.panelState[section]) {
    loadPanelSection(section).catch((error) => showToast(error.message));
  }
}

function updateSortFromHeader(target) {
  const column = target.dataset.sortColumn;
  if (!column) {
    return;
  }

  if (state.rowSort === column) {
    state.rowDirection = state.rowDirection === "ASC" ? "DESC" : "ASC";
  } else {
    state.rowSort = column;
    state.rowDirection = "ASC";
  }

  loadSelectedObjectData().catch((error) => showToast(error.message));
}

async function loadConfig() {
  const config = await request("/api/config");
  fillConnectionForm(config);
  setConnectionStatus(config);
  return config;
}

async function loadSavedConnections() {
  const connections = await request("/api/saved-connections");
  state.savedConnections = connections;
  renderSavedConnections(connections);
}

async function loadOverview() {
  state.overview = await request("/api/overview");
  renderMetrics();
}

async function loadLlmStatus() {
  state.llmStatus = await request("/api/llm/status");
  renderLlmStatus();
}

async function buildSemanticIndexRequest() {
  state.semanticBuildBusy = true;
  renderSemanticStatus();
  try {
    const payload = await request("/api/semantic/build", { method: "POST", body: JSON.stringify({}) });
    state.llmStatus = {
      ...(state.llmStatus || {}),
      semanticSearch: payload.semanticSearch
    };
    showToast(`Local semantic index built with ${formatNumber(payload.semanticSearch?.documentCount || 0)} documents.`);
    await loadLlmStatus();
    renderSemanticResults();
  } finally {
    state.semanticBuildBusy = false;
    renderSemanticStatus();
  }
}

async function runSemanticSearch() {
  const query = elements.semanticQuery.value.trim();
  if (!query) {
    showToast("Enter a semantic query first.");
    return;
  }

  state.semanticSearchBusy = true;
  renderSemanticStatus();
  renderSemanticResults();
  try {
    const payload = await request(`/api/semantic/search?q=${encodeURIComponent(query)}&limit=6`);
    state.semanticResults = payload.results || [];
    renderSemanticResults();
    showToast(`${formatNumber(payload.count || state.semanticResults.length)} semantic matches loaded.`);
  } finally {
    state.semanticSearchBusy = false;
    renderSemanticStatus();
    renderSemanticResults();
  }
}

async function loadObjects() {
  state.objects = await request(`/api/objects?type=${encodeURIComponent(state.filter)}`);
  renderObjectList();
}

async function sendChatPrompt() {
  const prompt = elements.chatInput.value.trim();
  if (!prompt) {
    showToast("Enter a prompt for Ask Atlas.");
    return;
  }

  state.chatMessages.push({ role: "user", content: prompt });
  renderChatTranscript();
  elements.chatInput.value = "";
  state.chatBusy = true;
  elements.chatSendButton.disabled = true;
  elements.chatSendButton.textContent = "Thinking...";

  try {
    const payload = await request("/api/llm/chat", {
      method: "POST",
      body: JSON.stringify({
        model: state.selectedModel,
        selectedObject: state.selectedObject,
        messages: state.chatMessages.map((message) => ({
          role: message.role,
          content: message.content
        }))
      })
    });

    state.chatMessages.push({
      role: "assistant",
      content: payload.reply || "No response returned.",
      toolTrace: payload.toolTrace || []
    });
    renderChatTranscript();
  } catch (error) {
    state.chatMessages.push({
      role: "assistant",
      content: `Ask Atlas error: ${error.message}`
    });
    renderChatTranscript();
    showToast(error.message);
  } finally {
    state.chatBusy = false;
    elements.chatSendButton.disabled = false;
    elements.chatSendButton.textContent = "Ask Atlas";
  }
}

async function loadSelectedObject(name) {
  const token = ++state.selectionToken;
  state.selectedObject = name;
  state.objectSummary = getObjectStub(name);
  state.dataSlice = null;
  state.rowPage = 1;
  state.rowSearch = "";
  state.rowSort = "";
  state.rowDirection = "ASC";
  state.rowsLoading = true;
  elements.rowSearch.value = "";

  const cache = getObjectCache(name);
  state.sectionLoading.schema = !cache?.schema?.columns?.length;
  state.sectionLoading.relationships = false;
  state.sectionLoading.indexes = false;
  state.sectionLoading.definition = false;

  renderObjectList();
  renderObjectHero();
  renderRowData();
  renderPanels();

  await loadSelectedObjectData(token);

  const openPanels = Object.keys(state.panelState).filter((section) => state.panelState[section] && section !== "schema");
  await Promise.allSettled(openPanels.map((section) => loadPanelSection(section, token)));
}

async function loadSelectedObjectData(token = state.selectionToken) {
  if (!state.selectedObject) {
    return;
  }

  const objectName = state.selectedObject;
  const cache = getObjectCache(objectName);
  const dataSliceCacheKey = getDataSliceCacheKey();
  const cachedSlice = cache?.dataSlices?.[dataSliceCacheKey];
  if (cachedSlice) {
    cache.summary = cache.summary || state.objectSummary;
    cache.schema = cache.schema || (cachedSlice.columns?.length ? { columns: cachedSlice.columns } : null);
    state.objectSummary = cache.summary || state.objectSummary;
    state.dataSlice = cachedSlice;
    state.rowsLoading = false;
    state.sectionLoading.schema = false;
    renderObjectHero();
    renderRowData();
    renderSchemaPanel();
    return;
  }

  const params = new URLSearchParams({
    page: String(state.rowPage),
    pageSize: String(state.pageSize),
    search: state.rowSearch,
    sort: state.rowSort,
    direction: state.rowDirection
  });

  state.rowsLoading = true;
  renderRowData();

  let payload;
  try {
    payload = await request(`/api/objects/${encodeURIComponent(objectName)}/data?${params.toString()}`);
  } catch (error) {
    if (token === state.selectionToken && objectName === state.selectedObject) {
      state.rowsLoading = false;
      renderRowData();
    }
    throw error;
  }
  if (token !== state.selectionToken || objectName !== state.selectedObject) {
    return;
  }

  cache.summary = payload.objectInfo || cache.summary || state.objectSummary;
  cache.schema = payload.columns?.length ? { columns: payload.columns } : cache.schema;
  cache.dataSlices[dataSliceCacheKey] = payload;

  state.objectSummary = cache.summary;
  state.dataSlice = payload;
  state.rowsLoading = false;
  state.sectionLoading.schema = false;

  renderObjectHero();
  renderRowData();
  renderSchemaPanel();
}

async function loadPanelSection(section, token = state.selectionToken) {
  if (!state.selectedObject) {
    return;
  }

  const cache = getSelectedCache();
  if (!cache) {
    return;
  }

  if (section === "schema") {
    renderSchemaPanel();
    return;
  }

  if (cache[section]) {
    renderPanels();
    return;
  }

  const summary = cache.summary || state.objectSummary;
  if (section === "indexes" && summary?.type === "VIEW") {
    cache.indexes = { indexes: [] };
    renderIndexesPanel();
    return;
  }

  if (state.sectionLoading[section]) {
    return;
  }

  const objectName = state.selectedObject;
  state.sectionLoading[section] = true;
  renderPanels();

  try {
    cache[section] = await request(
      `/api/objects/${encodeURIComponent(objectName)}/sections/${encodeURIComponent(section)}`
    );
  } finally {
    if (token === state.selectionToken && objectName === state.selectedObject) {
      state.sectionLoading[section] = false;
      renderPanels();
      renderObjectHero();
    }
  }
}

async function loadColumnProfile() {
  if (!state.selectedObject) {
    showToast("Choose an object before profiling a column.");
    return;
  }

  if (!state.panelState.schema) {
    state.panelState.schema = true;
    renderPanelState("schema");
  }

  await loadPanelSection("schema");

  const columns = getSelectedCache()?.schema?.columns || [];
  if (!columns.length) {
    showToast("Column metadata is not loaded yet.");
    return;
  }

  const choice = window.prompt(`Enter the column name to profile for ${state.selectedObject}:`, columns[0].name);
  if (!choice) {
    return;
  }

  const payload = await request(
    `/api/objects/${encodeURIComponent(state.selectedObject)}/profile?column=${encodeURIComponent(choice)}`
  );

  elements.columnProfile.innerHTML = `
    <div class="profile-grid">
      <div class="profile-card">
        <span>Total Rows</span>
        <strong>${escapeHtml(formatNumber(payload.summary.totalRows))}</strong>
      </div>
      <div class="profile-card">
        <span>Null Count</span>
        <strong>${escapeHtml(formatNumber(payload.summary.nullCount))}</strong>
      </div>
      <div class="profile-card">
        <span>Distinct Count</span>
        <strong>${escapeHtml(formatNumber(payload.summary.distinctCount))}</strong>
      </div>
    </div>
    <div class="profile-values">
      ${payload.topValues
        .map(
          (row) => `
            <span class="profile-chip">${escapeHtml(formatValue(row.value))} | ${escapeHtml(formatNumber(row.count))}</span>
          `
        )
        .join("")}
    </div>
  `;
}

async function performGlobalSearch() {
  const term = elements.globalSearch.value.trim();
  if (!term) {
    renderSearchResults({ tables: [], columns: [] });
    return;
  }
  const results = await request(`/api/search?q=${encodeURIComponent(term)}`);
  renderSearchResults(results);
}

async function runSql() {
  const payload = await request("/api/sql/query", {
    method: "POST",
    body: JSON.stringify({ sql: elements.sqlEditor.value })
  });

  elements.sqlStatus.textContent = `${formatNumber(payload.rows.length)} rows returned`;
  elements.sqlResultsTable.innerHTML = createTable(
    payload.columns.map((key) => ({ key, label: key })),
    payload.rows
  );
}

function bindTabs() {
  document.querySelectorAll(".tab-button").forEach((button) => {
    button.addEventListener("click", () => {
      document.querySelectorAll(".tab-button").forEach((item) => item.classList.remove("is-active"));
      document.querySelectorAll(".tab-panel").forEach((panel) => panel.classList.remove("is-active"));
      button.classList.add("is-active");
      document.querySelector(`[data-panel="${button.dataset.tab}"]`).classList.add("is-active");
    });
  });
}

function bindObjectFilters() {
  document.querySelectorAll("[data-object-filter]").forEach((button) => {
    button.addEventListener("click", async () => {
      document.querySelectorAll("[data-object-filter]").forEach((item) => item.classList.remove("is-active"));
      button.classList.add("is-active");
      state.filter = button.dataset.objectFilter;
      await loadObjects();
      if (state.selectedObject && !state.objects.some((item) => item.name === state.selectedObject)) {
        if (state.objects[0]?.name) {
          await loadSelectedObject(state.objects[0].name);
        } else {
          state.selectedObject = null;
          state.objectSummary = null;
          state.dataSlice = null;
          renderObjectHero();
          renderRowData();
          renderPanels();
        }
      }
    });
  });
}

function bindEvents() {
  elements.focusConnectButton.addEventListener("click", () => {
    elements.password.focus();
  });

  elements.refreshModelsButton.addEventListener("click", () => {
    loadLlmStatus().catch((error) => showToast(error.message));
  });

  elements.llmModelSelect.addEventListener("change", () => {
    state.selectedModel = elements.llmModelSelect.value;
  });

  elements.chatForm.addEventListener("submit", (event) => {
    event.preventDefault();
    sendChatPrompt().catch((error) => showToast(error.message));
  });

  elements.chatClearButton.addEventListener("click", () => {
    state.chatMessages = [];
    renderChatTranscript();
  });

  elements.buildSemanticIndexButton.addEventListener("click", () => {
    buildSemanticIndexRequest().catch((error) => {
      showToast(error.message);
      renderSemanticStatus();
    });
  });

  elements.semanticSearchForm.addEventListener("submit", (event) => {
    event.preventDefault();
    runSemanticSearch().catch((error) => {
      showToast(error.message);
      state.semanticSearchBusy = false;
      renderSemanticStatus();
      renderSemanticResults();
    });
  });

  elements.savedConnectionSelect.addEventListener("change", () => {
    const selected = state.savedConnections.find(
      (connection) => String(connection.id) === elements.savedConnectionSelect.value
    );
    if (!selected) {
      return;
    }
    fillConnectionForm(selected);
    elements.savedConnectionHint.textContent = `${selected.caption} loaded. Enter the password, then connect.`;
  });

  elements.connectForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    setConnectButtonBusy(true, "Connecting...");
    try {
      const payload = await request("/api/connect", {
        method: "POST",
        body: JSON.stringify({
          host: elements.host.value,
          port: elements.port.value,
          user: elements.user.value,
          password: elements.password.value,
          database: elements.database.value
        })
      });
      state.objectCache = {};
      state.selectedObject = null;
      state.objectSummary = null;
      state.dataSlice = null;
      setConnectionStatus(payload);
      await hydrateConnectedView({ autoSelectFirst: true });
      showToast(`Connected to ${payload.database} on ${payload.host}:${payload.port}`);
    } catch (error) {
      showToast(error.message);
    } finally {
      setConnectButtonBusy(false, state.connection?.connected ? "Refresh Schema View" : "Connect To Schema");
    }
  });

  elements.objectSearch.addEventListener("input", renderObjectList);
  elements.globalSearch.addEventListener("input", () => {
    window.clearTimeout(elements.globalSearch._timer);
    elements.globalSearch._timer = window.setTimeout(() => {
      performGlobalSearch().catch((error) => showToast(error.message));
    }, 180);
  });

  elements.objectList.addEventListener("click", (event) => {
    const button = event.target.closest("[data-object-name]");
    if (!button) {
      return;
    }
    loadSelectedObject(button.dataset.objectName).catch((error) => showToast(error.message));
  });

  elements.searchResults.addEventListener("click", (event) => {
    const button = event.target.closest("[data-object-name]");
    if (!button) {
      return;
    }
    loadSelectedObject(button.dataset.objectName).catch((error) => showToast(error.message));
  });

  elements.semanticResults.addEventListener("click", (event) => {
    const button = event.target.closest("[data-object-name]");
    if (!button) {
      return;
    }
    loadSelectedObject(button.dataset.objectName).catch((error) => showToast(error.message));
  });

  document.querySelectorAll("[data-toggle-panel]").forEach((button) => {
    button.addEventListener("click", () => {
      togglePanel(button.dataset.togglePanel);
    });
  });

  elements.rowSearch.addEventListener("input", () => {
    window.clearTimeout(elements.rowSearch._timer);
    elements.rowSearch._timer = window.setTimeout(() => {
      state.rowSearch = elements.rowSearch.value.trim();
      state.rowPage = 1;
      loadSelectedObjectData().catch((error) => showToast(error.message));
    }, 180);
  });

  elements.pageSizeSelect.addEventListener("change", () => {
    state.pageSize = Number(elements.pageSizeSelect.value);
    state.rowPage = 1;
    loadSelectedObjectData().catch((error) => showToast(error.message));
  });

  elements.previousPageButton.addEventListener("click", () => {
    if (state.rowPage === 1) {
      return;
    }
    state.rowPage -= 1;
    loadSelectedObjectData().catch((error) => showToast(error.message));
  });

  elements.nextPageButton.addEventListener("click", () => {
    state.rowPage += 1;
    loadSelectedObjectData().catch((error) => showToast(error.message));
  });

  elements.rowsTable.addEventListener("click", (event) => {
    const sortable = event.target.closest("[data-sort-column]");
    if (!sortable) {
      return;
    }
    updateSortFromHeader(sortable);
  });

  elements.exportCsvButton.addEventListener("click", () => {
    if (!state.selectedObject) {
      showToast("Pick an object before exporting.");
      return;
    }
    const params = new URLSearchParams({
      pageSize: String(Math.max(state.pageSize, 500)),
      search: state.rowSearch,
      sort: state.rowSort,
      direction: state.rowDirection,
      format: "csv"
    });
    window.open(`/api/objects/${encodeURIComponent(state.selectedObject)}/export?${params.toString()}`, "_blank");
  });

  elements.exportJsonButton.addEventListener("click", () => {
    if (!state.selectedObject) {
      showToast("Pick an object before exporting.");
      return;
    }
    const params = new URLSearchParams({
      pageSize: String(Math.max(state.pageSize, 500)),
      search: state.rowSearch,
      sort: state.rowSort,
      direction: state.rowDirection,
      format: "json"
    });
    window.open(`/api/objects/${encodeURIComponent(state.selectedObject)}/export?${params.toString()}`, "_blank");
  });

  elements.profileColumnButton.addEventListener("click", () => {
    loadColumnProfile().catch((error) => showToast(error.message));
  });

  elements.runQueryButton.addEventListener("click", () => {
    runSql().catch((error) => {
      elements.sqlStatus.textContent = error.message;
      showToast(error.message);
    });
  });

  elements.loadSampleQueryButton.addEventListener("click", () => {
    const summary = getSelectedCache()?.summary || state.objectSummary;
    if (summary?.isInferred) {
      elements.sqlEditor.value =
        getSelectedCache()?.definition?.definition?.statement ||
        `/* ${summary.name} is a placeholder database view.\nSchema Atlas replaces it with an inferred query shown in the Definition panel. */`;
      return;
    }
    elements.sqlEditor.value = state.selectedObject
      ? `SELECT * FROM ${state.selectedObject} LIMIT 25;`
      : "SHOW TABLES;";
  });
}

async function hydrateConnectedView({ autoSelectFirst = false } = {}) {
  await loadOverview();
  await loadObjects();
  renderSearchResults({ tables: [], columns: [] });
  renderMetrics();
  renderPanels();
  elements.sqlStatus.textContent = "Connected. Run read-only SQL or inspect a table from the explorer.";

  if (autoSelectFirst) {
    const targetObject = state.selectedObject || state.objects[0]?.name;
    if (targetObject) {
      await loadSelectedObject(targetObject);
      return;
    }
  }

  renderObjectHero();
  renderRowData();
}

async function initialize() {
  try {
    bindTabs();
    bindObjectFilters();
    bindEvents();
    renderPanelStates();
    renderChatTranscript();
    renderLlmContext();
    renderSemanticResults();
    await loadSavedConnections();
    const config = await loadConfig();
    await loadLlmStatus();
    if (config.connected) {
      await hydrateConnectedView({ autoSelectFirst: true });
    } else {
      renderMetrics();
      renderObjectHero();
      renderRowData();
      renderPanels();
      elements.sqlStatus.textContent = "Connect first, then run read-only SQL.";
    }
    renderSearchResults({ tables: [], columns: [] });
  } catch (error) {
    showToast(error.message);
  }
}

initialize();
