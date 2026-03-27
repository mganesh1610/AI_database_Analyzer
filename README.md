# Schema Atlas

Schema Atlas is a local, read-only database viewer for exploring a MariaDB schema from your workstation.

## What It Does

- Browses tables and views with search and type filters
- Shows columns, indexes, DDL, and foreign-key relationships
- Lets you inspect live table data with pagination, search, and sorting
- Profiles a selected column with null counts, distinct counts, and top values
- Includes a read-only SQL Lab for `SELECT`, `SHOW`, `DESCRIBE`, and `EXPLAIN`
- Exports the current data slice as CSV or JSON
- Adds an optional local Ollama-powered analysis tab

## Local Configuration

Set connection details with environment variables if you want defaults in the UI:

```powershell
$env:ATLAS_DB_HOST = "127.0.0.1"
$env:ATLAS_DB_PORT = "3306"
$env:ATLAS_DB_USER = "your-user"
$env:ATLAS_DB_NAME = "your-database"
$env:ATLAS_DB_PASSWORD = "your-password"
```

Optional local-only artifacts can live in:

- `schema_snapshots/`
- `.runtime_appdata/`

Those paths are intentionally git-ignored so private schema metadata and generated runtime state stay out of the public repo.

## Run Locally

Install dependencies:

```powershell
python -m pip install -r requirements.txt
```

Start the app:

```powershell
python app.py
```

Or from VS Code, run the `Start Schema Atlas` task.

Open:

- [http://127.0.0.1:4040](http://127.0.0.1:4040)

## Optional Local LLM Setup

`Ask Atlas` uses a local Ollama server. It is optional and separate from the core schema viewer.

Recommended local models:

- `qwen3:8b`
- `deepseek-r1:8b`
- `nomic-embed-text`

After installing Ollama locally, pull them with:

```powershell
ollama pull qwen3:8b
ollama pull deepseek-r1:8b
ollama pull nomic-embed-text
```

If you keep private schema snapshots or notes locally, the semantic-search feature can build a local index from them and persist it under `.runtime_appdata/`.

## Safety Model

- The backend is read-only by design.
- The SQL Lab blocks write and DDL statements.
- Ask Atlas only exposes curated read-only tools to the local model.
- Even the LLM SQL tool is forced through the same read-only validator as SQL Lab.
- Table browsing uses generated `SELECT` queries only.
- Database access is executed through the local `mysqlsh` client.

## Notes

- MariaDB table statistics such as row counts can be estimates, depending on engine and server settings.
- If your account cannot read some metadata objects, the app will still load the parts it can access.
- The viewer can read saved MySQL Shell connection metadata from your local profile and prefill matching fields in the connection form.
