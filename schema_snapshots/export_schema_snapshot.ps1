param(
  [string]$OutputPath = ".\\schema_snapshots\\local_schema_snapshot.json",
  [string]$SchemaName = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Invoke-AtlasSql {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Sql
  )

  $body = @{ sql = $Sql } | ConvertTo-Json
  $response = Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:4040/api/sql/query" -ContentType "application/json" -Body $body
  return $response.rows
}

function Invoke-AtlasPagedSql {
  param(
    [Parameter(Mandatory = $true)]
    [string]$SqlPrefix,
    [int]$BatchSize = 500
  )

  $rows = @()
  $offset = 0

  while ($true) {
    $batch = @(Invoke-AtlasSql "$SqlPrefix LIMIT $BatchSize OFFSET $offset")
    if ($batch.Count -eq 0) {
      break
    }
    $rows += $batch
    if ($batch.Count -lt $BatchSize) {
      break
    }
    $offset += $BatchSize
  }

  return $rows
}

$health = Invoke-RestMethod -Uri "http://127.0.0.1:4040/api/health"
if (-not $health.ok) {
  throw "Schema Atlas is not responding."
}

if (-not $SchemaName) {
  $SchemaName = "$($health.connection.database)".Trim()
}

if (-not $SchemaName) {
  throw "SchemaName was not provided and Schema Atlas is not connected to a database."
}

$tables = Invoke-AtlasSql @"
SELECT
  TABLE_NAME,
  TABLE_TYPE,
  ENGINE,
  TABLE_ROWS,
  DATA_LENGTH,
  INDEX_LENGTH,
  TABLE_COMMENT,
  CREATE_TIME,
  UPDATE_TIME
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = '$SchemaName'
ORDER BY TABLE_TYPE, TABLE_NAME
LIMIT 1000
"@

$columns = Invoke-AtlasPagedSql @"
SELECT
  TABLE_NAME,
  COLUMN_NAME,
  ORDINAL_POSITION,
  COLUMN_TYPE,
  DATA_TYPE,
  IS_NULLABLE,
  COLUMN_KEY,
  EXTRA
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = '$SchemaName'
ORDER BY TABLE_NAME, ORDINAL_POSITION
"@

$indexes = Invoke-AtlasSql @"
SELECT
  TABLE_NAME,
  INDEX_NAME,
  COLUMN_NAME,
  SEQ_IN_INDEX,
  NON_UNIQUE,
  INDEX_TYPE
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = '$SchemaName'
ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX
LIMIT 10000
"@

$relationships = Invoke-AtlasSql @"
SELECT
  TABLE_NAME,
  COLUMN_NAME,
  REFERENCED_TABLE_NAME,
  CONSTRAINT_NAME
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = '$SchemaName'
  AND REFERENCED_TABLE_NAME IS NOT NULL
ORDER BY TABLE_NAME, CONSTRAINT_NAME, COLUMN_NAME
LIMIT 5000
"@

$views = Invoke-AtlasSql @"
SELECT
  TABLE_NAME,
  VIEW_DEFINITION
FROM information_schema.VIEWS
WHERE TABLE_SCHEMA = '$SchemaName'
ORDER BY TABLE_NAME
LIMIT 1000
"@

$snapshot = [ordered]@{
  generatedAt = (Get-Date).ToString("o")
  schema = $SchemaName
  connection = $health.connection
  summary = [ordered]@{
    objectCount = @($tables).Count
    baseTableCount = @($tables | Where-Object TABLE_TYPE -eq "BASE TABLE").Count
    viewCount = @($tables | Where-Object TABLE_TYPE -eq "VIEW").Count
    columnCount = @($columns).Count
    indexCount = @($indexes).Count
    relationshipCount = @($relationships).Count
  }
  tables = $tables
  columns = $columns
  indexes = $indexes
  relationships = $relationships
  views = $views
}

$outputDir = Split-Path -Parent $OutputPath
if ($outputDir) {
  New-Item -ItemType Directory -Force -Path $outputDir | Out-Null
}

$snapshot | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 -Path $OutputPath
Write-Output "Schema snapshot written to $OutputPath"
