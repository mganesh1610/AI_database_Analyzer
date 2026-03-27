from __future__ import annotations

import csv
import hashlib
import html as html_lib
import io
import json
import math
import os
import re
import sqlite3
import subprocess
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from flask import Flask, Response, jsonify, request, send_from_directory

ROOT_DIR = Path(__file__).resolve().parent
PUBLIC_DIR = ROOT_DIR / "public"
SCHEMA_SNAPSHOT_PATH = ROOT_DIR / "schema_snapshots" / "local_schema_snapshot.json"
SEMANTIC_NOTES_PATH = ROOT_DIR / "schema_snapshots" / "local_inferred_views_notes.md"
RUNTIME_APPDATA = ROOT_DIR / ".runtime_appdata"
SEMANTIC_INDEX_PATH = RUNTIME_APPDATA / "semantic_index.json"
MYSQLSH_GUI_DB = (
    Path(os.getenv("APPDATA", ""))
    / "MySQL"
    / "mysqlsh-gui"
    / "plugin_data"
    / "gui_plugin"
    / "mysqlsh_gui_backend.sqlite3"
)
HOST = "127.0.0.1"
PORT = int(os.getenv("PORT", "4040"))
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434")
OLLAMA_TIMEOUT_SECONDS = int(os.getenv("OLLAMA_TIMEOUT_SECONDS", "300"))
CHAT_MESSAGE_LIMIT = 12
CHAT_TOOL_LOOP_LIMIT = 6
SEMANTIC_INDEX_VERSION = 1
SEMANTIC_BATCH_SIZE = int(os.getenv("ATLAS_SEMANTIC_BATCH_SIZE", "24"))
SEMANTIC_TOP_K = int(os.getenv("ATLAS_SEMANTIC_TOP_K", "6"))
SEMANTIC_EMBED_MODEL = os.getenv("ATLAS_SEMANTIC_EMBED_MODEL", "nomic-embed-text")

RECOMMENDED_LOCAL_MODELS = [
    {
        "id": "qwen3:8b",
        "label": "Qwen3 8B",
        "description": "Balanced local analyst for schema reasoning and tool use.",
        "speed": "Balanced",
    },
    {
        "id": "deepseek-r1:8b",
        "label": "DeepSeek R1 8B",
        "description": "Slower but stronger for deeper reasoning on ambiguous joins.",
        "speed": "Slow",
    },
]

RECOMMENDED_EMBEDDING_MODELS = [
    {
        "id": SEMANTIC_EMBED_MODEL,
        "label": "Nomic Embed Text",
        "description": "Local embedding model for semantic schema retrieval.",
    }
]

SEMANTIC_SEARCH_STATUS = {
    "enabled": False,
    "indexed": False,
    "provider": "ollama",
    "storage": "Planned as a local vector index stored on disk. A graph database is not required.",
    "buildTimeEstimate": (
        "Expected to take under a minute for the current schema snapshot alone, or roughly 1 to 3 "
        "minutes if schema notes and inferred mappings are included."
    ),
    "lifecycle": (
        "Usually a one-time build, then rebuild only when the schema snapshot, inferred notes, or "
        "local documentation changes."
    ),
    "reason": (
        "Semantic search is disabled by default. The panel is present so Schema Atlas can later "
        "build a local embedding index over schema snapshots and inferred notes without changing "
        "the core read-only workflow."
    ),
}

CHAT_STOPWORDS = {
    "a",
    "about",
    "an",
    "and",
    "are",
    "atlas",
    "briefly",
    "can",
    "data",
    "database",
    "describe",
    "does",
    "explain",
    "for",
    "from",
    "how",
    "in",
    "is",
    "it",
    "me",
    "of",
    "on",
    "or",
    "represent",
    "show",
    "table",
    "tell",
    "that",
    "the",
    "this",
    "to",
    "view",
    "what",
    "which",
    "with",
}

ASK_ATLAS_SYSTEM_PROMPT = """
You are Ask Atlas, a local read-only analyst for the connected MariaDB database.

Rules:
- You must remain strictly read-only.
- Never attempt to modify schema, rows, permissions, sessions, or server settings.
- If the user asks to edit data or schema, refuse briefly and offer read-only analysis instead.
- Use the provided tools to inspect structure and data before answering.
- Some schemas lack foreign key constraints. Treat joins as hypotheses until supported
  by the inferred view definitions, naming conventions, or overlap checks.
- Prefer explaining uncertainty instead of inventing unsupported relationships.
- When discussing an inferred view, explain that the original database view is a placeholder stub
  and Schema Atlas is substituting a heuristic query.
- Keep answers concise, practical, and oriented to understanding the data.
"""


@dataclass
class AppError(Exception):
    status: int
    message: str

    def __str__(self) -> str:
        return self.message


def find_mysqlsh_executable() -> Path:
    explicit = os.getenv("MYSQLSH_PATH")
    if explicit and Path(explicit).exists():
        return Path(explicit)

    gui_batch = Path(os.getenv("APPDATA", "")) / "MySQL" / "mysqlsh-gui" / "mysqlsh.bat"
    if gui_batch.exists():
        content = gui_batch.read_text(encoding="utf-8", errors="ignore")
        match = re.search(r'"([^"]*mysqlsh\.exe)"', content, re.IGNORECASE)
        if match and Path(match.group(1)).exists():
            return Path(match.group(1))

    home = Path.home()
    extension_roots = [
        home / ".vscode" / "extensions",
        home / ".cursor" / "extensions",
        home / ".windsurf" / "extensions",
    ]
    for root in extension_roots:
        if not root.exists():
            continue
        matches = sorted(root.glob("oracle.mysql-shell-for-vs-code-*/shell/bin/mysqlsh.exe"), reverse=True)
        if matches:
            return matches[0]

    raise AppError(500, "MySQL Shell executable was not found on this machine.")


MYSQLSH_EXE = find_mysqlsh_executable()

DEFAULT_CONFIG = {
    "host": os.getenv("ATLAS_DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("ATLAS_DB_PORT", "3306")),
    "user": os.getenv("ATLAS_DB_USER", ""),
    "password": os.getenv("ATLAS_DB_PASSWORD", ""),
    "database": os.getenv("ATLAS_DB_NAME", ""),
    "sslMode": os.getenv("ATLAS_DB_SSL_MODE", "PREFERRED"),
}
PREFERRED_CONNECTION_CAPTION = os.getenv("ATLAS_PREFERRED_CONNECTION_CAPTION", "").strip().lower()

active_config = DEFAULT_CONFIG.copy()
connected = False
runtime_cache: dict[str, Any] = {
    "overview": None,
    "objects": {},
    "search": {},
    "table_object": {},
    "columns": {},
    "indexes": {},
    "relationships": {},
    "relationship_hints": {},
    "definition": {},
    "inferred_specs": {},
}
semantic_index_cache: dict[str, Any] | None = None

app = Flask(__name__, static_folder=None)

INFERRED_VIEWS: dict[str, dict[str, Any]] = {}

INFERRED_VIEW_COLUMN_SOURCES: dict[str, dict[str, str]] = {}

VIEW_MODIFIER_TOKENS = {
    "view",
    "simple",
    "detail",
    "details",
    "detailed",
    "all",
    "more",
    "with",
    "without",
    "result",
    "results",
    "plotter",
    "statistic",
    "statistics",
}

TOKEN_REPLACEMENTS = {
    "org": "organization",
    "ethn": "ethnicity",
    "num": "number",
    "nbsp": "id",
}


def load_schema_snapshot() -> dict[str, Any]:
    if not SCHEMA_SNAPSHOT_PATH.exists():
        return {}
    try:
        return json.loads(SCHEMA_SNAPSHOT_PATH.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}


SCHEMA_SNAPSHOT = load_schema_snapshot()
SNAPSHOT_TABLE_ROWS = {row["TABLE_NAME"]: row for row in SCHEMA_SNAPSHOT.get("tables", [])}
SNAPSHOT_COLUMNS_BY_TABLE: dict[str, list[dict[str, Any]]] = {}
for row in SCHEMA_SNAPSHOT.get("columns", []):
    SNAPSHOT_COLUMNS_BY_TABLE.setdefault(row["TABLE_NAME"], []).append(row)
for rows in SNAPSHOT_COLUMNS_BY_TABLE.values():
    rows.sort(key=lambda item: int(item.get("ORDINAL_POSITION") or 0))

SNAPSHOT_VIEW_DEFINITIONS = {row["TABLE_NAME"]: row["VIEW_DEFINITION"] for row in SCHEMA_SNAPSHOT.get("views", [])}
SNAPSHOT_BASE_TABLES = sorted(
    row["TABLE_NAME"] for row in SCHEMA_SNAPSHOT.get("tables", []) if row.get("TABLE_TYPE") == "BASE TABLE"
)


def singularize_token(token: str) -> str:
    if token.endswith("ies") and len(token) > 3:
        return token[:-3] + "y"
    if token.endswith("s") and len(token) > 3 and not token.endswith("ss"):
        return token[:-1]
    return token


def normalize_name(value: str) -> str:
    text = html_lib.unescape(str(value or ""))
    text = text.replace("\xa0", " ").lower()
    text = text.replace("#", " number ")
    text = text.replace("&", " and ")
    text = text.replace("/", " ")
    text = text.replace("%", " percent ")
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    return text


def normalized_tokens(value: str, drop_modifiers: bool = False) -> list[str]:
    tokens = []
    for token in normalize_name(value).split("_"):
        if not token:
            continue
        token = TOKEN_REPLACEMENTS.get(token, token)
        token = singularize_token(token)
        if drop_modifiers and token in VIEW_MODIFIER_TOKENS:
            continue
        tokens.append(token)
    return tokens


def is_stub_view_definition(definition: str) -> bool:
    normalized = re.sub(r"\s+", " ", (definition or "").strip()).lower()
    return normalized.startswith("select 1 as ") and " from " not in normalized


def snapshot_view_columns(table_name: str) -> list[dict[str, Any]]:
    return SNAPSHOT_COLUMNS_BY_TABLE.get(table_name, [])


def snapshot_table_columns(table_name: str) -> list[dict[str, Any]]:
    return SNAPSHOT_COLUMNS_BY_TABLE.get(table_name, [])


def snapshot_table_index(table_name: str) -> dict[str, Any]:
    columns = snapshot_table_columns(table_name)
    exact: dict[str, str] = {}
    items: list[tuple[str, str, set[str]]] = []
    for row in columns:
        column_name = row["COLUMN_NAME"]
        normalized = normalize_name(column_name)
        exact.setdefault(normalized, column_name)
        items.append((column_name, normalized, set(normalized_tokens(column_name))))
    return {"exact": exact, "items": items}


SNAPSHOT_COLUMN_INDEXES = {table_name: snapshot_table_index(table_name) for table_name in SNAPSHOT_COLUMNS_BY_TABLE}


def join_variants_from_tokens(tokens: list[str]) -> list[str]:
    variants: list[str] = []
    if not tokens:
        return variants
    seen: set[str] = set()
    for variant in (
        "_".join(tokens),
        tokens[-1],
        singularize_token(tokens[-1]),
    ):
        if variant and variant not in seen:
            seen.add(variant)
            variants.append(variant)
    return variants


def match_alias_to_table_column(alias: str, table_name: str) -> tuple[str, str] | None:
    index = SNAPSHOT_COLUMN_INDEXES.get(table_name)
    if not index:
        return None

    alias_tokens = normalized_tokens(alias)
    table_tokens = set(normalized_tokens(table_name, drop_modifiers=True))
    residual_tokens = [token for token in alias_tokens if token not in table_tokens]
    candidate_norms: list[str] = []

    alias_norm = normalize_name(alias)
    if alias_norm:
        candidate_norms.append(alias_norm)
    candidate_norms.extend(join_variants_from_tokens(residual_tokens))

    if alias_tokens and alias_tokens[-1] == "id":
        candidate_norms.append("id")

    seen: set[str] = set()
    for candidate in candidate_norms:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        matched = index["exact"].get(candidate)
        if not matched:
            continue
        normalized_matched = normalize_name(matched)
        if normalized_matched.endswith("_id") and "id" not in alias_tokens:
            continue
        if normalized_matched == "id" and "id" not in alias_tokens and alias_norm not in {"id", "nbsp"}:
            continue
        return matched, f"{table_name}.{matched}"

    residual_set = set(residual_tokens)
    alias_set = set(alias_tokens)
    best_score = -1
    best_column = None
    for column_name, _normalized, column_tokens in index["items"]:
        normalized_column = normalize_name(column_name)
        if normalized_column.endswith("_id") and "id" not in alias_tokens:
            continue
        score = 0
        if residual_set and residual_set == column_tokens:
            score += 70
        elif residual_set and residual_set.issubset(column_tokens):
            score += 50
        elif residual_set and column_tokens.issubset(residual_set):
            score += 32
        score += len(alias_set & column_tokens) * 10
        if normalized_column == "id" and "id" in alias_tokens:
            score += 25
        if score > best_score:
            best_score = score
            best_column = column_name
    if best_column and best_score >= 40:
        return best_column, f"{table_name}.{best_column}"
    return None


def build_view_name_variants(view_name: str) -> set[str]:
    raw_tokens = normalized_tokens(view_name)
    filtered_tokens = normalized_tokens(view_name, drop_modifiers=True)
    variants = set()
    if raw_tokens:
        variants.add("_".join(token for token in raw_tokens if token != "view"))
    if filtered_tokens:
        variants.add("_".join(filtered_tokens))
        variants.add(filtered_tokens[-1])
    return {variant for variant in variants if variant}


def choose_primary_table(view_name: str, view_columns: list[dict[str, Any]]) -> str | None:
    view_tokens = set(normalized_tokens(view_name, drop_modifiers=True))
    name_variants = build_view_name_variants(view_name)
    best_score = -1
    best_table = None

    for table_name in SNAPSHOT_BASE_TABLES:
        table_tokens = set(normalized_tokens(table_name, drop_modifiers=True))
        table_variants = build_view_name_variants(table_name)
        score = 0

        if normalize_name(table_name) in name_variants or any(variant in name_variants for variant in table_variants):
            score += 80
        score += len(view_tokens & table_tokens) * 12

        column_matches = 0
        for column in view_columns:
            if match_alias_to_table_column(column["COLUMN_NAME"], table_name):
                column_matches += 1
        score += column_matches * 4

        if score > best_score:
            best_score = score
            best_table = table_name

    return best_table


def possible_fk_names_for_table(table_name: str) -> set[str]:
    tokens = normalized_tokens(table_name, drop_modifiers=True)
    candidates = set()
    for variant in join_variants_from_tokens(tokens):
        candidates.add(f"{variant}_id")
    return candidates


def choose_join_expression(alias: str, related_table: str) -> tuple[str, str] | None:
    direct_match = match_alias_to_table_column(alias, related_table)
    if direct_match:
        return direct_match

    alias_tokens = set(normalized_tokens(alias))
    table_tokens = set(normalized_tokens(related_table, drop_modifiers=True))
    if not alias_tokens & table_tokens:
        return None

    related_index = SNAPSHOT_COLUMN_INDEXES.get(related_table, {}).get("exact", {})
    if "code" in alias_tokens and "code" in related_index:
        return related_index["code"], f"{related_table}.{related_index['code']}"
    for display_column in ("name", "description", "title", "type", "model", "serial_number", "number"):
        if display_column in related_index:
            return related_index[display_column], f"{related_table}.{related_index[display_column]}"
    return None


def generate_inferred_view_spec(table_name: str) -> dict[str, Any] | None:
    cached = runtime_cache["inferred_specs"].get(table_name)
    if cached is not None:
        return cached

    if table_name in INFERRED_VIEWS:
        runtime_cache["inferred_specs"][table_name] = INFERRED_VIEWS[table_name]
        return INFERRED_VIEWS[table_name]

    definition = SNAPSHOT_VIEW_DEFINITIONS.get(table_name)
    view_columns = snapshot_view_columns(table_name)
    if not definition or not view_columns or not is_stub_view_definition(definition):
        runtime_cache["inferred_specs"][table_name] = None
        return None

    primary_table = choose_primary_table(table_name, view_columns)
    if not primary_table:
        runtime_cache["inferred_specs"][table_name] = None
        return None

    primary_index = SNAPSHOT_COLUMN_INDEXES.get(primary_table, {})
    joins: dict[str, dict[str, str]] = {}
    for candidate_table in SNAPSHOT_BASE_TABLES:
        if candidate_table == primary_table:
            continue
        related_index = SNAPSHOT_COLUMN_INDEXES.get(candidate_table, {}).get("exact", {})
        if "id" not in related_index:
            continue
        for fk_name in possible_fk_names_for_table(candidate_table):
            matched_fk = primary_index.get("exact", {}).get(fk_name)
            if matched_fk:
                joins[candidate_table] = {"fkColumn": matched_fk, "idColumn": related_index["id"]}
                break

    select_parts: list[str] = []
    sources: dict[str, str] = {}
    relationships: list[dict[str, str]] = []
    used_joins: dict[str, str] = {}

    for column in view_columns:
        alias = column["COLUMN_NAME"]
        column_type = column["COLUMN_TYPE"]
        expression = "NULL"
        source_summary = "No direct source mapped yet"

        direct_match = match_alias_to_table_column(alias, primary_table)
        if direct_match:
            column_name, source_summary = direct_match
            expression = f"main.{escape_identifier(column_name)}"
        else:
            for related_table, join_info in joins.items():
                join_match = choose_join_expression(alias, related_table)
                if not join_match:
                    continue
                related_column, source_summary = join_match
                join_alias = used_joins.setdefault(related_table, f"j{len(used_joins) + 1}")
                expression = f"{join_alias}.{escape_identifier(related_column)}"
                if not any(item["referencedTable"] == related_table for item in relationships):
                    relationships.append(
                        {
                            "constraintName": "heuristic",
                            "columnName": join_info["fkColumn"],
                            "referencedTable": related_table,
                            "referencedColumn": join_info["idColumn"],
                        }
                    )
                break

        select_parts.append(f"  {expression} AS {escape_identifier(alias)}")
        sources[alias] = source_summary
        column["COLUMN_TYPE"] = column_type

    sql_lines = ["SELECT", ",\n".join(select_parts), f"FROM {escape_identifier(primary_table)} main"]
    for related_table, join_alias in used_joins.items():
        join_info = joins[related_table]
        sql_lines.append(
            f"LEFT JOIN {escape_identifier(related_table)} {join_alias} "
            f"ON main.{escape_identifier(join_info['fkColumn'])} = {join_alias}.{escape_identifier(join_info['idColumn'])}"
        )

    spec = {
        "description": (
            f"Heuristically inferred in Schema Atlas from `{primary_table}`"
            + (
                f" with related lookups from {', '.join(f'`{name}`' for name in used_joins)}."
                if used_joins
                else "."
            )
            + " The original database view is a placeholder stub."
        ),
        "columns": [(column["COLUMN_NAME"], column["COLUMN_TYPE"]) for column in view_columns],
        "sql": "\n".join(sql_lines),
        "relationships": {"incoming": [], "outgoing": relationships},
        "sources": sources,
    }
    runtime_cache["inferred_specs"][table_name] = spec
    return spec


def inferred_view_names() -> list[str]:
    names = set(INFERRED_VIEWS)
    for table_name, definition in SNAPSHOT_VIEW_DEFINITIONS.items():
        if is_stub_view_definition(definition):
            names.add(table_name)
    return sorted(names)


def clear_runtime_cache() -> None:
    runtime_cache["overview"] = None
    runtime_cache["objects"] = {}
    runtime_cache["search"] = {}
    runtime_cache["table_object"] = {}
    runtime_cache["columns"] = {}
    runtime_cache["indexes"] = {}
    runtime_cache["relationships"] = {}
    runtime_cache["relationship_hints"] = {}
    runtime_cache["definition"] = {}
    runtime_cache["inferred_specs"] = {}


def is_inferred_view(table_name: str) -> bool:
    return generate_inferred_view_spec(table_name) is not None


def get_inferred_view_spec(table_name: str) -> dict[str, Any]:
    spec = generate_inferred_view_spec(table_name)
    if spec is None:
        raise AppError(404, f"No inferred view spec is configured for `{table_name}`.")
    return spec


def infer_data_type(column_type: str) -> str:
    return column_type.split("(", 1)[0].lower()


def get_inferred_columns(table_name: str) -> list[dict[str, Any]]:
    cached = runtime_cache["columns"].get(table_name)
    if cached is not None:
        return cached

    spec = get_inferred_view_spec(table_name)
    source_map = spec.get("sources") or INFERRED_VIEW_COLUMN_SOURCES.get(table_name, {})
    rows = []
    for ordinal, (name, column_type) in enumerate(spec["columns"], start=1):
        rows.append(
            {
                "name": name,
                "ordinalPosition": ordinal,
                "columnType": column_type,
                "dataType": infer_data_type(column_type),
                "isNullable": "YES",
                "columnDefault": None,
                "columnKey": "",
                "extra": "",
                "comment": "Inferred in Schema Atlas",
                "sourceSummary": source_map.get(name, "No direct source mapped yet"),
            }
        )
    runtime_cache["columns"][table_name] = rows
    return rows


def get_inferred_definition(table_name: str) -> dict[str, Any]:
    cached = runtime_cache["definition"].get(table_name)
    if cached is not None:
        return cached

    spec = get_inferred_view_spec(table_name)
    definition = {
        "statement": (
            f"/* {spec['description']} */\n"
            f"/* Placeholder database view returns SELECT 1 constants; Schema Atlas infers this replacement query. */\n"
            f"{spec['sql'].strip()}"
        )
    }
    runtime_cache["definition"][table_name] = definition
    return definition


def get_inferred_relationships(table_name: str) -> dict[str, Any]:
    cached = runtime_cache["relationships"].get(table_name)
    if cached is not None:
        return cached

    relationships = get_inferred_view_spec(table_name)["relationships"]
    runtime_cache["relationships"][table_name] = relationships
    return relationships


def table_name_match_score(column_name: str, candidate_table: str) -> tuple[int, str]:
    alias_tokens = normalized_tokens(column_name)
    table_tokens = normalized_tokens(candidate_table, drop_modifiers=True)
    if not alias_tokens or not table_tokens:
        return 0, ""

    residual_tokens = alias_tokens[:-1] if alias_tokens[-1] == "id" else alias_tokens
    residual_set = set(residual_tokens)
    table_set = set(table_tokens)

    score = 0
    if residual_tokens:
        joined = "_".join(residual_tokens)
        normalized_table = normalize_name(candidate_table)
        if joined == normalized_table:
            score += 90
        if singularize_token(joined) == normalized_table:
            score += 72
        score += len(residual_set & table_set) * 18
        if residual_set == table_set:
            score += 34
        elif residual_set and residual_set.issubset(table_set):
            score += 18
        elif table_set and table_set.issubset(residual_set):
            score += 12

    if alias_tokens[-1] == "id":
        score += 18

    reason = f"`{column_name}` resembles `{candidate_table}.ID`"
    return score, reason


def get_soft_relationship_hints(table_name: str, limit: int = 10) -> dict[str, Any]:
    cache_key = (table_name, limit)
    cached = runtime_cache["relationship_hints"].get(cache_key)
    if cached is not None:
        return cached

    actual = get_relationships(table_name)
    columns = get_columns(table_name)
    object_info = get_table_object(table_name)

    if object_info.get("isInferred"):
        hints = {
            "actual": actual,
            "heuristicOutgoing": actual.get("outgoing", []),
            "heuristicIncoming": actual.get("incoming", []),
            "note": "Inferred views use Schema Atlas replacement logic. The relationship list reflects that inferred mapping.",
        }
        runtime_cache["relationship_hints"][cache_key] = hints
        return hints

    outgoing_candidates: list[dict[str, Any]] = []
    seen_outgoing: set[tuple[str, str]] = set()
    for column in columns:
        column_name = column["name"]
        normalized_column = normalize_name(column_name)
        if not normalized_column or normalized_column == "id":
            continue

        scored_tables: list[tuple[int, str, str]] = []
        for candidate_table in SNAPSHOT_BASE_TABLES:
            if candidate_table == table_name:
                continue
            related_index = SNAPSHOT_COLUMN_INDEXES.get(candidate_table, {}).get("exact", {})
            if "id" not in related_index:
                continue
            score, reason = table_name_match_score(column_name, candidate_table)
            if score < 55:
                continue
            scored_tables.append((score, candidate_table, reason))

        for score, candidate_table, reason in sorted(scored_tables, reverse=True)[:3]:
            key = (column_name, candidate_table)
            if key in seen_outgoing:
                continue
            seen_outgoing.add(key)
            outgoing_candidates.append(
                {
                    "columnName": column_name,
                    "referencedTable": candidate_table,
                    "referencedColumn": "ID",
                    "confidence": "high" if score >= 120 else "medium" if score >= 80 else "low",
                    "score": score,
                    "reason": reason,
                }
            )

    incoming_candidates: list[dict[str, Any]] = []
    seen_incoming: set[tuple[str, str]] = set()
    expected_fk_names = possible_fk_names_for_table(table_name)
    for candidate_table in SNAPSHOT_BASE_TABLES:
        if candidate_table == table_name:
            continue
        for candidate_column in SNAPSHOT_COLUMNS_BY_TABLE.get(candidate_table, []):
            column_name = candidate_column["COLUMN_NAME"]
            normalized_column = normalize_name(column_name)
            if normalized_column not in expected_fk_names:
                continue
            key = (candidate_table, column_name)
            if key in seen_incoming:
                continue
            seen_incoming.add(key)
            incoming_candidates.append(
                {
                    "sourceTable": candidate_table,
                    "sourceColumn": column_name,
                    "referencedColumn": "ID",
                    "confidence": "high",
                    "reason": f"`{candidate_table}.{column_name}` looks like it may point to `{table_name}.ID`",
                }
            )

    hints = {
        "actual": actual,
        "heuristicOutgoing": sorted(outgoing_candidates, key=lambda item: item["score"], reverse=True)[:limit],
        "heuristicIncoming": incoming_candidates[:limit],
        "note": (
            "These join hints are heuristic because the connected schema does not declare every foreign key. "
            "Use overlap checks or inferred views before treating them as confirmed joins."
        ),
    }
    runtime_cache["relationship_hints"][cache_key] = hints
    return hints


def ensure_runtime_appdata() -> None:
    RUNTIME_APPDATA.mkdir(parents=True, exist_ok=True)


def read_text_if_exists(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def compute_semantic_source_hash() -> str:
    digest = hashlib.sha256()
    digest.update(f"semantic-index-v{SEMANTIC_INDEX_VERSION}".encode("utf-8"))
    if SCHEMA_SNAPSHOT_PATH.exists():
        digest.update(SCHEMA_SNAPSHOT_PATH.read_bytes())
    if SEMANTIC_NOTES_PATH.exists():
        digest.update(SEMANTIC_NOTES_PATH.read_bytes())
    digest.update("|".join(inferred_view_names()).encode("utf-8"))
    return digest.hexdigest()


def split_markdown_sections(text: str) -> list[tuple[str, str]]:
    if not text.strip():
        return []

    sections: list[tuple[str, str]] = []
    current_title = "Schema Notes"
    current_lines: list[str] = []
    for line in text.splitlines():
        if line.startswith("## "):
            section_body = "\n".join(current_lines).strip()
            if section_body:
                sections.append((current_title, section_body))
            current_title = line[3:].strip()
            current_lines = []
            continue
        current_lines.append(line)

    section_body = "\n".join(current_lines).strip()
    if section_body:
        sections.append((current_title, section_body))
    return sections


def summarize_source_tables_for_spec(spec: dict[str, Any]) -> list[str]:
    source_tables: set[str] = set()
    for source in (spec.get("sources") or {}).values():
        if "." in source:
            source_tables.add(source.split(".", 1)[0])
    for edge in (spec.get("relationships") or {}).get("outgoing", []):
        if edge.get("referencedTable"):
            source_tables.add(str(edge["referencedTable"]))
    sql_text = str(spec.get("sql") or "")
    for table_name in SNAPSHOT_BASE_TABLES:
        if re.search(rf"\b{re.escape(table_name)}\b", sql_text):
            source_tables.add(table_name)
    return sorted(source_tables)


def build_semantic_documents() -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []

    note_text = read_text_if_exists(SEMANTIC_NOTES_PATH)
    for index, (title, body) in enumerate(split_markdown_sections(note_text), start=1):
        documents.append(
            {
                "id": f"note:{index}",
                "kind": "note",
                "title": title,
                "objectName": None,
                "sourceLabel": str(SEMANTIC_NOTES_PATH.name),
                "text": trim_chat_text(body, 2400),
            }
        )

    for row in sorted(SCHEMA_SNAPSHOT.get("tables", []), key=lambda item: item.get("TABLE_NAME", "")):
        table_name = str(row.get("TABLE_NAME") or "")
        if not table_name:
            continue
        object_type = str(row.get("TABLE_TYPE") or "TABLE")
        columns = snapshot_table_columns(table_name)
        column_summary = ", ".join(
            f"{column['COLUMN_NAME']} {column.get('COLUMN_TYPE') or column.get('DATA_TYPE') or ''}".strip()
            for column in columns[:80]
        )
        text = "\n".join(
            [
                f"Database object: {table_name}",
                f"Type: {object_type}",
                f"Engine: {row.get('ENGINE') or 'n/a'}",
                f"Comment: {row.get('TABLE_COMMENT') or 'none'}",
                f"Columns: {column_summary or 'none listed in snapshot'}",
            ]
        )
        documents.append(
            {
                "id": f"object:{table_name}",
                "kind": "object",
                "title": table_name,
                "objectName": table_name,
                "sourceLabel": object_type,
                "text": trim_chat_text(text, 2800),
            }
        )

    for view_name in inferred_view_names():
        try:
            spec = get_inferred_view_spec(view_name)
            columns = get_inferred_columns(view_name)
        except AppError:
            continue

        source_tables = summarize_source_tables_for_spec(spec)
        source_pairs = ", ".join(
            f"{column['name']} <= {column.get('sourceSummary') or 'unmapped'}" for column in columns[:80]
        )
        text = "\n".join(
            [
                f"Inferred Schema Atlas view: {view_name}",
                "Original database object: placeholder stub view that returned SELECT 1 constants",
                f"Description: {spec.get('description') or ''}",
                f"Source tables: {', '.join(source_tables) if source_tables else 'not yet resolved'}",
                f"Inferred columns: {source_pairs or 'none'}",
            ]
        )
        documents.append(
            {
                "id": f"inferred:{view_name}",
                "kind": "inferred_view",
                "title": view_name,
                "objectName": view_name,
                "sourceLabel": "Inferred View",
                "text": trim_chat_text(text, 3200),
            }
        )

    return documents


def ollama_embed(model: str, inputs: list[str], timeout_seconds: int = 600) -> list[list[float]]:
    if not inputs:
        return []
    payload = ollama_request(
        "/api/embed",
        payload={"model": model, "input": inputs, "truncate": True},
        timeout_seconds=timeout_seconds,
    )
    embeddings = payload.get("embeddings")
    if isinstance(embeddings, list) and embeddings and isinstance(embeddings[0], list):
        return [[float(value) for value in vector] for vector in embeddings]
    if isinstance(payload.get("embedding"), list):
        return [[float(value) for value in payload["embedding"]]]
    raise AppError(502, "Ollama did not return embeddings for the local semantic index build.")


def vector_norm(vector: list[float]) -> float:
    return math.sqrt(sum(value * value for value in vector))


def cosine_similarity(left: list[float], right: list[float], left_norm: float | None = None, right_norm: float | None = None) -> float:
    if not left or not right:
        return 0.0
    norm_left = left_norm if left_norm is not None else vector_norm(left)
    norm_right = right_norm if right_norm is not None else vector_norm(right)
    if norm_left <= 0 or norm_right <= 0:
        return 0.0
    return sum(a * b for a, b in zip(left, right)) / (norm_left * norm_right)


def load_semantic_index(force_reload: bool = False) -> dict[str, Any] | None:
    global semantic_index_cache
    if semantic_index_cache is not None and not force_reload:
        return semantic_index_cache
    if not SEMANTIC_INDEX_PATH.exists():
        semantic_index_cache = None
        return None
    try:
        semantic_index_cache = json.loads(SEMANTIC_INDEX_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise AppError(500, f"Could not read the local semantic index: {error}") from error
    return semantic_index_cache


def persist_semantic_index(payload: dict[str, Any]) -> None:
    global semantic_index_cache
    ensure_runtime_appdata()
    SEMANTIC_INDEX_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    semantic_index_cache = payload


def get_semantic_status(installed_names: set[str] | None = None) -> dict[str, Any]:
    installed_names = installed_names or set()
    resolved_model = resolve_local_model_name(SEMANTIC_EMBED_MODEL, installed_names) if installed_names else None
    status = dict(SEMANTIC_SEARCH_STATUS)
    status.update(
        {
            "embedModel": SEMANTIC_EMBED_MODEL,
            "embedModelResolvedName": resolved_model,
            "embedModelInstalled": bool(resolved_model),
            "indexPath": str(SEMANTIC_INDEX_PATH),
        }
    )

    try:
        payload = load_semantic_index()
    except AppError as error:
        status["lastError"] = error.message
        return status

    if not payload:
        return status

    source_hash = compute_semantic_source_hash()
    status.update(
        {
            "enabled": True,
            "indexed": True,
            "model": payload.get("model"),
            "documentCount": payload.get("documentCount", 0),
            "lastBuiltAt": payload.get("createdAt"),
            "sourceHash": payload.get("sourceHash"),
            "stale": payload.get("sourceHash") != source_hash,
            "reason": (
                "Local semantic index is available and Ask Atlas can retrieve schema notes and inferred mappings semantically."
            ),
        }
    )
    return status


def build_semantic_index() -> dict[str, Any]:
    installed = get_ollama_models()
    installed_names = {item.get("name", "") for item in installed}
    resolved_model = resolve_local_model_name(SEMANTIC_EMBED_MODEL, installed_names)
    if not resolved_model:
        raise AppError(
            400,
            f"Embedding model `{SEMANTIC_EMBED_MODEL}` is not installed in Ollama. Run `ollama pull {SEMANTIC_EMBED_MODEL}` locally, then build the index again.",
        )

    documents = build_semantic_documents()
    embedded_documents: list[dict[str, Any]] = []
    for start in range(0, len(documents), SEMANTIC_BATCH_SIZE):
        batch = documents[start : start + SEMANTIC_BATCH_SIZE]
        vectors = ollama_embed(resolved_model, [item["text"] for item in batch], timeout_seconds=600)
        if len(vectors) != len(batch):
            raise AppError(502, "Ollama returned an unexpected number of embeddings during the local index build.")
        for document, vector in zip(batch, vectors):
            normalized_vector = [round(float(value), 6) for value in vector]
            embedded_documents.append(
                {
                    **document,
                    "embedding": normalized_vector,
                    "norm": round(vector_norm(normalized_vector), 6),
                }
            )

    payload = {
        "version": SEMANTIC_INDEX_VERSION,
        "provider": "ollama",
        "model": resolved_model,
        "documentCount": len(embedded_documents),
        "createdAt": datetime.now().isoformat(timespec="seconds"),
        "sourceHash": compute_semantic_source_hash(),
        "documents": embedded_documents,
    }
    persist_semantic_index(payload)
    return get_semantic_status(installed_names)


def semantic_search(query_text: str, limit: int = SEMANTIC_TOP_K) -> dict[str, Any]:
    query_text = trim_chat_text(query_text or "", 2000)
    if not query_text:
        raise AppError(400, "Enter a semantic search query first.")

    index_payload = load_semantic_index()
    if not index_payload:
        raise AppError(400, "No local semantic index exists yet. Build the local index first.")

    installed = get_ollama_models()
    installed_names = {item.get("name", "") for item in installed}
    resolved_model = resolve_local_model_name(str(index_payload.get("model") or SEMANTIC_EMBED_MODEL), installed_names)
    if not resolved_model:
        raise AppError(
            400,
            f"The semantic index was built with `{index_payload.get('model')}`, but that embedding model is not currently available in Ollama.",
        )

    query_vector = ollama_embed(resolved_model, [query_text], timeout_seconds=180)[0]
    query_norm = vector_norm(query_vector)
    top_hits: list[dict[str, Any]] = []
    for document in index_payload.get("documents", []):
        score = cosine_similarity(
            query_vector,
            [float(value) for value in document.get("embedding") or []],
            left_norm=query_norm,
            right_norm=float(document.get("norm") or 0),
        )
        if score <= 0:
            continue
        top_hits.append(
            {
                "id": document.get("id"),
                "kind": document.get("kind"),
                "title": document.get("title"),
                "objectName": document.get("objectName"),
                "sourceLabel": document.get("sourceLabel"),
                "score": round(score, 4),
                "snippet": trim_chat_text(str(document.get("text") or ""), 600),
            }
        )

    top_hits.sort(key=lambda item: item["score"], reverse=True)
    return {
        "query": query_text,
        "model": resolved_model,
        "count": min(limit, len(top_hits)),
        "results": top_hits[:limit],
        "status": get_semantic_status(installed_names),
    }


def get_join_overlap_summary(
    left_table: str,
    left_column: str,
    right_table: str,
    right_column: str,
    sample_size: int = 200,
) -> dict[str, Any]:
    sample_size = clamp_integer(sample_size, 25, 500, 200)
    left_columns = {column["name"] for column in get_columns(left_table)}
    right_columns = {column["name"] for column in get_columns(right_table)}

    if left_column not in left_columns:
        raise AppError(400, f"Column `{left_column}` does not exist on `{left_table}`.")
    if right_column not in right_columns:
        raise AppError(400, f"Column `{right_column}` does not exist on `{right_table}`.")

    left_source = get_object_source_sql(left_table)
    right_source = get_object_source_sql(right_table)
    left_id = escape_identifier(left_column)
    right_id = escape_identifier(right_column)

    summary_rows = query(
        f"""
        SELECT
          COUNT(*) AS comparedRows,
          SUM(
            CASE
              WHEN EXISTS (
                SELECT 1
                FROM (SELECT * FROM {right_source}) r
                WHERE r.{right_id} = l.{left_id}
                LIMIT 1
              ) THEN 1
              ELSE 0
            END
          ) AS matchedRows
        FROM (
          SELECT src.{left_id}
          FROM (SELECT * FROM {left_source}) src
          WHERE src.{left_id} IS NOT NULL
          LIMIT %s
        ) l
        """,
        [sample_size],
    )
    summary = summary_rows[0] if summary_rows else {"comparedRows": 0, "matchedRows": 0}
    compared_rows = int(summary.get("comparedRows") or 0)
    matched_rows = int(summary.get("matchedRows") or 0)

    examples = query(
        f"""
        SELECT DISTINCT CAST(l.{left_id} AS CHAR) AS value
        FROM (
          SELECT src.{left_id}
          FROM (SELECT * FROM {left_source}) src
          WHERE src.{left_id} IS NOT NULL
          LIMIT %s
        ) l
        WHERE EXISTS (
          SELECT 1
          FROM (SELECT * FROM {right_source}) r
          WHERE r.{right_id} = l.{left_id}
          LIMIT 1
        )
        LIMIT 10
        """,
        [sample_size],
    )

    return {
        "leftTable": left_table,
        "leftColumn": left_column,
        "rightTable": right_table,
        "rightColumn": right_column,
        "sampleSize": sample_size,
        "comparedRows": compared_rows,
        "matchedRows": matched_rows,
        "matchRate": round((matched_rows / compared_rows), 3) if compared_rows else 0,
        "exampleMatches": [row["value"] for row in examples],
        "note": "This is a sampled overlap check. It helps validate likely joins in a schema that does not declare all foreign keys.",
    }


def trim_chat_text(value: str, limit: int = 6000) -> str:
    text = value.strip()
    if len(text) <= limit:
        return text
    return f"{text[:limit].rstrip()}\n... [truncated]"


def compact_rows(rows: list[dict[str, Any]], row_limit: int = 8, column_limit: int = 12) -> list[dict[str, Any]]:
    compacted: list[dict[str, Any]] = []
    for row in rows[:row_limit]:
        compacted.append({key: row[key] for key in list(row.keys())[:column_limit]})
    return compacted


def compact_columns(columns: list[dict[str, Any]], limit: int = 24) -> list[dict[str, Any]]:
    trimmed: list[dict[str, Any]] = []
    for column in columns[:limit]:
        trimmed.append(
            {
                "name": column.get("name"),
                "type": column.get("columnType"),
                "nullable": column.get("isNullable"),
                "key": column.get("columnKey"),
                "source": column.get("sourceSummary"),
            }
        )
    return trimmed


def ollama_request(path: str, payload: dict[str, Any] | None = None, timeout_seconds: int | None = None) -> dict[str, Any]:
    url = f"{OLLAMA_BASE_URL}{path}"
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    request_obj = Request(url, data=body, method="POST" if payload is not None else "GET")
    request_obj.add_header("Content-Type", "application/json")
    timeout = timeout_seconds or OLLAMA_TIMEOUT_SECONDS

    try:
        with urlopen(request_obj, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
    except HTTPError as error:
        detail = error.read().decode("utf-8", errors="ignore").strip()
        raise AppError(502, trim_chat_text(detail or f"Ollama request failed with status {error.code}.", 500)) from error
    except URLError as error:
        raise AppError(
            503,
            (
                f"Ollama is not reachable on {OLLAMA_BASE_URL}. Start Ollama locally and pull "
                "`qwen3:8b` and `deepseek-r1:8b` before using Ask Atlas."
            ),
        ) from error

    if not raw.strip():
        return {}

    try:
        return json.loads(raw)
    except json.JSONDecodeError as error:
        raise AppError(502, f"Unexpected Ollama response: {trim_chat_text(raw, 400)}") from error


def get_ollama_models() -> list[dict[str, Any]]:
    payload = ollama_request("/api/tags", timeout_seconds=20)
    return payload.get("models", [])


def model_installed(model_name: str, installed_names: set[str]) -> bool:
    if model_name in installed_names:
        return True
    family = model_name.split(":", 1)[0]
    target_tag = model_name.split(":", 1)[1] if ":" in model_name else ""
    for installed in installed_names:
        if installed == family:
            return True
        if not installed.startswith(family + ":"):
            continue
        if not target_tag or installed.endswith(":" + target_tag):
            return True
    return False


def resolve_local_model_name(model_name: str, installed_names: set[str]) -> str | None:
    if model_name in installed_names:
        return model_name
    family = model_name.split(":", 1)[0]
    target_tag = model_name.split(":", 1)[1] if ":" in model_name else ""
    family_matches = sorted(name for name in installed_names if name == family or name.startswith(family + ":"))
    if not family_matches:
        return None
    if target_tag:
        for match in family_matches:
            if match.endswith(":" + target_tag):
                return match
    return family_matches[0]


def get_llm_status() -> dict[str, Any]:
    try:
        installed = get_ollama_models()
        installed_names = {item.get("name", "") for item in installed}
        models = []
        for model in RECOMMENDED_LOCAL_MODELS:
            resolved_name = resolve_local_model_name(model["id"], installed_names)
            models.append(
                {
                    **model,
                    "installed": bool(resolved_name),
                    "resolvedName": resolved_name,
                }
            )
        embed_models = []
        for model in RECOMMENDED_EMBEDDING_MODELS:
            resolved_name = resolve_local_model_name(model["id"], installed_names)
            embed_models.append(
                {
                    **model,
                    "installed": bool(resolved_name),
                    "resolvedName": resolved_name,
                }
            )
        return {
            "ollamaReachable": True,
            "baseUrl": OLLAMA_BASE_URL,
            "models": models,
            "embeddingModels": embed_models,
            "installedModels": [
                {
                    "name": item.get("name"),
                    "size": item.get("size"),
                    "modifiedAt": item.get("modified_at"),
                }
                for item in installed
            ],
            "semanticSearch": get_semantic_status(installed_names),
        }
    except AppError as error:
        return {
            "ollamaReachable": False,
            "baseUrl": OLLAMA_BASE_URL,
            "models": [{**model, "installed": False} for model in RECOMMENDED_LOCAL_MODELS],
            "embeddingModels": [{**model, "installed": False} for model in RECOMMENDED_EMBEDDING_MODELS],
            "installedModels": [],
            "semanticSearch": get_semantic_status(set()),
            "error": error.message,
        }


def llm_tool_specs() -> list[dict[str, Any]]:
    return [
        {
            "type": "function",
            "function": {
                "name": "list_objects",
                "description": "List database tables, raw database views, or inferred views.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "object_type": {
                            "type": "string",
                            "enum": ["all", "tables", "views", "inferred"],
                            "description": "Which object family to list.",
                        },
                        "search": {
                            "type": "string",
                            "description": "Optional substring filter for object names.",
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of objects to return.",
                        },
                    },
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "describe_object",
                "description": "Describe a database table or view with column metadata and relationship hints.",
                "parameters": {
                    "type": "object",
                    "required": ["table_name"],
                    "properties": {
                        "table_name": {"type": "string", "description": "The exact table or view name."},
                    },
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "sample_rows",
                "description": "Return a small read-only sample of rows from a table or inferred view.",
                "parameters": {
                    "type": "object",
                    "required": ["table_name"],
                    "properties": {
                        "table_name": {"type": "string", "description": "The exact table or view name."},
                        "row_limit": {"type": "integer", "description": "How many rows to sample."},
                        "search": {"type": "string", "description": "Optional search string across visible columns."},
                        "sort": {"type": "string", "description": "Optional sort column."},
                        "direction": {"type": "string", "enum": ["ASC", "DESC"], "description": "Sort direction."},
                    },
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "profile_column",
                "description": "Profile one column for nulls, distinct counts, and common values.",
                "parameters": {
                    "type": "object",
                    "required": ["table_name", "column_name"],
                    "properties": {
                        "table_name": {"type": "string"},
                        "column_name": {"type": "string"},
                    },
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "search_schema",
                "description": "Search tables and column names by keyword.",
                "parameters": {
                    "type": "object",
                    "required": ["term"],
                    "properties": {
                        "term": {"type": "string", "description": "Keyword to search in table and column names."},
                    },
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "explain_inferred_view",
                "description": "Explain how Schema Atlas replaces a placeholder stub view with inferred source tables and columns.",
                "parameters": {
                    "type": "object",
                    "required": ["view_name"],
                    "properties": {
                        "view_name": {"type": "string", "description": "The inferred view name."},
                    },
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "suggest_relationships",
                "description": "Suggest likely join paths for an object, including heuristic candidates when foreign keys are missing.",
                "parameters": {
                    "type": "object",
                    "required": ["table_name"],
                    "properties": {
                        "table_name": {"type": "string"},
                        "limit": {"type": "integer", "description": "Maximum number of outgoing or incoming hints."},
                    },
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "check_join_overlap",
                "description": "Validate a likely join by checking sampled value overlap between two columns.",
                "parameters": {
                    "type": "object",
                    "required": ["left_table", "left_column", "right_table", "right_column"],
                    "properties": {
                        "left_table": {"type": "string"},
                        "left_column": {"type": "string"},
                        "right_table": {"type": "string"},
                        "right_column": {"type": "string"},
                        "sample_size": {"type": "integer", "description": "Rows to sample from the left side."},
                    },
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "run_read_only_sql",
                "description": "Run a read-only SQL statement against the connected database. Only SELECT/SHOW/DESCRIBE/EXPLAIN/WITH are allowed.",
                "parameters": {
                    "type": "object",
                    "required": ["sql"],
                    "properties": {
                        "sql": {"type": "string", "description": "A single read-only SQL statement with LIMIT where applicable."},
                    },
                },
            },
        },
    ]


def execute_llm_tool(tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    if tool_name == "list_objects":
        object_type = str(arguments.get("object_type") or "all")
        search = str(arguments.get("search") or "").strip()
        limit = clamp_integer(arguments.get("limit"), 1, 50, 20)
        objects = get_objects(search, object_type)[:limit]
        return {
            "objectType": object_type,
            "search": search,
            "count": len(objects),
            "objects": objects,
        }

    if tool_name == "describe_object":
        table_name = str(arguments.get("table_name") or "").strip()
        object_info = get_table_object(table_name)
        columns = compact_columns(get_columns(table_name), limit=12)
        hints = get_soft_relationship_hints(table_name, limit=8)
        return {
            "object": object_info,
            "columns": columns,
            "relationshipHints": {
                "note": hints["note"],
                "actual": hints["actual"],
                "heuristicOutgoing": hints["heuristicOutgoing"][:4],
                "heuristicIncoming": hints["heuristicIncoming"][:4],
            },
        }

    if tool_name == "sample_rows":
        table_name = str(arguments.get("table_name") or "").strip()
        row_limit = clamp_integer(arguments.get("row_limit"), 1, 25, 8)
        payload = get_data_slice(
            table_name,
            {
                "page": 1,
                "pageSize": row_limit,
                "search": str(arguments.get("search") or "").strip(),
                "sort": str(arguments.get("sort") or "").strip(),
                "direction": str(arguments.get("direction") or "ASC").upper(),
            },
        )
        return {
            "object": payload["objectInfo"],
            "columns": compact_columns(payload["columns"], limit=20),
            "rows": compact_rows(payload["rows"], row_limit=row_limit, column_limit=16),
            "total": payload["total"],
            "totalIsEstimate": payload["totalIsEstimate"],
        }

    if tool_name == "profile_column":
        table_name = str(arguments.get("table_name") or "").strip()
        column_name = str(arguments.get("column_name") or "").strip()
        payload = get_column_profile(table_name, column_name)
        payload["topValues"] = payload["topValues"][:10]
        return payload

    if tool_name == "search_schema":
        term = str(arguments.get("term") or "").strip()
        payload = search_schema(term)
        return {
            "tables": payload["tables"][:15],
            "columns": payload["columns"][:20],
        }

    if tool_name == "explain_inferred_view":
        view_name = str(arguments.get("view_name") or "").strip()
        spec = get_inferred_view_spec(view_name)
        definition = get_inferred_definition(view_name)
        columns = compact_columns(get_inferred_columns(view_name), limit=30)
        return {
            "view": view_name,
            "description": spec["description"],
            "columns": columns[:12],
            "relationships": spec["relationships"],
            "replacementQuery": trim_chat_text(definition["statement"], 1200),
        }

    if tool_name == "suggest_relationships":
        table_name = str(arguments.get("table_name") or "").strip()
        limit = clamp_integer(arguments.get("limit"), 1, 20, 8)
        return get_soft_relationship_hints(table_name, limit=limit)

    if tool_name == "check_join_overlap":
        return get_join_overlap_summary(
            str(arguments.get("left_table") or "").strip(),
            str(arguments.get("left_column") or "").strip(),
            str(arguments.get("right_table") or "").strip(),
            str(arguments.get("right_column") or "").strip(),
            sample_size=arguments.get("sample_size") or 200,
        )

    if tool_name == "run_read_only_sql":
        sql = str(arguments.get("sql") or "").strip()
        result = run_sql(sql)
        return {
            "columns": result["columns"],
            "rows": compact_rows(result["rows"], row_limit=12, column_limit=16),
            "rowCount": len(result["rows"]),
            "note": "This tool is read-only. Schema Atlas blocks write and DDL statements.",
        }

    raise AppError(400, f"Unknown Ask Atlas tool `{tool_name}`.")


def extract_chat_keywords(prompt: str, limit: int = 4) -> list[str]:
    words = re.findall(r"[A-Za-z_][A-Za-z0-9_]{2,}", prompt.lower())
    keywords: list[str] = []
    for word in words:
        if word in CHAT_STOPWORDS:
            continue
        if word not in keywords:
            keywords.append(word)
        if len(keywords) >= limit:
            break
    return keywords


def build_ask_atlas_context(user_prompt: str, selected_object: str = "") -> tuple[str, list[dict[str, Any]]]:
    prompt_lower = user_prompt.lower()
    tool_trace: list[dict[str, Any]] = []
    sections: list[str] = []

    def run_context_tool(name: str, arguments: dict[str, Any], section_title: str, trim_limit: int = 2400) -> None:
        result = execute_llm_tool(name, arguments)
        preview = trim_chat_text(json.dumps(result, indent=2, ensure_ascii=False), trim_limit)
        tool_trace.append({"name": name, "arguments": arguments, "resultPreview": preview})
        sections.append(f"{section_title}:\n{preview}")

    semantic_status = get_semantic_status()
    if semantic_status.get("indexed"):
        try:
            semantic_payload = semantic_search(user_prompt, limit=4)
        except AppError:
            semantic_payload = None
        if semantic_payload and semantic_payload.get("results"):
            preview = trim_chat_text(json.dumps(semantic_payload["results"], indent=2, ensure_ascii=False), 2400)
            tool_trace.append(
                {
                    "name": "semantic_search",
                    "arguments": {"query": trim_chat_text(user_prompt, 160), "limit": 4},
                    "resultPreview": preview,
                }
            )
            sections.append(f"Semantic schema retrieval:\n{preview}")

    if selected_object:
        run_context_tool("describe_object", {"table_name": selected_object}, "Selected object summary")

        if any(token in prompt_lower for token in ("sample", "row", "value", "example", "record", "data")):
            run_context_tool("sample_rows", {"table_name": selected_object, "row_limit": 4}, "Selected object sample rows")

        if any(token in prompt_lower for token in ("join", "related", "relationship", "link", "foreign key", "fk")):
            run_context_tool("suggest_relationships", {"table_name": selected_object, "limit": 8}, "Selected object relationship hints")

        if selected_object.endswith("_view") and is_inferred_view(selected_object) and any(
            token in prompt_lower for token in ("inferred", "placeholder", "source", "built", "join", "derive")
        ):
            run_context_tool("explain_inferred_view", {"view_name": selected_object}, "Inferred view explanation", trim_limit=3200)

    keywords = extract_chat_keywords(user_prompt)
    if keywords and not selected_object:
        search_term = keywords[0]
        run_context_tool("search_schema", {"term": search_term}, f"Schema search for `{search_term}`", trim_limit=1800)

    if not selected_object and not keywords:
        overview = get_overview()
        overview_preview = trim_chat_text(json.dumps(overview, indent=2, ensure_ascii=False), 1800)
        tool_trace.append({"name": "get_overview", "arguments": {}, "resultPreview": overview_preview})
        sections.append(f"Schema overview:\n{overview_preview}")

    context_text = "\n\n".join(sections)
    return context_text, tool_trace


def sanitize_chat_messages(messages: Any) -> list[dict[str, str]]:
    if not isinstance(messages, list):
        return []

    cleaned: list[dict[str, str]] = []
    for item in messages[-CHAT_MESSAGE_LIMIT:]:
        if not isinstance(item, dict):
            continue
        role = str(item.get("role") or "").strip().lower()
        if role not in {"user", "assistant"}:
            continue
        content = trim_chat_text(str(item.get("content") or ""), 4000)
        if not content:
            continue
        cleaned.append({"role": role, "content": content})
    return cleaned


def build_chat_messages(messages: list[dict[str, str]], selected_object: str = "") -> list[dict[str, Any]]:
    system_content = ASK_ATLAS_SYSTEM_PROMPT.strip()
    if selected_object:
        system_content += (
            f"\n\nCurrent selected object in the Schema Atlas UI: `{selected_object}`. "
            "Use that as context when the user says 'this table', 'this view', or similar."
        )
    return [{"role": "system", "content": system_content}, *messages]


def ollama_chat(model: str, messages: list[dict[str, Any]], tools: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "stream": False,
        "think": False,
    }
    if tools:
        payload["tools"] = tools
    return ollama_request("/api/chat", payload=payload, timeout_seconds=OLLAMA_TIMEOUT_SECONDS)


def ask_atlas(model: str, messages: list[dict[str, Any]], selected_object: str = "") -> dict[str, Any]:
    ensure_connected()
    safe_messages = sanitize_chat_messages(messages)
    if not safe_messages or safe_messages[-1]["role"] != "user":
        raise AppError(400, "Ask Atlas expects the latest chat message to be from the user.")

    allowed_models = {item["id"] for item in RECOMMENDED_LOCAL_MODELS}
    if model not in allowed_models:
        raise AppError(400, f"Unsupported local model `{model}`.")

    status = get_llm_status()
    if not status["ollamaReachable"]:
        raise AppError(503, status.get("error") or "Ollama is not reachable.")
    model_status = next((item for item in status["models"] if item["id"] == model), None)
    if not model_status or not model_status["installed"]:
        raise AppError(
            400,
            f"Model `{model}` is not installed in Ollama. Run `ollama pull {model}` locally, then try again.",
        )
    resolved_model = model_status.get("resolvedName") or model

    latest_user_message = safe_messages[-1]["content"]
    context_text, tool_trace = build_ask_atlas_context(latest_user_message, selected_object=selected_object)
    conversation = build_chat_messages(safe_messages, selected_object=selected_object)
    if context_text:
        conversation.insert(
            1,
            {
                "role": "system",
                "content": (
                    "Schema Atlas gathered the following read-only context before your answer. "
                    "Use it directly. If something is uncertain, say so.\n\n"
                    f"{context_text}"
                ),
            },
        )

    payload = ollama_chat(resolved_model, conversation)
    message = payload.get("message") or {}
    thinking = trim_chat_text(str(message.get("thinking") or ""), 2500) if message.get("thinking") else ""
    return {
        "reply": trim_chat_text(str(message.get("content") or ""), 8000),
        "toolTrace": tool_trace,
        "model": model,
        "resolvedModel": resolved_model,
        "thinking": thinking,
    }


def sanitize_config(raw: dict[str, Any] | None) -> dict[str, Any]:
    raw = raw or {}
    config = {
        "host": str(raw.get("host") or DEFAULT_CONFIG["host"]).strip() or DEFAULT_CONFIG["host"],
        "port": int(raw.get("port") or DEFAULT_CONFIG["port"]),
        "user": str(raw.get("user") or DEFAULT_CONFIG["user"]).strip() or DEFAULT_CONFIG["user"],
        "password": str(raw.get("password") or ""),
        "database": str(raw.get("database") or DEFAULT_CONFIG["database"]).strip() or DEFAULT_CONFIG["database"],
        "sslMode": str(raw.get("sslMode") or DEFAULT_CONFIG["sslMode"]).strip() or DEFAULT_CONFIG["sslMode"],
    }

    if not (1 <= config["port"] <= 65535):
        raise AppError(400, "Port must be a valid TCP port.")
    if not config["database"]:
        raise AppError(400, "Database name is required.")
    return config


def connection_public_view(config: dict[str, Any], is_connected: bool = False) -> dict[str, Any]:
    return {
        "host": config["host"],
        "port": config["port"],
        "user": config["user"],
        "database": config["database"],
        "sslMode": config["sslMode"],
        "hasPassword": bool(config["password"]),
        "connected": is_connected,
        "client": "mysqlsh",
    }


def normalize_cell(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (bytes, bytearray)):
        return f"<binary {len(value)} bytes>"
    return value


def normalize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{key: normalize_cell(value) for key, value in row.items()} for row in rows]


def clamp_integer(value: Any, minimum: int, maximum: int, fallback: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return fallback
    return max(minimum, min(maximum, parsed))


def escape_identifier(value: str) -> str:
    return f"`{value.replace('`', '``')}`"


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float, Decimal)):
        return str(value)
    text = str(value).replace("\\", "\\\\").replace("'", "''")
    return f"'{text}'"


def format_sql(sql: str, params: list[Any] | tuple[Any, ...] = ()) -> str:
    parts = sql.split("%s")
    if len(parts) - 1 != len(params):
        raise AppError(500, "SQL formatting mismatch.")
    rendered = parts[0]
    for index, value in enumerate(params):
        rendered += sql_literal(value) + parts[index + 1]
    return rendered


def mysqlsh_env() -> dict[str, str]:
    env = os.environ.copy()
    RUNTIME_APPDATA.mkdir(exist_ok=True)
    env["APPDATA"] = str(RUNTIME_APPDATA)
    return env


def run_mysqlsh_sql(config: dict[str, Any], sql: str, timeout_seconds: int = 60) -> dict[str, Any]:
    command = [
        str(MYSQLSH_EXE),
        "--sql",
        "--mc",
        "--json=raw",
        "--result-format=json/raw",
        f"--host={config['host']}",
        f"--port={config['port']}",
        f"--user={config['user']}",
        f"--database={config['database']}",
        f"--ssl-mode={config['sslMode']}",
        "--passwords-from-stdin",
        "--quiet-start=2",
        "--log-level=1",
        "-e",
        sql,
    ]

    process = subprocess.run(
        command,
        input=f"{config['password']}\n",
        text=True,
        capture_output=True,
        env=mysqlsh_env(),
        timeout=timeout_seconds,
    )

    if process.returncode != 0:
        message = (process.stderr or process.stdout or "MySQL Shell query failed.").strip()
        message = re.sub(r"\s+", " ", message)
        raise AppError(400, message)

    output = process.stdout.strip()
    if not output:
        return {"rows": [], "warnings": []}

    try:
        return json.loads(output)
    except json.JSONDecodeError as error:
        raise AppError(500, f"Unexpected MySQL Shell output: {output[:300]}") from error


def connect_database(config: dict[str, Any]) -> dict[str, Any]:
    global active_config, connected
    run_mysqlsh_sql(config, "SELECT 1 AS ok", timeout_seconds=20)
    active_config = config.copy()
    connected = True
    clear_runtime_cache()
    return connection_public_view(active_config, True)


def ensure_connected() -> None:
    if connected:
        return
    if not active_config["password"]:
        raise AppError(400, "Not connected. Enter the database password and connect first.")
    connect_database(active_config)


def query(sql: str, params: list[Any] | tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    ensure_connected()
    rendered_sql = format_sql(sql, params)
    payload = run_mysqlsh_sql(active_config, rendered_sql, timeout_seconds=90)
    return normalize_rows(payload.get("rows", []))


def get_table_object(table_name: str) -> dict[str, Any]:
    cached = runtime_cache["table_object"].get(table_name)
    if cached is not None:
        return cached

    rows = query(
        """
        SELECT
          TABLE_NAME AS name,
          TABLE_TYPE AS type,
          ENGINE AS engine,
          TABLE_ROWS AS estimatedRows,
          ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2) AS sizeMb,
          TABLE_COLLATION AS collation,
          CREATE_TIME AS createdAt,
          UPDATE_TIME AS updatedAt,
          TABLE_COMMENT AS comment
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        LIMIT 1
        """,
        [active_config["database"], table_name],
    )

    if not rows and not is_inferred_view(table_name):
        raise AppError(404, f"Object `{table_name}` was not found in schema `{active_config['database']}`.")

    object_info = rows[0] if rows else {"name": table_name, "type": "VIEW"}
    object_info["isInferred"] = False

    if is_inferred_view(table_name):
        spec = get_inferred_view_spec(table_name)
        object_info.update(
            {
                "name": table_name,
                "type": "VIEW",
                "engine": "Schema Atlas",
                "estimatedRows": None,
                "sizeMb": None,
                "comment": spec["description"],
                "isInferred": True,
            }
        )

    runtime_cache["table_object"][table_name] = object_info
    return object_info


def get_columns(table_name: str) -> list[dict[str, Any]]:
    if is_inferred_view(table_name):
        return get_inferred_columns(table_name)

    cached = runtime_cache["columns"].get(table_name)
    if cached is not None:
        return cached

    rows = query(
        """
        SELECT
          COLUMN_NAME AS name,
          ORDINAL_POSITION AS ordinalPosition,
          COLUMN_TYPE AS columnType,
          DATA_TYPE AS dataType,
          IS_NULLABLE AS isNullable,
          COLUMN_DEFAULT AS columnDefault,
          COLUMN_KEY AS columnKey,
          EXTRA AS extra,
          COLUMN_COMMENT AS comment
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
        """,
        [active_config["database"], table_name],
    )
    runtime_cache["columns"][table_name] = rows
    return rows


def get_indexes(table_name: str) -> list[dict[str, Any]]:
    if is_inferred_view(table_name):
        runtime_cache["indexes"][table_name] = []
        return []

    cached = runtime_cache["indexes"].get(table_name)
    if cached is not None:
        return cached

    rows = query(f"SHOW INDEX FROM {escape_identifier(table_name)}")
    runtime_cache["indexes"][table_name] = rows
    return rows


def get_definition(table_name: str, object_type: str) -> dict[str, Any]:
    if is_inferred_view(table_name):
        return get_inferred_definition(table_name)

    cached = runtime_cache["definition"].get(table_name)
    if cached is not None:
        return cached

    statement_type = "VIEW" if object_type == "VIEW" else "TABLE"
    rows = query(f"SHOW CREATE {statement_type} {escape_identifier(table_name)}")
    first = rows[0] if rows else {}
    definition = {"statement": first.get("Create View") or first.get("Create Table") or ""}
    runtime_cache["definition"][table_name] = definition
    return definition


def get_relationships(table_name: str) -> dict[str, Any]:
    if is_inferred_view(table_name):
        return get_inferred_relationships(table_name)

    cached = runtime_cache["relationships"].get(table_name)
    if cached is not None:
        return cached

    outgoing = query(
        """
        SELECT
          kcu.CONSTRAINT_NAME AS constraintName,
          kcu.COLUMN_NAME AS columnName,
          kcu.REFERENCED_TABLE_NAME AS referencedTable,
          kcu.REFERENCED_COLUMN_NAME AS referencedColumn
        FROM information_schema.KEY_COLUMN_USAGE kcu
        WHERE kcu.TABLE_SCHEMA = %s
          AND kcu.TABLE_NAME = %s
          AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
        ORDER BY kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
        """,
        [active_config["database"], table_name],
    )

    incoming = query(
        """
        SELECT
          kcu.TABLE_NAME AS sourceTable,
          kcu.COLUMN_NAME AS sourceColumn,
          kcu.CONSTRAINT_NAME AS constraintName,
          kcu.REFERENCED_COLUMN_NAME AS referencedColumn
        FROM information_schema.KEY_COLUMN_USAGE kcu
        WHERE kcu.TABLE_SCHEMA = %s
          AND kcu.REFERENCED_TABLE_SCHEMA = %s
          AND kcu.REFERENCED_TABLE_NAME = %s
        ORDER BY kcu.TABLE_NAME, kcu.CONSTRAINT_NAME
        """,
        [active_config["database"], active_config["database"], table_name],
    )
    relationships = {"outgoing": outgoing, "incoming": incoming}
    runtime_cache["relationships"][table_name] = relationships
    return relationships


def build_search_clause(columns: list[dict[str, Any]], term: str) -> tuple[str, list[Any]]:
    if not term:
        return "", []
    searchable = columns[:10]
    if not searchable:
        return "", []
    like_value = f"%{term}%"
    checks = [f"CAST({escape_identifier(column['name'])} AS CHAR) LIKE %s" for column in searchable]
    return f"WHERE {' OR '.join(checks)}", [like_value] * len(searchable)


def get_object_source_sql(table_name: str) -> str:
    if is_inferred_view(table_name):
        sql = get_inferred_view_spec(table_name)["sql"].strip()
        return f"({sql}) AS atlas_inferred_view"
    return escape_identifier(table_name)


def get_data_slice(table_name: str, options: dict[str, Any]) -> dict[str, Any]:
    object_info = get_table_object(table_name)
    columns = get_columns(table_name)
    source_sql = get_object_source_sql(table_name)
    page = clamp_integer(options.get("page"), 1, 5000, 1)
    page_size = clamp_integer(options.get("pageSize"), 10, 200, 50)
    offset = (page - 1) * page_size
    search = str(options.get("search") or "").strip()
    direction = "DESC" if str(options.get("direction") or "").upper() == "DESC" else "ASC"
    column_names = {column["name"] for column in columns}
    default_sort = columns[0]["name"] if columns else ""
    sort_column = options.get("sort") if options.get("sort") in column_names else default_sort
    order_sql = f"ORDER BY {escape_identifier(sort_column)} {direction}" if sort_column else ""
    where_sql, where_params = build_search_clause(columns, search)

    if search:
        count_rows = query(
            f"SELECT COUNT(*) AS total FROM {source_sql} {where_sql}",
            where_params,
        )
        total = int(count_rows[0]["total"]) if count_rows else 0
        total_is_estimate = False
    else:
        estimated_rows = object_info.get("estimatedRows")
        total = int(estimated_rows) if estimated_rows is not None else None
        total_is_estimate = total is not None

    rows = query(
        f"""
        SELECT *
        FROM {source_sql}
        {where_sql}
        {order_sql}
        LIMIT %s OFFSET %s
        """,
        [*where_params, page_size, offset],
    )

    return {
        "objectInfo": object_info,
        "page": page,
        "pageSize": page_size,
        "total": total,
        "totalIsEstimate": total_is_estimate,
        "sort": sort_column,
        "direction": direction,
        "search": search,
        "columns": columns,
        "rows": rows,
    }


def get_column_profile(table_name: str, column_name: str) -> dict[str, Any]:
    columns = get_columns(table_name)
    allowed_columns = {column["name"] for column in columns}
    if column_name not in allowed_columns:
        raise AppError(400, f"Column `{column_name}` is not available on `{table_name}`.")

    target = escape_identifier(column_name)
    table_ref = get_object_source_sql(table_name)
    summary = query(
        f"""
        SELECT
          COUNT(*) AS totalRows,
          SUM(CASE WHEN {target} IS NULL THEN 1 ELSE 0 END) AS nullCount,
          COUNT(DISTINCT {target}) AS distinctCount
        FROM {table_ref}
        """
    )[0]

    top_values = query(
        f"""
        SELECT
          CAST({target} AS CHAR) AS value,
          COUNT(*) AS count
        FROM {table_ref}
        GROUP BY {target}
        ORDER BY count DESC
        LIMIT 12
        """
    )
    return {"summary": summary, "topValues": top_values}


def export_rows(table_name: str, fmt: str, options: dict[str, Any]) -> tuple[str, str]:
    slice_payload = get_data_slice(
        table_name,
        {
            **options,
            "page": 1,
            "pageSize": clamp_integer(options.get("pageSize"), 10, 5000, 500),
        },
    )

    if fmt == "json":
        return "application/json; charset=utf-8", json.dumps(slice_payload["rows"], indent=2)

    buffer = io.StringIO()
    rows = slice_payload["rows"]
    if rows:
        writer = csv.DictWriter(buffer, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return "text/csv; charset=utf-8", buffer.getvalue()


def get_overview() -> dict[str, Any]:
    cached = runtime_cache["overview"]
    if cached is not None:
        return cached

    objects = query(
        """
        SELECT
          TABLE_NAME AS name,
          TABLE_TYPE AS type,
          ENGINE AS engine,
          TABLE_ROWS AS estimatedRows,
          ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2) AS sizeMb,
          UPDATE_TIME AS updatedAt
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s
        ORDER BY TABLE_NAME
        """,
        [active_config["database"]],
    )

    relation_count = query(
        """
        SELECT COUNT(*) AS count
        FROM information_schema.REFERENTIAL_CONSTRAINTS
        WHERE CONSTRAINT_SCHEMA = %s
        """,
        [active_config["database"]],
    )[0]["count"]

    total_tables = sum(1 for item in objects if item["type"] == "BASE TABLE")
    total_views = sum(1 for item in objects if item["type"] == "VIEW")
    total_inferred_views = sum(1 for item in objects if item["type"] == "VIEW" and is_inferred_view(item["name"]))
    total_estimated_rows = sum(int(item["estimatedRows"] or 0) for item in objects)
    total_size_mb = round(sum(float(item["sizeMb"] or 0) for item in objects), 2)
    largest_objects = sorted(objects, key=lambda item: float(item["sizeMb"] or 0), reverse=True)[:8]

    prefix_map: dict[str, int] = {}
    for item in objects:
        prefix = item["name"].split("_")[0] if "_" in item["name"] else item["name"][:1]
        prefix_map[prefix] = prefix_map.get(prefix, 0) + 1

    clusters = sorted(
        [{"name": name, "count": count} for name, count in prefix_map.items()],
        key=lambda item: item["count"],
        reverse=True,
    )[:10]

    overview = {
        "schema": active_config["database"],
        "totals": {
            "tables": total_tables,
            "views": total_views,
            "inferredViews": total_inferred_views,
            "relationships": int(relation_count or 0),
            "estimatedRows": total_estimated_rows,
            "sizeMb": total_size_mb,
        },
        "largestObjects": largest_objects,
        "clusters": clusters,
    }
    runtime_cache["overview"] = overview
    return overview


def get_saved_connections() -> list[dict[str, Any]]:
    if not MYSQLSH_GUI_DB.exists():
        return []

    connection = sqlite3.connect(MYSQLSH_GUI_DB)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            SELECT id, caption, description, options
            FROM db_connection
            WHERE db_type = 'MySQL'
            ORDER BY caption
            """
        ).fetchall()
    finally:
        connection.close()

    saved_connections = []
    for row in rows:
        options = json.loads(row["options"] or "{}")
        caption = str(row["caption"] or "").strip()
        saved_connections.append(
            {
                "id": row["id"],
                "caption": caption,
                "description": row["description"],
                "host": options.get("host") or DEFAULT_CONFIG["host"],
                "port": int(options.get("port") or DEFAULT_CONFIG["port"]),
                "user": options.get("user") or DEFAULT_CONFIG["user"],
                "database": active_config["database"],
                "sslMode": options.get("ssl-mode") or DEFAULT_CONFIG["sslMode"],
                "preferred": bool(PREFERRED_CONNECTION_CAPTION and caption.lower() == PREFERRED_CONNECTION_CAPTION),
            }
        )
    return saved_connections


def get_objects(search: str = "", object_type: str = "all") -> list[dict[str, Any]]:
    cache_key = (search, object_type)
    cached = runtime_cache["objects"].get(cache_key)
    if cached is not None:
        return cached

    params: list[Any] = [active_config["database"]]
    where_sql = "WHERE TABLE_SCHEMA = %s"

    if object_type == "tables":
        where_sql += " AND TABLE_TYPE = 'BASE TABLE'"
    elif object_type in {"views", "inferred"}:
        where_sql += " AND TABLE_TYPE = 'VIEW'"

    if search:
        where_sql += " AND TABLE_NAME LIKE %s"
        params.append(f"%{search}%")

    rows = query(
        f"""
        SELECT
          TABLE_NAME AS name,
          TABLE_TYPE AS type,
          ENGINE AS engine,
          TABLE_ROWS AS estimatedRows,
          ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2) AS sizeMb,
          UPDATE_TIME AS updatedAt
        FROM information_schema.TABLES
        {where_sql}
        ORDER BY TABLE_TYPE DESC, TABLE_NAME ASC
        """,
        params,
    )

    for row in rows:
        row["isInferred"] = is_inferred_view(row["name"])
        if row["isInferred"]:
            row["engine"] = "Schema Atlas"
            row["estimatedRows"] = None
            row["sizeMb"] = None
            row["comment"] = get_inferred_view_spec(row["name"])["description"]

    if object_type == "inferred":
        rows = [row for row in rows if row["isInferred"]]

    runtime_cache["objects"][cache_key] = rows
    return rows


def search_schema(term: str) -> dict[str, Any]:
    cached = runtime_cache["search"].get(term)
    if cached is not None:
        return cached

    query_term = f"%{term}%"
    tables = query(
        """
        SELECT
          TABLE_NAME AS objectName,
          TABLE_TYPE AS objectType
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME LIKE %s
        ORDER BY TABLE_NAME
        LIMIT 25
        """,
        [active_config["database"], query_term],
    )
    columns = query(
        """
        SELECT
          TABLE_NAME AS objectName,
          COLUMN_NAME AS columnName,
          COLUMN_TYPE AS columnType
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND COLUMN_NAME LIKE %s
        ORDER BY TABLE_NAME, ORDINAL_POSITION
        LIMIT 40
        """,
        [active_config["database"], query_term],
    )
    payload = {"tables": tables, "columns": columns}
    runtime_cache["search"][term] = payload
    return payload


def strip_leading_sql_comments(sql: str) -> str:
    return re.sub(r"^(?:\s|--.*?$|/\*[\s\S]*?\*/)+", "", sql, flags=re.MULTILINE).strip()


def assert_read_only_sql(sql: str) -> str:
    normalized = strip_leading_sql_comments(sql)
    first_token = (normalized.split()[:1] or [""])[0].upper()
    allowed = {"SELECT", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "WITH"}

    if first_token not in allowed:
        raise AppError(400, "Only read-only SQL is allowed. Use SELECT, SHOW, DESCRIBE, DESC, EXPLAIN, or WITH.")

    blocked_pattern = re.compile(
        r"\b(INSERT|UPDATE|DELETE|MERGE|REPLACE|DROP|TRUNCATE|ALTER|CREATE|RENAME|GRANT|REVOKE|CALL|SET|LOCK|UNLOCK|LOAD|HANDLER|ANALYZE|OPTIMIZE|REPAIR)\b",
        re.IGNORECASE,
    )
    if blocked_pattern.search(normalized):
        raise AppError(400, "Write or administrative SQL is blocked in this viewer.")

    if first_token in {"SELECT", "WITH"} and "LIMIT" not in normalized.upper() and "COUNT(" not in normalized.upper():
        raise AppError(400, "Add a LIMIT clause for ad hoc SELECT queries to keep responses manageable.")

    if len([segment for segment in normalized.split(";") if segment.strip()]) > 1:
        raise AppError(400, "Submit one statement at a time.")
    return normalized


def run_sql(sql: str) -> dict[str, Any]:
    safe_sql = assert_read_only_sql(sql)
    payload = run_mysqlsh_sql(active_config, safe_sql, timeout_seconds=90)
    rows = normalize_rows(payload.get("rows", []))
    columns = list(rows[0].keys()) if rows else []
    return {"columns": columns, "rows": rows}


@app.errorhandler(AppError)
def handle_app_error(error: AppError) -> tuple[Response, int]:
    return jsonify({"error": error.message}), error.status


@app.errorhandler(Exception)
def handle_unknown_error(error: Exception) -> tuple[Response, int]:
    return jsonify({"error": str(error)}), 500


@app.get("/api/health")
def health() -> Response:
    return jsonify({"ok": True, "connection": connection_public_view(active_config, connected)})


@app.get("/api/config")
def config() -> Response:
    return jsonify(connection_public_view(active_config, connected))


@app.post("/api/connect")
def connect() -> Response:
    payload = request.get_json(silent=True) or {}
    config = sanitize_config(payload)
    return jsonify(connect_database(config))


@app.get("/api/overview")
def overview() -> Response:
    return jsonify(get_overview())


@app.get("/api/objects")
def objects() -> Response:
    search = (request.args.get("search") or "").strip()
    object_type = (request.args.get("type") or "all").strip()
    return jsonify(get_objects(search, object_type))


@app.get("/api/saved-connections")
def saved_connections() -> Response:
    return jsonify(get_saved_connections())


@app.get("/api/search")
def schema_search() -> Response:
    term = (request.args.get("q") or "").strip()
    if not term:
        return jsonify({"tables": [], "columns": []})
    return jsonify(search_schema(term))


@app.post("/api/sql/query")
def sql_query() -> Response:
    payload = request.get_json(silent=True) or {}
    return jsonify(run_sql(str(payload.get("sql") or "")))


@app.get("/api/llm/status")
def llm_status() -> Response:
    return jsonify(get_llm_status())


@app.post("/api/llm/chat")
def llm_chat() -> Response:
    payload = request.get_json(silent=True) or {}
    return jsonify(
        ask_atlas(
            model=str(payload.get("model") or "qwen3:8b"),
            messages=payload.get("messages") or [],
            selected_object=str(payload.get("selectedObject") or "").strip(),
        )
    )


@app.post("/api/semantic/build")
def semantic_build() -> Response:
    return jsonify({"semanticSearch": build_semantic_index()})


@app.get("/api/semantic/search")
def semantic_search_route() -> Response:
    query_text = (request.args.get("q") or "").strip()
    limit = clamp_integer(request.args.get("limit"), 1, 12, SEMANTIC_TOP_K)
    return jsonify(semantic_search(query_text, limit=limit))


@app.get("/api/objects/<path:table_name>")
def object_details(table_name: str) -> Response:
    object_info = get_table_object(table_name)
    columns = get_columns(table_name)
    indexes = [] if object_info["type"] == "VIEW" else get_indexes(table_name)
    relationships = get_relationships(table_name)
    definition = get_definition(table_name, object_info["type"])
    return jsonify({
        "objectInfo": object_info,
        "columns": columns,
        "indexes": indexes,
        "relationships": relationships,
        "definition": definition,
    })


@app.get("/api/objects/<path:table_name>/sections/<section_name>")
def object_section(table_name: str, section_name: str) -> Response:
    object_info = get_table_object(table_name)

    if section_name == "schema":
        return jsonify({"columns": get_columns(table_name)})
    if section_name == "relationships":
        return jsonify({"relationships": get_relationships(table_name)})
    if section_name == "indexes":
        indexes = [] if object_info["type"] == "VIEW" else get_indexes(table_name)
        return jsonify({"indexes": indexes})
    if section_name == "definition":
        return jsonify({"definition": get_definition(table_name, object_info["type"])})

    raise AppError(404, f"Unknown object section `{section_name}`.")


@app.get("/api/objects/<path:table_name>/data")
def object_data(table_name: str) -> Response:
    return jsonify(
        get_data_slice(
            table_name,
            {
                "page": request.args.get("page"),
                "pageSize": request.args.get("pageSize"),
                "search": request.args.get("search"),
                "sort": request.args.get("sort"),
                "direction": request.args.get("direction"),
            },
        )
    )


@app.get("/api/objects/<path:table_name>/profile")
def object_profile(table_name: str) -> Response:
    column = (request.args.get("column") or "").strip()
    if not column:
        raise AppError(400, "Column name is required for profiling.")
    return jsonify(get_column_profile(table_name, column))


@app.get("/api/objects/<path:table_name>/export")
def object_export(table_name: str) -> Response:
    fmt = "json" if (request.args.get("format") or "").lower() == "json" else "csv"
    content_type, body = export_rows(
        table_name,
        fmt,
        {
            "pageSize": request.args.get("pageSize"),
            "search": request.args.get("search"),
            "sort": request.args.get("sort"),
            "direction": request.args.get("direction"),
        },
    )
    extension = "json" if fmt == "json" else "csv"
    return Response(
        body,
        mimetype=content_type,
        headers={"Content-Disposition": f'attachment; filename="{table_name}.{extension}"'},
    )


@app.get("/")
def index() -> Response:
    return send_from_directory(PUBLIC_DIR, "index.html")


@app.get("/<path:filename>")
def static_files(filename: str) -> Response:
    target = PUBLIC_DIR / filename
    if not target.exists():
        raise AppError(404, "Not found.")
    return send_from_directory(PUBLIC_DIR, filename)


if __name__ == "__main__":
    app.run(host=HOST, port=PORT, debug=False)
