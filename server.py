import json
import os
import sqlite3
from pathlib import Path
from typing import Annotated

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from pydantic import Field

mcp = FastMCP(
    name="SQLite MCP Server",
    instructions="A database server for SQLite. Use tools to create tables, query data, insert/update/delete rows.",
)

SUPPORTED_TYPES = {"TEXT", "INTEGER", "REAL", "BLOB", "BOOLEAN", "DATETIME"}


def get_connection() -> sqlite3.Connection:
    """Get a SQLite connection with WAL mode and Row factory."""
    db_path = os.environ.get("SQLITE_DB_PATH", "data/database.db")
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


# ── Table Management Tools ──────────────────────────────────────────


@mcp.tool
def create_table(
    table_name: Annotated[str, Field(description="Name of the table to create")],
    columns: Annotated[
        list[dict],
        Field(
            description="List of column definitions. Each dict has 'name', 'type', and optional 'primary_key', 'not_null', 'default'."
        ),
    ],
) -> str:
    """Create a new table in the database with the specified columns."""
    conn = get_connection()
    try:
        # Check if table already exists
        existing = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        if existing:
            raise ToolError(f"Table '{table_name}' already exists.")

        col_defs = []
        for col in columns:
            col_type = col.get("type", "TEXT").upper()
            if col_type not in SUPPORTED_TYPES:
                raise ToolError(
                    f"Unsupported column type: {col_type}. Supported: {', '.join(sorted(SUPPORTED_TYPES))}"
                )
            parts = [col["name"], col_type]
            if col.get("primary_key"):
                parts.append("PRIMARY KEY")
            if col.get("not_null"):
                parts.append("NOT NULL")
            if "default" in col:
                parts.append(f"DEFAULT {col['default']!r}")
            col_defs.append(" ".join(parts))

        sql = f"CREATE TABLE {table_name} ({', '.join(col_defs)})"
        conn.execute(sql)
        conn.commit()
        return f"Table '{table_name}' created successfully.\nSQL: {sql}"
    finally:
        conn.close()


@mcp.tool
def list_tables() -> str:
    """List all tables in the database."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        tables = [row["name"] for row in rows]
        return json.dumps(tables)
    finally:
        conn.close()


@mcp.tool
def describe_table(
    table_name: Annotated[str, Field(description="Name of the table to describe")],
) -> str:
    """Describe the schema of a table, showing column info."""
    conn = get_connection()
    try:
        # Check table exists
        existing = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        if not existing:
            raise ToolError(f"Table '{table_name}' does not exist.")

        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        columns = []
        for row in rows:
            columns.append(
                {
                    "name": row["name"],
                    "type": row["type"],
                    "nullable": not row["notnull"],
                    "default": row["dflt_value"],
                    "primary_key": bool(row["pk"]),
                }
            )
        return json.dumps(columns, indent=2)
    finally:
        conn.close()


@mcp.tool
def drop_table(
    table_name: Annotated[str, Field(description="Name of the table to drop")],
) -> str:
    """Drop a table from the database."""
    conn = get_connection()
    try:
        existing = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        if not existing:
            raise ToolError(f"Table '{table_name}' does not exist.")

        conn.execute(f"DROP TABLE {table_name}")
        conn.commit()
        return f"Table '{table_name}' dropped successfully."
    finally:
        conn.close()


# ── Main ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    transport = os.environ.get("MCP_TRANSPORT", "http").lower()
    if transport == "stdio":
        mcp.run(transport="stdio")
    else:
        mcp.run(transport="http", port=8000)
