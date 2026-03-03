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


# ── CRUD Tools ──────────────────────────────────────────────────────


@mcp.tool
def insert_row(
    table_name: Annotated[str, Field(description="Name of the table")],
    data: Annotated[dict, Field(description="Column-value mapping for the row")],
) -> str:
    """Insert a single row into a table."""
    conn = get_connection()
    try:
        # Check table exists
        existing = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        if not existing:
            raise ToolError(f"Table '{table_name}' does not exist.")

        columns = ", ".join(data.keys())
        placeholders = ", ".join("?" for _ in data)
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        try:
            cursor = conn.execute(sql, list(data.values()))
            conn.commit()
            return f"Inserted row with ID {cursor.lastrowid} into '{table_name}'."
        except sqlite3.IntegrityError as e:
            raise ToolError(f"Constraint violation: {e}")
    finally:
        conn.close()


@mcp.tool
def insert_rows(
    table_name: Annotated[str, Field(description="Name of the table")],
    rows: Annotated[list[dict], Field(description="List of column-value mappings")],
) -> str:
    """Insert multiple rows into a table using batch insert."""
    conn = get_connection()
    try:
        # Check table exists
        existing = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        if not existing:
            raise ToolError(f"Table '{table_name}' does not exist.")

        if not rows:
            return "No rows to insert."

        columns = ", ".join(rows[0].keys())
        placeholders = ", ".join("?" for _ in rows[0])
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        try:
            conn.executemany(sql, [list(r.values()) for r in rows])
            conn.commit()
            return f"Inserted {len(rows)} rows into '{table_name}'."
        except sqlite3.IntegrityError as e:
            raise ToolError(f"Constraint violation: {e}")
    finally:
        conn.close()


@mcp.tool
def query(
    sql: Annotated[str, Field(description="SELECT query to execute")],
    params: Annotated[list | None, Field(description="Bind parameters for the query")] = None,
) -> str:
    """Execute a SELECT query and return results as a list of dicts."""
    # Only allow SELECT statements
    stripped = sql.strip().upper()
    if not stripped.startswith("SELECT"):
        raise ToolError("Only SELECT queries are allowed. Use insert_row, update_rows, or delete_rows for mutations.")

    conn = get_connection()
    try:
        cursor = conn.execute(sql, params or [])
        rows = cursor.fetchmany(1000)
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in rows]
        return json.dumps(results)
    finally:
        conn.close()


@mcp.tool
def update_rows(
    table_name: Annotated[str, Field(description="Name of the table")],
    data: Annotated[dict, Field(description="Column-value mapping of fields to update")],
    where: Annotated[str, Field(description="WHERE clause (without the WHERE keyword)")],
    params: Annotated[list | None, Field(description="Bind parameters for the WHERE clause")] = None,
) -> str:
    """Update rows in a table matching the WHERE clause."""
    if not where or not where.strip():
        raise ToolError("WHERE clause is required for safety. To update all rows, use 'WHERE 1=1'.")

    conn = get_connection()
    try:
        set_clause = ", ".join(f"{col} = ?" for col in data.keys())
        sql = f"UPDATE {table_name} SET {set_clause} WHERE {where}"
        all_params = list(data.values()) + (params or [])
        cursor = conn.execute(sql, all_params)
        conn.commit()
        return f"Updated {cursor.rowcount} row(s) in '{table_name}'."
    finally:
        conn.close()


@mcp.tool
def delete_rows(
    table_name: Annotated[str, Field(description="Name of the table")],
    where: Annotated[str, Field(description="WHERE clause (without the WHERE keyword)")],
    params: Annotated[list | None, Field(description="Bind parameters for the WHERE clause")] = None,
) -> str:
    """Delete rows from a table matching the WHERE clause."""
    if not where or not where.strip():
        raise ToolError("WHERE clause is required for safety. To delete all rows, use 'WHERE 1=1'.")

    conn = get_connection()
    try:
        sql = f"DELETE FROM {table_name} WHERE {where}"
        cursor = conn.execute(sql, params or [])
        conn.commit()
        return f"Deleted {cursor.rowcount} row(s) from '{table_name}'."
    finally:
        conn.close()


# ── Resources ──────────────────────────────────────────────────────


@mcp.resource("db://tables")
def list_tables_resource() -> str:
    """List all tables in the database as a JSON array."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        tables = [row["name"] for row in rows]
        return json.dumps(tables)
    finally:
        conn.close()


@mcp.resource("db://tables/{table_name}/schema")
def table_schema_resource(table_name: str) -> str:
    """Return the schema for a specific table as JSON."""
    conn = get_connection()
    try:
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


@mcp.resource("db://tables/{table_name}/count")
def table_count_resource(table_name: str) -> str:
    """Return the row count for a specific table."""
    conn = get_connection()
    try:
        row = conn.execute(f"SELECT COUNT(*) as cnt FROM {table_name}").fetchone()
        return json.dumps({"table": table_name, "count": row["cnt"]})
    finally:
        conn.close()


@mcp.resource("db://stats")
def db_stats_resource() -> str:
    """Return database statistics: file size, table count, total rows."""
    conn = get_connection()
    try:
        db_path = os.environ.get("SQLITE_DB_PATH", "data/database.db")
        file_size = os.path.getsize(db_path) if os.path.exists(db_path) else 0

        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        table_count = len(tables)

        total_rows = 0
        for table in tables:
            row = conn.execute(
                f"SELECT COUNT(*) as cnt FROM {table['name']}"
            ).fetchone()
            total_rows += row["cnt"]

        return json.dumps(
            {
                "file_size_bytes": file_size,
                "table_count": table_count,
                "total_rows": total_rows,
            }
        )
    finally:
        conn.close()


# ── Main ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    transport = os.environ.get("MCP_TRANSPORT", "http").lower()
    if transport == "stdio":
        mcp.run(transport="stdio")
    else:
        mcp.run(transport="http", port=8000)
