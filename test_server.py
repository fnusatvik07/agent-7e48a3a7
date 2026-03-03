"""Functional tests for SQLite MCP Server using FastMCP in-memory Client."""

import json
import os
import sqlite3

import pytest
from fastmcp import Client
from fastmcp.exceptions import ToolError

from server import mcp


@pytest.fixture(autouse=True)
def fresh_db(tmp_path, monkeypatch):
    """Provide a fresh database path for each test."""
    db_path = str(tmp_path / "test.db")
    monkeypatch.setenv("SQLITE_DB_PATH", db_path)
    return db_path


# ── Helper ────────────────────────────────────────────────────────────


async def _create_users_table(client: Client) -> None:
    """Helper to create a standard users table."""
    await client.call_tool(
        "create_table",
        {
            "table_name": "users",
            "columns": [
                {"name": "id", "type": "INTEGER", "primary_key": True},
                {"name": "name", "type": "TEXT", "not_null": True},
                {"name": "email", "type": "TEXT"},
            ],
        },
    )


# ── Table Management Tests (6) ────────────────────────────────────────


@pytest.mark.asyncio
async def test_create_table_and_list():
    """Test 1: create_table → verify table exists via list_tables."""
    async with Client(mcp) as client:
        result = await client.call_tool(
            "create_table",
            {
                "table_name": "products",
                "columns": [
                    {"name": "id", "type": "INTEGER", "primary_key": True},
                    {"name": "name", "type": "TEXT"},
                ],
            },
        )
        assert "products" in result.data
        assert "created successfully" in result.data

        tables_result = await client.call_tool("list_tables", {})
        tables = json.loads(tables_result.data)
        assert "products" in tables


@pytest.mark.asyncio
async def test_create_table_all_column_options():
    """Test 2: create_table with all column options (primary_key, not_null, default)."""
    async with Client(mcp) as client:
        result = await client.call_tool(
            "create_table",
            {
                "table_name": "items",
                "columns": [
                    {"name": "id", "type": "INTEGER", "primary_key": True},
                    {"name": "title", "type": "TEXT", "not_null": True},
                    {"name": "price", "type": "REAL", "not_null": True, "default": 0.0},
                    {"name": "created_at", "type": "DATETIME"},
                    {"name": "is_active", "type": "BOOLEAN", "default": True},
                    {"name": "image", "type": "BLOB"},
                ],
            },
        )
        assert "items" in result.data
        assert "created successfully" in result.data

        # Verify schema via describe_table
        schema_result = await client.call_tool(
            "describe_table", {"table_name": "items"}
        )
        columns = json.loads(schema_result.data)
        col_names = [c["name"] for c in columns]
        assert col_names == ["id", "title", "price", "created_at", "is_active", "image"]

        # Check primary key
        id_col = next(c for c in columns if c["name"] == "id")
        assert id_col["primary_key"] is True

        # Check not_null
        title_col = next(c for c in columns if c["name"] == "title")
        assert title_col["nullable"] is False


@pytest.mark.asyncio
async def test_create_duplicate_table_raises_error():
    """Test 3: ToolError on duplicate table creation."""
    async with Client(mcp) as client:
        await _create_users_table(client)
        with pytest.raises(ToolError, match="already exists"):
            await client.call_tool(
                "create_table",
                {
                    "table_name": "users",
                    "columns": [{"name": "id", "type": "INTEGER"}],
                },
            )


@pytest.mark.asyncio
async def test_describe_table_schema():
    """Test 4: describe_table returns correct schema info."""
    async with Client(mcp) as client:
        await _create_users_table(client)
        result = await client.call_tool(
            "describe_table", {"table_name": "users"}
        )
        columns = json.loads(result.data)
        assert len(columns) == 3
        assert columns[0]["name"] == "id"
        assert columns[0]["type"] == "INTEGER"
        assert columns[0]["primary_key"] is True
        assert columns[1]["name"] == "name"
        assert columns[1]["nullable"] is False
        assert columns[2]["name"] == "email"
        assert columns[2]["nullable"] is True


@pytest.mark.asyncio
async def test_drop_table():
    """Test 5: drop_table removes the table."""
    async with Client(mcp) as client:
        await _create_users_table(client)
        result = await client.call_tool("drop_table", {"table_name": "users"})
        assert "dropped successfully" in result.data

        tables_result = await client.call_tool("list_tables", {})
        tables = json.loads(tables_result.data)
        assert "users" not in tables


@pytest.mark.asyncio
async def test_nonexistent_table_errors():
    """Test 6: ToolError on describe/drop for non-existent table."""
    async with Client(mcp) as client:
        with pytest.raises(ToolError, match="does not exist"):
            await client.call_tool("describe_table", {"table_name": "ghost"})
        with pytest.raises(ToolError, match="does not exist"):
            await client.call_tool("drop_table", {"table_name": "ghost"})


# ── CRUD Tests (8) ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_insert_row_and_query():
    """Test 7: insert_row → verify via query tool."""
    async with Client(mcp) as client:
        await _create_users_table(client)
        result = await client.call_tool(
            "insert_row",
            {"table_name": "users", "data": {"name": "Alice", "email": "alice@test.com"}},
        )
        assert "Inserted row" in result.data

        query_result = await client.call_tool(
            "query", {"sql": "SELECT * FROM users"}
        )
        rows = json.loads(query_result.data)
        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"
        assert rows[0]["email"] == "alice@test.com"


@pytest.mark.asyncio
async def test_insert_rows_batch():
    """Test 8: insert_rows (batch) → verify count matches."""
    async with Client(mcp) as client:
        await _create_users_table(client)
        result = await client.call_tool(
            "insert_rows",
            {
                "table_name": "users",
                "rows": [
                    {"name": "Alice", "email": "alice@test.com"},
                    {"name": "Bob", "email": "bob@test.com"},
                    {"name": "Charlie", "email": "charlie@test.com"},
                ],
            },
        )
        assert "3 rows" in result.data

        query_result = await client.call_tool(
            "query", {"sql": "SELECT COUNT(*) as cnt FROM users"}
        )
        rows = json.loads(query_result.data)
        assert rows[0]["cnt"] == 3


@pytest.mark.asyncio
async def test_query_with_where_and_params():
    """Test 9: query with WHERE clause and bind params."""
    async with Client(mcp) as client:
        await _create_users_table(client)
        await client.call_tool(
            "insert_rows",
            {
                "table_name": "users",
                "rows": [
                    {"name": "Alice", "email": "alice@test.com"},
                    {"name": "Bob", "email": "bob@test.com"},
                ],
            },
        )
        result = await client.call_tool(
            "query",
            {"sql": "SELECT * FROM users WHERE name = ?", "params": ["Bob"]},
        )
        rows = json.loads(result.data)
        assert len(rows) == 1
        assert rows[0]["name"] == "Bob"


@pytest.mark.asyncio
async def test_query_rejects_non_select():
    """Test 10: query rejects non-SELECT statements (INSERT, UPDATE, DELETE, DROP)."""
    async with Client(mcp) as client:
        await _create_users_table(client)
        for stmt in [
            "INSERT INTO users (name) VALUES ('hack')",
            "UPDATE users SET name='hack'",
            "DELETE FROM users",
            "DROP TABLE users",
        ]:
            with pytest.raises(ToolError, match="Only SELECT"):
                await client.call_tool("query", {"sql": stmt})


@pytest.mark.asyncio
async def test_update_rows_and_verify():
    """Test 11: update_rows → verify changed values via query."""
    async with Client(mcp) as client:
        await _create_users_table(client)
        await client.call_tool(
            "insert_row",
            {"table_name": "users", "data": {"name": "Alice", "email": "old@test.com"}},
        )
        result = await client.call_tool(
            "update_rows",
            {
                "table_name": "users",
                "data": {"email": "new@test.com"},
                "where": "name = ?",
                "params": ["Alice"],
            },
        )
        assert "Updated 1 row" in result.data

        query_result = await client.call_tool(
            "query", {"sql": "SELECT email FROM users WHERE name = 'Alice'"}
        )
        rows = json.loads(query_result.data)
        assert rows[0]["email"] == "new@test.com"


@pytest.mark.asyncio
async def test_update_rows_no_where_raises_error():
    """Test 12: update_rows ToolError when no WHERE clause."""
    async with Client(mcp) as client:
        await _create_users_table(client)
        with pytest.raises(ToolError, match="WHERE clause is required"):
            await client.call_tool(
                "update_rows",
                {
                    "table_name": "users",
                    "data": {"name": "hack"},
                    "where": "",
                },
            )


@pytest.mark.asyncio
async def test_delete_rows_and_verify():
    """Test 13: delete_rows → verify row removed via query."""
    async with Client(mcp) as client:
        await _create_users_table(client)
        await client.call_tool(
            "insert_rows",
            {
                "table_name": "users",
                "rows": [
                    {"name": "Alice", "email": "alice@test.com"},
                    {"name": "Bob", "email": "bob@test.com"},
                ],
            },
        )
        result = await client.call_tool(
            "delete_rows",
            {"table_name": "users", "where": "name = ?", "params": ["Alice"]},
        )
        assert "Deleted 1 row" in result.data

        query_result = await client.call_tool(
            "query", {"sql": "SELECT * FROM users"}
        )
        rows = json.loads(query_result.data)
        assert len(rows) == 1
        assert rows[0]["name"] == "Bob"


@pytest.mark.asyncio
async def test_delete_rows_no_where_raises_error():
    """Test 14: delete_rows ToolError when no WHERE clause."""
    async with Client(mcp) as client:
        await _create_users_table(client)
        with pytest.raises(ToolError, match="WHERE clause is required"):
            await client.call_tool(
                "delete_rows",
                {"table_name": "users", "where": ""},
            )


# ── Resource Tests (4) ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_resource_tables_list():
    """Test 15: db://tables returns table list after creating tables."""
    async with Client(mcp) as client:
        await _create_users_table(client)
        await client.call_tool(
            "create_table",
            {
                "table_name": "orders",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
        )
        result = await client.read_resource("db://tables")
        data = json.loads(result[0].text)
        assert "users" in data
        assert "orders" in data


@pytest.mark.asyncio
async def test_resource_table_schema():
    """Test 16: db://tables/{name}/schema returns correct schema JSON."""
    async with Client(mcp) as client:
        await _create_users_table(client)
        result = await client.read_resource("db://tables/users/schema")
        columns = json.loads(result[0].text)
        assert len(columns) == 3
        assert columns[0]["name"] == "id"
        assert columns[0]["type"] == "INTEGER"
        assert columns[0]["primary_key"] is True


@pytest.mark.asyncio
async def test_resource_table_count():
    """Test 17: db://tables/{name}/count returns correct row count."""
    async with Client(mcp) as client:
        await _create_users_table(client)
        await client.call_tool(
            "insert_rows",
            {
                "table_name": "users",
                "rows": [
                    {"name": "Alice", "email": "a@test.com"},
                    {"name": "Bob", "email": "b@test.com"},
                    {"name": "Charlie", "email": "c@test.com"},
                ],
            },
        )
        result = await client.read_resource("db://tables/users/count")
        data = json.loads(result[0].text)
        assert data["table"] == "users"
        assert data["count"] == 3


@pytest.mark.asyncio
async def test_resource_db_stats():
    """Test 18: db://stats returns file size, table count, total rows."""
    async with Client(mcp) as client:
        await _create_users_table(client)
        await client.call_tool(
            "insert_rows",
            {
                "table_name": "users",
                "rows": [
                    {"name": "Alice", "email": "a@test.com"},
                    {"name": "Bob", "email": "b@test.com"},
                ],
            },
        )
        result = await client.read_resource("db://stats")
        data = json.loads(result[0].text)
        assert data["file_size_bytes"] > 0
        assert data["table_count"] == 1
        assert data["total_rows"] == 2


# ── Edge Case Tests (4) ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_query_1000_row_limit():
    """Test 19: query result limit — insert 1001+ rows, verify max 1000 returned."""
    async with Client(mcp) as client:
        await client.call_tool(
            "create_table",
            {
                "table_name": "numbers",
                "columns": [{"name": "val", "type": "INTEGER"}],
            },
        )
        # Insert 1005 rows in batches
        batch_size = 500
        total = 1005
        for start in range(0, total, batch_size):
            end = min(start + batch_size, total)
            rows = [{"val": i} for i in range(start, end)]
            await client.call_tool(
                "insert_rows", {"table_name": "numbers", "rows": rows}
            )

        result = await client.call_tool(
            "query", {"sql": "SELECT * FROM numbers"}
        )
        rows = json.loads(result.data)
        assert len(rows) == 1000


@pytest.mark.asyncio
async def test_wal_mode_enabled(fresh_db):
    """Test 20: WAL mode is enabled on the database."""
    async with Client(mcp) as client:
        # Create a table to initialize the database
        await _create_users_table(client)

    # Open a direct connection to verify WAL mode
    conn = sqlite3.connect(fresh_db)
    try:
        result = conn.execute("PRAGMA journal_mode").fetchone()
        assert result[0] == "wal"
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_empty_table_returns_empty_list():
    """Test 21: empty table query returns empty list."""
    async with Client(mcp) as client:
        await _create_users_table(client)
        result = await client.call_tool(
            "query", {"sql": "SELECT * FROM users"}
        )
        rows = json.loads(result.data)
        assert rows == []


@pytest.mark.asyncio
async def test_insert_missing_required_column_raises_error():
    """Test 22: insert with missing required column raises ToolError."""
    async with Client(mcp) as client:
        await client.call_tool(
            "create_table",
            {
                "table_name": "strict_table",
                "columns": [
                    {"name": "id", "type": "INTEGER", "primary_key": True},
                    {"name": "required_field", "type": "TEXT", "not_null": True},
                ],
            },
        )
        with pytest.raises(ToolError, match="Constraint violation"):
            await client.call_tool(
                "insert_row",
                {"table_name": "strict_table", "data": {"id": 1}},
            )
