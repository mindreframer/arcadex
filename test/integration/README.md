# Integration Tests

This directory contains integration tests that run against a real ArcadeDB server.

## Prerequisites

- Running ArcadeDB server (default: `http://localhost:2480`)
- Default credentials: `root` / `playwithdata`

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ARCADEDB_URL` | `http://localhost:2480` | ArcadeDB server URL |
| `ARCADEDB_USER` | `root` | Username for authentication |
| `ARCADEDB_PASSWORD` | `playwithdata` | Password for authentication |

## Test Tags

| Tag | Description |
|-----|-------------|
| `@moduletag :integration` | Applied to all integration tests |
| `@tag :fresh_db` | Test gets its own isolated database |
| `async: false` | Test must run sequentially (server-level operations) |

## Running Tests

```bash
# Run all tests (unit + integration)
mix test

# Run only integration tests
mix test --only integration

# Run unit tests only (no server needed)
mix test --exclude integration

# Run specific integration test file
mix test test/integration/crud_test.exs

# Run sequential/server tests only
mix test test/integration/server_test.exs
```

## Test Organization

```
test/integration/
├── support/
│   └── integration_case.ex   # Base test case module
├── crud_test.exs             # Basic CRUD operations
├── transaction_test.exs      # Transaction commit/rollback
├── script_test.exs           # SQL scripts with LET/RETURN
├── types_test.exs            # Document/Vertex/Edge types
├── indexes_test.exs          # Index operations
├── links_test.exs            # LINK property tests
├── projections_test.exs      # Nested projections
├── server_test.exs           # Server-level operations
├── infrastructure_test.exs   # Base infrastructure tests
└── sql/
    ├── select_test.exs       # SELECT with ORDER BY, GROUP BY, etc.
    ├── insert_test.exs       # INSERT operations
    ├── update_test.exs       # UPDATE operations
    ├── delete_test.exs       # DELETE operations
    └── functions_test.exs    # SQL functions (string, date, math)
```

## Database Isolation

### Module-level Database (Default)

Tests in the same module share a database. Each test uses unique IDs to avoid conflicts.

```elixir
defmodule ArcadeDB.Integration.CrudTest do
  use ArcadeDB.IntegrationCase, async: true  # Shared DB for all tests

  test "my test", %{conn: conn} do
    uid = generate_uid()  # Unique per test
    # ...
  end
end
```

### Test-level Database

Use `@tag :fresh_db` for tests that need complete isolation:

```elixir
@tag :fresh_db
test "destructive operation", %{conn: conn} do
  # Gets its own database, cleaned up after
end
```

### Sequential Execution

Use `async: false` for server-level operations:

```elixir
defmodule ArcadeDB.Integration.ServerTest do
  use ArcadeDB.IntegrationCase, async: false

  test "create database", %{conn: conn} do
    # Runs alone without other tests
  end
end
```

## Automatic Cleanup

All test databases are automatically dropped after tests complete via `on_exit` callbacks.

## Writing New Tests

1. Use `ArcadeDB.IntegrationCase` as the base
2. Set up types in `setup_all` for module-level fixtures
3. Use `generate_uid()` for unique test data
4. Use `@tag :fresh_db` for destructive operations
5. Use `async: false` for server-level operations

Example:

```elixir
defmodule ArcadeDB.Integration.MyTest do
  use ArcadeDB.IntegrationCase, async: true

  setup_all %{conn: conn} do
    ArcadeDB.command!(conn, "CREATE DOCUMENT TYPE MyType")
    :ok
  end

  test "my operation", %{conn: conn} do
    uid = generate_uid()
    [result] = ArcadeDB.command!(conn,
      "INSERT INTO MyType SET uid = :uid",
      %{uid: uid}
    )
    assert result["uid"] == uid
  end
end
```
