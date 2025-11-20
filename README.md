# Arcadex

A lean Elixir wrapper for ArcadeDB's REST API with connection pooling, transactions, and database switching.

## Features

- Connection pooling via Finch
- Transaction support with auto-commit/rollback
- Database switching on existing connections
- Parameter binding for safe queries
- Bang variants that raise on error

## Installation

Add `arcadex` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:arcadex, "~> 0.1.0"}
  ]
end
```

The application automatically starts a Finch connection pool named `Arcadex.Finch`.

## Quick Start

### Create a Connection

```elixir
conn = Arcadex.connect("http://localhost:2480", "mydb",
  auth: {"root", "password"}
)
```

### Query Data

```elixir
# SELECT query
{:ok, users} = Arcadex.query(conn, "SELECT FROM User WHERE active = true")

# Query with parameters
{:ok, users} = Arcadex.query(conn, "SELECT FROM User WHERE age > :age", %{age: 21})

# Bang version (raises on error)
users = Arcadex.query!(conn, "SELECT FROM User")
```

### Execute Commands

```elixir
# INSERT command
{:ok, [user]} = Arcadex.command(conn, "INSERT INTO User SET name = 'John', email = 'john@example.com'")

# Command with parameters
{:ok, [user]} = Arcadex.command(conn,
  "INSERT INTO User SET name = :name, email = :email",
  %{name: "Jane", email: "jane@example.com"}
)

# DDL commands
{:ok, _} = Arcadex.command(conn, "CREATE VERTEX TYPE Person")

# Bang version
user = Arcadex.command!(conn, "INSERT INTO User SET name = 'Bob'")
```

### Transactions

```elixir
{:ok, result} = Arcadex.transaction(conn, fn tx ->
  # tx is a connection with session_id
  user = Arcadex.command!(tx, "INSERT INTO User SET name = 'John'")

  Arcadex.command!(tx, "INSERT INTO Log SET action = 'created', user = :rid",
    %{rid: user["@rid"]}
  )

  user
end)
```

Transactions automatically:
- Commit on successful completion
- Rollback on exception or error

### Database Management

```elixir
# Create a database
Arcadex.create_database!(conn, "newdb")

# Check if database exists
if Arcadex.database_exists?(conn, "mydb") do
  IO.puts("Database exists!")
end

# Switch to a different database
conn2 = Arcadex.with_database(conn, "newdb")

# Drop a database
Arcadex.drop_database!(conn, "olddb")
```

## Error Handling

All functions return `{:ok, result}` or `{:error, %Arcadex.Error{}}`.

```elixir
case Arcadex.query(conn, sql) do
  {:ok, results} ->
    process(results)
  {:error, %Arcadex.Error{message: msg, detail: detail}} ->
    Logger.error("Query failed: #{msg} - #{detail}")
end
```

Bang variants raise `Arcadex.Error`:

```elixir
try do
  Arcadex.query!(conn, "INVALID SQL")
rescue
  e in Arcadex.Error ->
    Logger.error("Error: #{e.message}")
end
```

## Connection Options

| Option | Default | Description |
|--------|---------|-------------|
| `:auth` | `{"root", "root"}` | Authentication tuple `{username, password}` |
| `:finch` | `Arcadex.Finch` | Finch pool name for connection pooling |

## Custom Finch Pool

For advanced connection pooling configuration:

```elixir
# In your application.ex
children = [
  {Finch,
    name: MyApp.ArcadeFinch,
    pools: %{
      "http://localhost:2480" => [size: 10, count: 1]
    }
  }
]

# Use custom pool
conn = Arcadex.connect("http://localhost:2480", "mydb",
  auth: {"root", "password"},
  finch: MyApp.ArcadeFinch
)
```

## ArcadeDB REST API Reference

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/v1/query/{db}` | POST | Read queries (SELECT) |
| `/api/v1/command/{db}` | POST | Write commands (INSERT/UPDATE/DELETE/DDL) |
| `/api/v1/begin/{db}` | POST | Begin transaction |
| `/api/v1/commit/{db}` | POST | Commit transaction |
| `/api/v1/rollback/{db}` | POST | Rollback transaction |
| `/api/v1/server` | POST | Server commands (create/drop database) |
| `/api/v1/exists/{db}` | GET | Check database exists |

## License

MIT
