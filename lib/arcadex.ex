defmodule Arcadex do
  @moduledoc """
  ArcadeDB Elixir Client.

  A lean Elixir wrapper for ArcadeDB's REST API with connection pooling,
  transactions, and database switching.

  ## Quick Start

      # Create connection
      conn = Arcadex.connect("http://localhost:2480", "mydb",
        auth: {"root", "password"}
      )

      # Query
      {:ok, users} = Arcadex.query(conn, "SELECT FROM User WHERE active = true")

      # Command with params
      {:ok, [user]} = Arcadex.command(conn,
        "INSERT INTO User SET name = :name, email = :email",
        %{name: "John", email: "john@example.com"}
      )

      # Transaction
      {:ok, result} = Arcadex.transaction(conn, fn tx ->
        user = Arcadex.command!(tx, "INSERT INTO User SET name = 'Jane'")
        Arcadex.command!(tx, "INSERT INTO Log SET action = 'created', user = :rid",
          %{rid: user["@rid"]}
        )
        user
      end)

      # Database management
      Arcadex.create_database!(conn, "newdb")
      conn2 = Arcadex.with_database(conn, "newdb")
      Arcadex.drop_database!(conn, "newdb")

  """

  alias Arcadex.{Conn, Query, Transaction, Server}

  # Connection

  @doc """
  Create a new connection context.

  ## Options

    * `:auth` - Tuple of `{username, password}`. Defaults to `{"root", "root"}`.
    * `:finch` - Finch pool name. Defaults to `Arcadex.Finch`.

  ## Examples

      iex> conn = Arcadex.connect("http://localhost:2480", "mydb")
      iex> conn.database
      "mydb"

      iex> conn = Arcadex.connect("http://localhost:2480", "mydb", auth: {"admin", "pass"})
      iex> conn.auth
      {"admin", "pass"}

  """
  @spec connect(String.t(), String.t(), keyword()) :: Conn.t()
  defdelegate connect(base_url, database, opts \\ []), to: Conn, as: :new

  @doc """
  Return new conn with different database (same pool).

  Clears any existing session_id since sessions are database-specific.

  ## Examples

      iex> conn = Arcadex.connect("http://localhost:2480", "db1")
      iex> conn2 = Arcadex.with_database(conn, "db2")
      iex> conn2.database
      "db2"

  """
  @spec with_database(Conn.t(), String.t()) :: Conn.t()
  defdelegate with_database(conn, database), to: Conn

  # Query/Command

  @doc """
  Execute a read query (SELECT).

  Returns `{:ok, results}` or `{:error, %Arcadex.Error{}}`.

  ## Options

    * `:limit` - Maximum number of results to return
    * `:serializer` - Result format: "record", "graph", or "studio"

  ## Examples

      Arcadex.query(conn, "SELECT FROM User WHERE active = true")
      {:ok, [%{"@rid" => "#1:0", "name" => "John", "active" => true}]}

      Arcadex.query(conn, "SELECT FROM User WHERE age > :age", %{age: 21})
      {:ok, [%{"@rid" => "#1:0", "name" => "John", "age" => 25}]}

      Arcadex.query(conn, "SELECT FROM User", %{}, limit: 100)
      {:ok, [...]}

  """
  @spec query(Conn.t(), String.t(), map(), keyword()) ::
          {:ok, list()} | {:error, Arcadex.Error.t()}
  defdelegate query(conn, sql, params \\ %{}, opts \\ []), to: Query

  @doc """
  Execute a read query. Raises on error.

  Returns the result list directly or raises `Arcadex.Error`.

  ## Examples

      Arcadex.query!(conn, "SELECT FROM User")
      [%{"@rid" => "#1:0", "name" => "John"}]

  """
  @spec query!(Conn.t(), String.t(), map(), keyword()) :: list()
  defdelegate query!(conn, sql, params \\ %{}, opts \\ []), to: Query

  @doc """
  Execute a write command (INSERT/UPDATE/DELETE/DDL).

  Returns `{:ok, results}` or `{:error, %Arcadex.Error{}}`.

  ## Options

    * `:limit` - Maximum number of results to return
    * `:retries` - Number of retry attempts for transient failures
    * `:serializer` - Result format: "record", "graph", or "studio"

  ## Examples

      Arcadex.command(conn, "INSERT INTO User SET name = 'John'")
      {:ok, [%{"@rid" => "#1:0", "name" => "John"}]}

      Arcadex.command(conn, "INSERT INTO User SET name = :name", %{name: "Jane"})
      {:ok, [%{"@rid" => "#1:1", "name" => "Jane"}]}

      Arcadex.command(conn, "INSERT INTO User SET name = 'John'", %{}, retries: 3)
      {:ok, [%{"@rid" => "#1:0", "name" => "John"}]}

  """
  @spec command(Conn.t(), String.t(), map(), keyword()) ::
          {:ok, list()} | {:error, Arcadex.Error.t()}
  defdelegate command(conn, sql, params \\ %{}, opts \\ []), to: Query

  @doc """
  Execute a write command. Raises on error.

  Returns the result list directly or raises `Arcadex.Error`.

  ## Examples

      Arcadex.command!(conn, "INSERT INTO User SET name = 'John'")
      [%{"@rid" => "#1:0", "name" => "John"}]

  """
  @spec command!(Conn.t(), String.t(), map(), keyword()) :: list()
  defdelegate command!(conn, sql, params \\ %{}, opts \\ []), to: Query

  # Script

  @doc """
  Execute SQL script (multiple statements with LET/RETURN).

  Uses 'sqlscript' language to execute multiple SQL statements
  with variable assignment and return values.

  Returns `{:ok, results}` or `{:error, %Arcadex.Error{}}`.

  ## Options

    * `:limit` - Maximum number of results to return
    * `:retries` - Number of retry attempts for transient failures
    * `:serializer` - Result format: "record", "graph", or "studio"

  ## Examples

      {:ok, result} = Arcadex.script(conn, \"""
        LET user = SELECT FROM User WHERE name = :name;
        LET orders = SELECT FROM Order WHERE user = $user[0].@rid;
        RETURN { user: $user, orders: $orders }
      \""", %{name: "John"})

  """
  @spec script(Conn.t(), String.t(), map(), keyword()) ::
          {:ok, list()} | {:error, Arcadex.Error.t()}
  defdelegate script(conn, script, params \\ %{}, opts \\ []), to: Query

  @doc """
  Execute SQL script. Raises on error.

  Returns the result list directly or raises `Arcadex.Error`.

  ## Examples

      Arcadex.script!(conn, "LET x = SELECT 1; RETURN $x")
      [1]

  """
  @spec script!(Conn.t(), String.t(), map(), keyword()) :: list()
  defdelegate script!(conn, script, params \\ %{}, opts \\ []), to: Query

  # Execute (multi-language)

  @doc """
  Execute command with explicit language.

  Supports multiple query languages: sql, sqlscript, cypher, gremlin, graphql, mongo.
  Returns `{:ok, results}` or `{:error, %Arcadex.Error{}}`.

  ## Options

    * `:limit` - Maximum number of results to return
    * `:retries` - Number of retry attempts for transient failures
    * `:serializer` - Result format: "record", "graph", or "studio"

  ## Examples

      {:ok, users} = Arcadex.execute(conn, "cypher",
        "MATCH (n:User) RETURN n LIMIT 10"
      )

      {:ok, users} = Arcadex.execute(conn, "gremlin",
        "g.V().hasLabel('User').limit(10)"
      )

      {:ok, result} = Arcadex.execute(conn, "graphql", \"""
        {
          users(limit: 10) {
            name
            email
          }
        }
      \""")

  """
  @spec execute(Conn.t(), String.t(), String.t(), map(), keyword()) ::
          {:ok, list()} | {:error, Arcadex.Error.t()}
  defdelegate execute(conn, language, command, params \\ %{}, opts \\ []), to: Query

  @doc """
  Execute command with explicit language. Raises on error.

  Returns the result list directly or raises `Arcadex.Error`.

  ## Examples

      Arcadex.execute!(conn, "cypher", "MATCH (n:User) RETURN n")
      [%{"n" => %{"name" => "John"}}]

  """
  @spec execute!(Conn.t(), String.t(), String.t(), map(), keyword()) :: list()
  defdelegate execute!(conn, language, command, params \\ %{}, opts \\ []), to: Query

  # Async

  @doc """
  Execute command asynchronously (fire and forget).

  Returns `:ok` immediately without waiting for the command to complete.
  The result is logged on the server side.

  Returns `:ok` on success or `{:error, %Arcadex.Error{}}` on failure.

  ## Options

    * `:limit` - Maximum number of results to return
    * `:retries` - Number of retry attempts for transient failures
    * `:serializer` - Result format: "record", "graph", or "studio"

  ## Examples

      :ok = Arcadex.command_async(conn, "INSERT INTO Log SET event = 'audit'")

      :ok = Arcadex.command_async(conn, "INSERT INTO Log SET event = :event", %{event: "login"})

  """
  @spec command_async(Conn.t(), String.t(), map(), keyword()) :: :ok | {:error, Arcadex.Error.t()}
  defdelegate command_async(conn, sql, params \\ %{}, opts \\ []), to: Query

  # Transactions

  @doc """
  Execute function within a transaction.

  Auto-commits on success, rolls back on error.
  Returns `{:ok, result}` or `{:error, %Arcadex.Error{}}`.

  ## Examples

      {:ok, user} = Arcadex.transaction(conn, fn tx ->
        user = Arcadex.command!(tx, "INSERT INTO User SET name = 'John'")
        Arcadex.command!(tx, "INSERT INTO Log SET user = :rid", %{rid: user["@rid"]})
        user
      end)

  """
  @spec transaction(Conn.t(), (Conn.t() -> any())) :: {:ok, any()} | {:error, Arcadex.Error.t()}
  defdelegate transaction(conn, fun), to: Transaction

  # Server management

  @doc """
  Create a new database.

  Returns `:ok` on success or `{:error, %Arcadex.Error{}}` on failure.

  ## Examples

      Arcadex.create_database(conn, "newdb")
      :ok

  """
  @spec create_database(Conn.t(), String.t()) :: :ok | {:error, Arcadex.Error.t()}
  defdelegate create_database(conn, name), to: Server

  @doc """
  Create a new database. Raises on error.

  ## Examples

      Arcadex.create_database!(conn, "newdb")
      :ok

  """
  @spec create_database!(Conn.t(), String.t()) :: :ok
  defdelegate create_database!(conn, name), to: Server

  @doc """
  Drop a database.

  Returns `:ok` on success or `{:error, %Arcadex.Error{}}` on failure.

  ## Examples

      Arcadex.drop_database(conn, "olddb")
      :ok

  """
  @spec drop_database(Conn.t(), String.t()) :: :ok | {:error, Arcadex.Error.t()}
  defdelegate drop_database(conn, name), to: Server

  @doc """
  Drop a database. Raises on error.

  ## Examples

      Arcadex.drop_database!(conn, "olddb")
      :ok

  """
  @spec drop_database!(Conn.t(), String.t()) :: :ok
  defdelegate drop_database!(conn, name), to: Server

  @doc """
  Check if database exists.

  Returns `true` if the database exists, `false` otherwise.

  ## Examples

      Arcadex.database_exists?(conn, "mydb")
      true

  """
  @spec database_exists?(Conn.t(), String.t()) :: boolean()
  defdelegate database_exists?(conn, name), to: Server
end
