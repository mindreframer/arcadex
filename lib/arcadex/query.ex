defmodule Arcadex.Query do
  @moduledoc """
  Query and command functions for ArcadeDB.

  Provides functions to execute read queries (SELECT) and write commands
  (INSERT/UPDATE/DELETE/DDL) against ArcadeDB.

  ## Options

  The following options can be passed to query/4 and command/4:

    * `:limit` - Maximum number of results to return
    * `:retries` - Number of retry attempts for transient failures
    * `:serializer` - Result format: "record", "graph", or "studio"
    * `:await_response` - If false, command returns immediately without waiting

  """

  alias Arcadex.{Conn, Client, Error}

  @type execute_opts :: [
          limit: pos_integer(),
          retries: pos_integer(),
          serializer: String.t(),
          await_response: boolean()
        ]

  @doc """
  Execute a read query (SELECT).

  Returns `{:ok, results}` or `{:error, %Arcadex.Error{}}`.

  ## Parameters

    * `conn` - Connection context
    * `sql` - SQL query string
    * `params` - Optional map of parameters (default: empty map)
    * `opts` - Optional keyword list of options

  ## Options

    * `:limit` - Maximum number of results to return
    * `:serializer` - Result format: "record", "graph", or "studio"

  ## Examples

      iex> Arcadex.Query.query(conn, "SELECT FROM User WHERE active = true")
      {:ok, [%{"@rid" => "#1:0", "name" => "John", "active" => true}]}

      iex> Arcadex.Query.query(conn, "SELECT FROM User WHERE age > :age", %{age: 21})
      {:ok, [%{"@rid" => "#1:0", "name" => "John", "age" => 25}]}

      iex> Arcadex.Query.query(conn, "SELECT FROM User", %{}, limit: 100)
      {:ok, [...]}

      iex> Arcadex.Query.query(conn, "SELECT FROM User", %{}, serializer: "graph")
      {:ok, [...]}

  """
  @spec query(Conn.t(), String.t(), map(), execute_opts()) :: {:ok, list()} | {:error, Error.t()}
  def query(%Conn{} = conn, sql, params \\ %{}, opts \\ []) do
    body = build_body("sql", sql, params, opts)

    case Client.post(conn, "/api/v1/query/#{conn.database}", body) do
      {:ok, %{"result" => result}} -> {:ok, result}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Execute a read query. Raises on error.

  Returns the result list directly or raises `Arcadex.Error`.

  ## Examples

      iex> Arcadex.Query.query!(conn, "SELECT FROM User")
      [%{"@rid" => "#1:0", "name" => "John"}]

      iex> Arcadex.Query.query!(conn, "INVALID SQL")
      ** (Arcadex.Error) Syntax error

  """
  @spec query!(Conn.t(), String.t(), map(), execute_opts()) :: list()
  def query!(%Conn{} = conn, sql, params \\ %{}, opts \\ []) do
    case query(conn, sql, params, opts) do
      {:ok, result} -> result
      {:error, error} -> raise error
    end
  end

  @doc """
  Execute a write command (INSERT/UPDATE/DELETE/DDL).

  Returns `{:ok, results}` or `{:error, %Arcadex.Error{}}`.

  ## Parameters

    * `conn` - Connection context
    * `sql` - SQL command string
    * `params` - Optional map of parameters (default: empty map)
    * `opts` - Optional keyword list of options

  ## Options

    * `:limit` - Maximum number of results to return
    * `:retries` - Number of retry attempts for transient failures
    * `:serializer` - Result format: "record", "graph", or "studio"

  ## Examples

      iex> Arcadex.Query.command(conn, "INSERT INTO User SET name = 'John'")
      {:ok, [%{"@rid" => "#1:0", "name" => "John"}]}

      iex> Arcadex.Query.command(conn, "CREATE VERTEX TYPE Person")
      {:ok, []}

      iex> Arcadex.Query.command(conn, "INSERT INTO User SET name = :name", %{name: "Jane"})
      {:ok, [%{"@rid" => "#1:1", "name" => "Jane"}]}

      iex> Arcadex.Query.command(conn, "INSERT INTO User SET name = 'John'", %{}, retries: 3)
      {:ok, [%{"@rid" => "#1:0", "name" => "John"}]}

  """
  @spec command(Conn.t(), String.t(), map(), execute_opts()) ::
          {:ok, list()} | {:error, Error.t()}
  def command(%Conn{} = conn, sql, params \\ %{}, opts \\ []) do
    body = build_body("sql", sql, params, opts)

    case Client.post(conn, "/api/v1/command/#{conn.database}", body) do
      {:ok, %{"result" => result}} -> {:ok, result}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Execute a write command. Raises on error.

  Returns the result list directly or raises `Arcadex.Error`.

  ## Examples

      iex> Arcadex.Query.command!(conn, "INSERT INTO User SET name = 'John'")
      [%{"@rid" => "#1:0", "name" => "John"}]

      iex> Arcadex.Query.command!(conn, "INVALID COMMAND")
      ** (Arcadex.Error) Syntax error

  """
  @spec command!(Conn.t(), String.t(), map(), execute_opts()) :: list()
  def command!(%Conn{} = conn, sql, params \\ %{}, opts \\ []) do
    case command(conn, sql, params, opts) do
      {:ok, result} -> result
      {:error, error} -> raise error
    end
  end

  # Private Functions

  @doc false
  @spec build_body(String.t(), String.t(), map(), execute_opts()) :: map()
  def build_body(language, command, params, opts) do
    body = %{language: language, command: command}

    body = if map_size(params) > 0, do: Map.put(body, :params, params), else: body
    body = if opts[:limit], do: Map.put(body, :limit, opts[:limit]), else: body
    body = if opts[:retries], do: Map.put(body, :retries, opts[:retries]), else: body
    body = if opts[:serializer], do: Map.put(body, :serializer, opts[:serializer]), else: body

    if opts[:await_response] == false do
      Map.put(body, :awaitResponse, false)
    else
      body
    end
  end
end
