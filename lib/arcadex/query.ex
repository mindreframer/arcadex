defmodule Arcadex.Query do
  @moduledoc """
  Query and command functions for ArcadeDB.

  Provides functions to execute read queries (SELECT) and write commands
  (INSERT/UPDATE/DELETE/DDL) against ArcadeDB.
  """

  alias Arcadex.{Conn, Client, Error}

  @doc """
  Execute a read query (SELECT).

  Returns `{:ok, results}` or `{:error, %Arcadex.Error{}}`.

  ## Parameters

    * `conn` - Connection context
    * `sql` - SQL query string
    * `params` - Optional map of parameters (default: empty map)

  ## Examples

      iex> Arcadex.Query.query(conn, "SELECT FROM User WHERE active = true")
      {:ok, [%{"@rid" => "#1:0", "name" => "John", "active" => true}]}

      iex> Arcadex.Query.query(conn, "SELECT FROM User WHERE age > :age", %{age: 21})
      {:ok, [%{"@rid" => "#1:0", "name" => "John", "age" => 25}]}

  """
  @spec query(Conn.t(), String.t(), map()) :: {:ok, list()} | {:error, Error.t()}
  def query(%Conn{} = conn, sql, params \\ %{}) do
    body = %{language: "sql", command: sql}
    body = if map_size(params) > 0, do: Map.put(body, :params, params), else: body

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
  @spec query!(Conn.t(), String.t(), map()) :: list()
  def query!(%Conn{} = conn, sql, params \\ %{}) do
    case query(conn, sql, params) do
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

  ## Examples

      iex> Arcadex.Query.command(conn, "INSERT INTO User SET name = 'John'")
      {:ok, [%{"@rid" => "#1:0", "name" => "John"}]}

      iex> Arcadex.Query.command(conn, "CREATE VERTEX TYPE Person")
      {:ok, []}

      iex> Arcadex.Query.command(conn, "INSERT INTO User SET name = :name", %{name: "Jane"})
      {:ok, [%{"@rid" => "#1:1", "name" => "Jane"}]}

  """
  @spec command(Conn.t(), String.t(), map()) :: {:ok, list()} | {:error, Error.t()}
  def command(%Conn{} = conn, sql, params \\ %{}) do
    body = %{language: "sql", command: sql}
    body = if map_size(params) > 0, do: Map.put(body, :params, params), else: body

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
  @spec command!(Conn.t(), String.t(), map()) :: list()
  def command!(%Conn{} = conn, sql, params \\ %{}) do
    case command(conn, sql, params) do
      {:ok, result} -> result
      {:error, error} -> raise error
    end
  end
end
