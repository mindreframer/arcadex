defmodule Arcadex.Server do
  @moduledoc """
  Server management operations for ArcadeDB.

  Provides functions to create, drop, and check database existence.
  These operations use the `/api/v1/server` and `/api/v1/exists` endpoints.
  """

  alias Arcadex.{Client, Conn, Error}

  @doc """
  Create a new database.

  Returns `:ok` on success or `{:error, %Arcadex.Error{}}` on failure.

  ## Examples

      iex> Arcadex.Server.create_database(conn, "newdb")
      :ok

      iex> Arcadex.Server.create_database(conn, "existing")
      {:error, %Arcadex.Error{message: "Database 'existing' already exists"}}

  """
  @spec create_database(Conn.t(), String.t()) :: :ok | {:error, Error.t()}
  def create_database(%Conn{} = conn, name) do
    case Client.post(conn, "/api/v1/server", %{command: "create database #{name}"}) do
      {:ok, %{"result" => "ok"}} -> :ok
      {:ok, _} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Create a new database. Raises on error.

  Returns `:ok` on success or raises `Arcadex.Error`.

  ## Examples

      iex> Arcadex.Server.create_database!(conn, "newdb")
      :ok

  """
  @spec create_database!(Conn.t(), String.t()) :: :ok
  def create_database!(%Conn{} = conn, name) do
    case create_database(conn, name) do
      :ok -> :ok
      {:error, error} -> raise error
    end
  end

  @doc """
  Drop a database.

  Returns `:ok` on success or `{:error, %Arcadex.Error{}}` on failure.

  ## Examples

      iex> Arcadex.Server.drop_database(conn, "olddb")
      :ok

      iex> Arcadex.Server.drop_database(conn, "nonexistent")
      {:error, %Arcadex.Error{message: "Database 'nonexistent' does not exist"}}

  """
  @spec drop_database(Conn.t(), String.t()) :: :ok | {:error, Error.t()}
  def drop_database(%Conn{} = conn, name) do
    case Client.post(conn, "/api/v1/server", %{command: "drop database #{name}"}) do
      {:ok, %{"result" => "ok"}} -> :ok
      {:ok, _} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Drop a database. Raises on error.

  Returns `:ok` on success or raises `Arcadex.Error`.

  ## Examples

      iex> Arcadex.Server.drop_database!(conn, "olddb")
      :ok

  """
  @spec drop_database!(Conn.t(), String.t()) :: :ok
  def drop_database!(%Conn{} = conn, name) do
    case drop_database(conn, name) do
      :ok -> :ok
      {:error, error} -> raise error
    end
  end

  @doc """
  Check if database exists.

  Returns `true` if the database exists, `false` otherwise.

  ## Examples

      iex> Arcadex.Server.database_exists?(conn, "mydb")
      true

      iex> Arcadex.Server.database_exists?(conn, "nonexistent")
      false

  """
  @spec database_exists?(Conn.t(), String.t()) :: boolean()
  def database_exists?(%Conn{} = conn, name) do
    case Client.get(conn, "/api/v1/exists/#{name}") do
      {:ok, %{"result" => true}} -> true
      {:ok, %{"result" => false}} -> false
      {:error, _} -> false
    end
  end
end
