defmodule Arcadex.Transaction do
  @moduledoc """
  Transaction support for ArcadeDB.

  Provides functions to manage transactions including begin, commit, rollback,
  and a wrapper function that auto-commits on success and rolls back on error.
  """

  alias Arcadex.{Conn, Client, Error}

  @doc """
  Execute function within a transaction.

  Auto-commits on success, rolls back on error.
  Returns `{:ok, result}` or `{:error, %Arcadex.Error{}}`.

  ## Examples

      iex> Arcadex.Transaction.transaction(conn, fn tx ->
      ...>   Arcadex.Query.command!(tx, "INSERT INTO User SET name = 'John'")
      ...> end)
      {:ok, [%{"@rid" => "#1:0", "name" => "John"}]}

      iex> Arcadex.Transaction.transaction(conn, fn tx ->
      ...>   raise "oops"
      ...> end)
      {:error, %Arcadex.Error{message: "Transaction failed", detail: "oops"}}

  """
  @spec transaction(Conn.t(), (Conn.t() -> any())) :: {:ok, any()} | {:error, Error.t()}
  def transaction(%Conn{} = conn, fun) when is_function(fun, 1) do
    with {:ok, session_id} <- begin_tx(conn) do
      tx_conn = Conn.with_session(conn, session_id)

      try do
        result = fun.(tx_conn)

        case commit(tx_conn) do
          :ok -> {:ok, result}
          {:error, error} -> {:error, error}
        end
      rescue
        e ->
          rollback(tx_conn)
          {:error, %Error{message: "Transaction failed", detail: Exception.message(e)}}
      catch
        :throw, value ->
          rollback(tx_conn)
          {:error, %Error{message: "Transaction aborted", detail: inspect(value)}}
      end
    end
  end

  @doc """
  Begin a transaction.

  Returns `{:ok, session_id}` or `{:error, %Arcadex.Error{}}`.
  The session_id must be included in subsequent requests within this transaction.

  ## Examples

      iex> Arcadex.Transaction.begin_tx(conn)
      {:ok, "AS-1234-5678"}

  """
  @spec begin_tx(Conn.t()) :: {:ok, String.t()} | {:error, Error.t()}
  def begin_tx(%Conn{} = conn) do
    case Client.post(conn, "/api/v1/begin/#{conn.database}", %{}) do
      {:ok, %{"result" => session_id}} -> {:ok, session_id}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Commit current transaction.

  Returns `:ok` or `{:error, %Arcadex.Error{}}`.

  ## Examples

      iex> Arcadex.Transaction.commit(conn_with_session)
      :ok

  """
  @spec commit(Conn.t()) :: :ok | {:error, Error.t()}
  def commit(%Conn{session_id: nil}), do: {:error, %Error{message: "No active transaction"}}

  def commit(%Conn{} = conn) do
    case Client.post(conn, "/api/v1/commit/#{conn.database}", %{}) do
      {:ok, _} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Rollback current transaction.

  Returns `:ok` even on error (rollback errors are ignored).
  Returns `:ok` if there's no active transaction.

  ## Examples

      iex> Arcadex.Transaction.rollback(conn_with_session)
      :ok

  """
  @spec rollback(Conn.t()) :: :ok
  def rollback(%Conn{session_id: nil}), do: :ok

  def rollback(%Conn{} = conn) do
    case Client.post(conn, "/api/v1/rollback/#{conn.database}", %{}) do
      {:ok, _} -> :ok
      # Ignore rollback errors
      {:error, _} -> :ok
    end
  end
end
