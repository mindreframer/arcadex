defmodule Arcadex.Conn do
  @moduledoc """
  Connection context for ArcadeDB.

  Holds connection details including base URL, database name, authentication,
  session ID (for transactions), and Finch pool name.
  """

  @type t :: %__MODULE__{
          base_url: String.t(),
          database: String.t(),
          auth: {String.t(), String.t()},
          session_id: String.t() | nil,
          finch_name: atom()
        }

  defstruct [
    :base_url,
    :database,
    :auth,
    :session_id,
    :finch_name
  ]

  @doc """
  Create a new connection context.

  ## Options

    * `:auth` - Tuple of `{username, password}`. Defaults to `{"root", "root"}`.
    * `:finch` - Finch pool name. Defaults to `Arcadex.Finch`.

  ## Examples

      iex> Arcadex.Conn.new("http://localhost:2480", "mydb")
      %Arcadex.Conn{
        base_url: "http://localhost:2480",
        database: "mydb",
        auth: {"root", "root"},
        session_id: nil,
        finch_name: Arcadex.Finch
      }

      iex> Arcadex.Conn.new("http://localhost:2480", "mydb", auth: {"admin", "pass"})
      %Arcadex.Conn{
        base_url: "http://localhost:2480",
        database: "mydb",
        auth: {"admin", "pass"},
        session_id: nil,
        finch_name: Arcadex.Finch
      }

  """
  @spec new(String.t(), String.t(), keyword()) :: t()
  def new(base_url, database, opts \\ []) do
    %__MODULE__{
      base_url: String.trim_trailing(base_url, "/"),
      database: database,
      auth: Keyword.get(opts, :auth, {"root", "root"}),
      session_id: nil,
      finch_name: Keyword.get(opts, :finch, Arcadex.Finch)
    }
  end

  @doc """
  Return new conn with different database (same pool).

  Clears any existing session_id since sessions are database-specific.

  ## Examples

      iex> conn = Arcadex.Conn.new("http://localhost:2480", "db1")
      iex> conn2 = Arcadex.Conn.with_database(conn, "db2")
      iex> conn2.database
      "db2"
      iex> conn2.session_id
      nil

  """
  @spec with_database(t(), String.t()) :: t()
  def with_database(%__MODULE__{} = conn, database) do
    %{conn | database: database, session_id: nil}
  end

  @doc """
  Return new conn with session ID (for transactions).

  ## Examples

      iex> conn = Arcadex.Conn.new("http://localhost:2480", "mydb")
      iex> conn_with_session = Arcadex.Conn.with_session(conn, "sess123")
      iex> conn_with_session.session_id
      "sess123"

  """
  @spec with_session(t(), String.t()) :: t()
  def with_session(%__MODULE__{} = conn, session_id) do
    %{conn | session_id: session_id}
  end
end
