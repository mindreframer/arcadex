defmodule Arcadex.IntegrationCase do
  @moduledoc """
  Base case for integration tests that run against a real ArcadeDB server.

  ## Usage

      defmodule MyIntegrationTest do
        use Arcadex.IntegrationCase, async: true

        test "example", %{conn: conn} do
          result = Arcadex.query!(conn, "SELECT 1")
          assert result
        end
      end

  ## Database Modes

  - **Module-level** (default): All tests in the module share the same database.
  - **Test-level** (`@tag :fresh_db`): Each test gets its own fresh database.
  - **Sequential** (`async: false`): For server-level operations that must run alone.

  ## Tags

  - `@moduletag :integration` - Applied automatically to all tests
  - `@tag :fresh_db` - Give this test its own database
  """

  use ExUnit.CaseTemplate

  using do
    quote do
      @moduletag :integration
      import Arcadex.IntegrationCase
    end
  end

  setup_all context do
    db_name = generate_db_name(context.module)
    conn = create_connection()

    Arcadex.create_database!(conn, db_name)
    conn = Arcadex.with_database(conn, db_name)

    on_exit(fn ->
      cleanup_conn = create_connection()
      Arcadex.drop_database!(cleanup_conn, db_name)
    end)

    {:ok, conn: conn, db_name: db_name}
  end

  setup context do
    if context[:fresh_db] do
      db_name = "#{context.db_name}_#{:erlang.unique_integer([:positive])}"
      conn = create_connection()

      Arcadex.create_database!(conn, db_name)
      conn = Arcadex.with_database(conn, db_name)

      on_exit(fn ->
        cleanup_conn = create_connection()
        Arcadex.drop_database!(cleanup_conn, db_name)
      end)

      {:ok, conn: conn, db_name: db_name}
    else
      :ok
    end
  end

  @doc """
  Generate a unique database name based on the test module.
  """
  def generate_db_name(module) do
    suffix =
      module
      |> Module.split()
      |> List.last()
      |> Macro.underscore()

    "arx_test_#{suffix}_#{:erlang.unique_integer([:positive])}"
  end

  @doc """
  Create a connection to the ArcadeDB server using environment variables.
  """
  def create_connection do
    url = System.get_env("ARCADEDB_URL", "http://localhost:2480")
    user = System.get_env("ARCADEDB_USER", "root")
    password = System.get_env("ARCADEDB_PASSWORD", "playwithdata")

    Arcadex.connect(url, "system", auth: {user, password})
  end

  @doc """
  Generate a unique identifier for test records.
  """
  def generate_uid do
    :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
  end
end
