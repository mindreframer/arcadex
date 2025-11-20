defmodule Arcadex.MigratorApiTest do
  use ExUnit.Case, async: true

  alias Arcadex.Migrator

  setup do
    bypass = Bypass.open()
    conn = Arcadex.connect("http://localhost:#{bypass.port}", "testdb")
    {:ok, bypass: bypass, conn: conn}
  end

  describe "ARX003_3A_T1: migrate/2 runs pending migrations" do
    test "runs all pending migrations and returns count", %{bypass: bypass, conn: conn} do
      # Setup ETS to track calls
      calls = :ets.new(:migrate_calls, [:ordered_set, :public])
      :ets.insert(calls, {:call_count, 0})

      Bypass.stub(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        [{:call_count, count}] = :ets.lookup(calls, :call_count)
        :ets.insert(calls, {:call_count, count + 1})

        cond do
          # First query: check if _migrations exists
          request["command"] =~ "schema:types" ->
            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(
              200,
              Jason.encode!(%{"result" => [%{"name" => "_migrations"}]})
            )

          # Second query: get applied versions
          request["command"] =~ "SELECT version FROM _migrations" ->
            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))

          true ->
            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
        end
      end)

      # Expect commands for migrations
      Bypass.stub(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => [%{"@rid" => "#10:0"}]}))
      end)

      assert {:ok, 2} = Migrator.migrate(conn, Arcadex.TestMigrations)

      :ets.delete(calls)
    end
  end

  describe "ARX003_3A_T2: migrate/2 returns {:ok, 0} when none pending" do
    test "returns zero count when all migrations applied", %{bypass: bypass, conn: conn} do
      Bypass.stub(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        cond do
          request["command"] =~ "schema:types" ->
            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(
              200,
              Jason.encode!(%{"result" => [%{"name" => "_migrations"}]})
            )

          request["command"] =~ "SELECT version FROM _migrations" ->
            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(
              200,
              Jason.encode!(%{"result" => [%{"version" => 1}, %{"version" => 2}]})
            )

          true ->
            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
        end
      end)

      assert {:ok, 0} = Migrator.migrate(conn, Arcadex.TestMigrations)
    end
  end

  describe "ARX003_3A_T3: migrate/2 stops on error" do
    test "returns error when migration fails", %{bypass: bypass, conn: conn} do
      Bypass.stub(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        cond do
          request["command"] =~ "schema:types" ->
            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(
              200,
              Jason.encode!(%{"result" => [%{"name" => "_migrations"}]})
            )

          request["command"] =~ "SELECT version FROM _migrations" ->
            # V001 already applied, V003 (failing) is pending
            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(
              200,
              Jason.encode!(%{"result" => [%{"version" => 1}]})
            )

          true ->
            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
        end
      end)

      # No commands expected since V003 raises immediately
      assert {:error, %Arcadex.Error{message: "Migration failed"}} =
               Migrator.migrate(conn, Arcadex.TestMigrationsWithFailure)
    end
  end

  describe "ARX003_3A_T4: rollback/3 rolls back n migrations" do
    test "rolls back last n migrations", %{bypass: bypass, conn: conn} do
      Bypass.stub(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        cond do
          request["command"] =~ "schema:types" ->
            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(
              200,
              Jason.encode!(%{"result" => [%{"name" => "_migrations"}]})
            )

          request["command"] =~ "SELECT version FROM _migrations" ->
            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(
              200,
              Jason.encode!(%{"result" => [%{"version" => 1}, %{"version" => 2}]})
            )

          true ->
            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
        end
      end)

      commands = :ets.new(:rollback_commands, [:ordered_set, :public])
      :ets.insert(commands, {:count, 0})

      Bypass.stub(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        [{:count, count}] = :ets.lookup(commands, :count)
        :ets.insert(commands, {:count, count + 1})
        :ets.insert(commands, {count, request["command"]})

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert {:ok, 2} = Migrator.rollback(conn, Arcadex.TestMigrations, 2)

      # Should have commands for both migrations (DROP + DELETE for each)
      [{:count, count}] = :ets.lookup(commands, :count)
      assert count == 4

      # First rollback is V002 (highest version first)
      [{0, cmd1}] = :ets.lookup(commands, 0)
      assert cmd1 =~ "DROP TYPE TestOrder"

      # Then V001
      [{2, cmd3}] = :ets.lookup(commands, 2)
      assert cmd3 =~ "DROP TYPE TestUser"

      :ets.delete(commands)
    end
  end

  describe "ARX003_3A_T5: rollback/3 returns {:ok, 0} when none applied" do
    test "returns zero count when no migrations to rollback", %{bypass: bypass, conn: conn} do
      Bypass.stub(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        cond do
          request["command"] =~ "schema:types" ->
            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(
              200,
              Jason.encode!(%{"result" => [%{"name" => "_migrations"}]})
            )

          request["command"] =~ "SELECT version FROM _migrations" ->
            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))

          true ->
            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
        end
      end)

      assert {:ok, 0} = Migrator.rollback(conn, Arcadex.TestMigrations, 1)
    end
  end

  describe "ARX003_3A_T6: status/2 returns correct status" do
    test "returns status list with applied and pending migrations", %{bypass: bypass, conn: conn} do
      Bypass.stub(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        cond do
          request["command"] =~ "schema:types" ->
            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(
              200,
              Jason.encode!(%{"result" => [%{"name" => "_migrations"}]})
            )

          request["command"] =~ "SELECT version FROM _migrations" ->
            # Only V001 applied
            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(
              200,
              Jason.encode!(%{"result" => [%{"version" => 1}]})
            )

          true ->
            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
        end
      end)

      assert {:ok, status} = Migrator.status(conn, Arcadex.TestMigrations)

      assert length(status) == 2

      [first, second] = status
      assert first.version == 1
      assert first.name == "V001CreateUser"
      assert first.status == :applied

      assert second.version == 2
      assert second.name == "V002CreateOrder"
      assert second.status == :pending
    end
  end

  describe "ARX003_3A_T7: reset/2 rolls back then migrates" do
    test "rolls back all then applies all migrations", %{bypass: bypass, conn: conn} do
      query_count = :ets.new(:reset_queries, [:set, :public])
      :ets.insert(query_count, {:count, 0})

      Bypass.stub(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        [{:count, count}] = :ets.lookup(query_count, :count)
        :ets.insert(query_count, {:count, count + 1})

        cond do
          request["command"] =~ "schema:types" ->
            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(
              200,
              Jason.encode!(%{"result" => [%{"name" => "_migrations"}]})
            )

          request["command"] =~ "SELECT version FROM _migrations" ->
            # First few calls return applied versions, later calls return empty
            if count < 5 do
              http_conn
              |> Plug.Conn.put_resp_content_type("application/json")
              |> Plug.Conn.resp(
                200,
                Jason.encode!(%{"result" => [%{"version" => 1}, %{"version" => 2}]})
              )
            else
              http_conn
              |> Plug.Conn.put_resp_content_type("application/json")
              |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
            end

          true ->
            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
        end
      end)

      Bypass.stub(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => [%{"@rid" => "#10:0"}]}))
      end)

      assert {:ok, 2} = Migrator.reset(conn, Arcadex.TestMigrations)

      :ets.delete(query_count)
    end
  end

  describe "ARX003_3A_T8: multiple migrations in sequence" do
    test "processes migrations in correct order", %{bypass: bypass, conn: conn} do
      Bypass.stub(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        cond do
          request["command"] =~ "schema:types" ->
            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(
              200,
              Jason.encode!(%{"result" => [%{"name" => "_migrations"}]})
            )

          request["command"] =~ "SELECT version FROM _migrations" ->
            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))

          true ->
            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
        end
      end)

      commands = :ets.new(:sequence_commands, [:ordered_set, :public])
      :ets.insert(commands, {:count, 0})

      Bypass.stub(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        [{:count, count}] = :ets.lookup(commands, :count)
        :ets.insert(commands, {:count, count + 1})
        :ets.insert(commands, {count, request["command"]})

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => [%{"@rid" => "#10:0"}]}))
      end)

      assert {:ok, 2} = Migrator.migrate(conn, Arcadex.TestMigrations)

      # Commands should be in order: V001 first, then V002
      [{0, cmd1}] = :ets.lookup(commands, 0)

      # V001 creates TestUser
      assert cmd1 =~ "TestUser"

      # V001's second command or V002's first command
      # V001 has 2 commands, then INSERT, then V002 has 1 command, then INSERT

      # Get all commands for verification
      [{:count, total}] = :ets.lookup(commands, :count)

      # Should have:
      # 0: CREATE DOCUMENT TYPE TestUser
      # 1: CREATE PROPERTY TestUser.name STRING
      # 2: INSERT INTO _migrations (for V001)
      # 3: CREATE DOCUMENT TYPE TestOrder
      # 4: INSERT INTO _migrations (for V002)
      assert total == 5

      [{3, cmd4}] = :ets.lookup(commands, 3)
      assert cmd4 =~ "TestOrder"

      :ets.delete(commands)
    end
  end
end
