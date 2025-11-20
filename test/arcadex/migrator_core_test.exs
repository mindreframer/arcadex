defmodule Arcadex.MigratorCoreTest do
  use ExUnit.Case, async: true

  alias Arcadex.Migrator

  setup do
    bypass = Bypass.open()
    conn = Arcadex.connect("http://localhost:#{bypass.port}", "testdb")
    {:ok, bypass: bypass, conn: conn}
  end

  describe "ARX003_2A_T1: ensure_migrations_table creates type" do
    test "creates _migrations type when it doesn't exist", %{bypass: bypass, conn: conn} do
      # Query for existing type - returns empty
      Bypass.expect_once(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)
        assert request["command"] =~ "schema:types"

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      # Expect 4 commands to create type, properties, and index
      commands_received = :ets.new(:commands, [:set, :public])
      :ets.insert(commands_received, {:count, 0})

      Bypass.stub(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        [{:count, count}] = :ets.lookup(commands_received, :count)
        :ets.insert(commands_received, {:count, count + 1})
        :ets.insert(commands_received, {count, request["command"]})

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert :ok = Migrator.ensure_migrations_table(conn)

      [{:count, count}] = :ets.lookup(commands_received, :count)
      assert count == 5

      [{0, cmd1}] = :ets.lookup(commands_received, 0)
      [{1, cmd2}] = :ets.lookup(commands_received, 1)
      [{2, cmd3}] = :ets.lookup(commands_received, 2)
      [{3, cmd4}] = :ets.lookup(commands_received, 3)
      [{4, cmd5}] = :ets.lookup(commands_received, 4)

      assert cmd1 == "CREATE DOCUMENT TYPE _migrations"
      assert cmd2 == "CREATE PROPERTY _migrations.version LONG"
      assert cmd3 == "CREATE PROPERTY _migrations.name STRING"
      assert cmd4 == "CREATE PROPERTY _migrations.applied_at DATETIME"
      assert cmd5 =~ "CREATE INDEX"

      :ets.delete(commands_received)
    end
  end

  describe "ARX003_2A_T2: ensure_migrations_table is idempotent" do
    test "does nothing when _migrations type already exists", %{bypass: bypass, conn: conn} do
      # Query for existing type - returns the type
      Bypass.expect_once(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(
          200,
          Jason.encode!(%{"result" => [%{"name" => "_migrations"}]})
        )
      end)

      # No command calls should happen
      assert :ok = Migrator.ensure_migrations_table(conn)
    end
  end

  describe "ARX003_2A_T3: get_applied_versions returns versions" do
    test "returns list of version integers", %{bypass: bypass, conn: conn} do
      Bypass.expect_once(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)
        assert request["command"] =~ "SELECT version FROM _migrations"

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(
          200,
          Jason.encode!(%{
            "result" => [
              %{"version" => 1},
              %{"version" => 2},
              %{"version" => 3}
            ]
          })
        )
      end)

      assert [1, 2, 3] = Migrator.get_applied_versions(conn)
    end
  end

  describe "ARX003_2A_T4: get_applied_versions returns empty list" do
    test "returns empty list when no migrations applied", %{bypass: bypass, conn: conn} do
      Bypass.expect_once(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert [] = Migrator.get_applied_versions(conn)
    end
  end

  describe "ARX003_2A_T5: get_pending_migrations filters correctly" do
    test "filters out applied migrations and sorts by version" do
      applied = [1]

      pending = Migrator.get_pending_migrations(Arcadex.TestMigrations, applied)

      assert length(pending) == 1
      assert hd(pending) == Arcadex.TestMigrations.V002CreateOrder
    end

    test "returns all migrations when none applied" do
      applied = []

      pending = Migrator.get_pending_migrations(Arcadex.TestMigrations, applied)

      assert length(pending) == 2
      assert Enum.at(pending, 0) == Arcadex.TestMigrations.V001CreateUser
      assert Enum.at(pending, 1) == Arcadex.TestMigrations.V002CreateOrder
    end

    test "returns empty list when all migrations applied" do
      applied = [1, 2]

      pending = Migrator.get_pending_migrations(Arcadex.TestMigrations, applied)

      assert pending == []
    end
  end

  describe "ARX003_2A_T6: get_rollback_migrations returns in desc order" do
    test "returns migrations in descending version order" do
      applied = [1, 2]

      rollback = Migrator.get_rollback_migrations(Arcadex.TestMigrations, applied, 2)

      assert length(rollback) == 2
      assert Enum.at(rollback, 0) == Arcadex.TestMigrations.V002CreateOrder
      assert Enum.at(rollback, 1) == Arcadex.TestMigrations.V001CreateUser
    end

    test "takes only n migrations" do
      applied = [1, 2]

      rollback = Migrator.get_rollback_migrations(Arcadex.TestMigrations, applied, 1)

      assert length(rollback) == 1
      assert hd(rollback) == Arcadex.TestMigrations.V002CreateOrder
    end

    test "returns empty list when none applied" do
      applied = []

      rollback = Migrator.get_rollback_migrations(Arcadex.TestMigrations, applied, 2)

      assert rollback == []
    end
  end

  describe "ARX003_2A_T7: run_one :up inserts to _migrations" do
    test "executes up/1 and inserts migration record", %{bypass: bypass, conn: conn} do
      commands_received = :ets.new(:up_commands, [:set, :public])
      :ets.insert(commands_received, {:count, 0})

      Bypass.stub(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        [{:count, count}] = :ets.lookup(commands_received, :count)
        :ets.insert(commands_received, {:count, count + 1})
        :ets.insert(commands_received, {count, request["command"]})

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => [%{"@rid" => "#10:0"}]}))
      end)

      assert :ok = Migrator.run_one(conn, Arcadex.TestMigrations.V001CreateUser, :up)

      [{:count, count}] = :ets.lookup(commands_received, :count)
      assert count == 3

      [{0, cmd1}] = :ets.lookup(commands_received, 0)
      [{1, cmd2}] = :ets.lookup(commands_received, 1)
      [{2, cmd3}] = :ets.lookup(commands_received, 2)

      assert cmd1 == "CREATE DOCUMENT TYPE TestUser"
      assert cmd2 == "CREATE PROPERTY TestUser.name STRING"
      assert cmd3 =~ "INSERT INTO _migrations"

      :ets.delete(commands_received)
    end

    test "returns error when migration fails", %{bypass: bypass, conn: conn} do
      # Mock for CREATE DOCUMENT TYPE TestUser - fails
      Bypass.expect_once(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(400, Jason.encode!(%{"error" => "Type already exists"}))
      end)

      assert {:error, %Arcadex.Error{message: "Migration failed"}} =
               Migrator.run_one(conn, Arcadex.TestMigrations.V001CreateUser, :up)
    end
  end

  describe "ARX003_2A_T8: run_one :down deletes from _migrations" do
    test "executes down/1 and deletes migration record", %{bypass: bypass, conn: conn} do
      commands_received = :ets.new(:down_commands, [:set, :public])
      :ets.insert(commands_received, {:count, 0})

      Bypass.stub(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        [{:count, count}] = :ets.lookup(commands_received, :count)
        :ets.insert(commands_received, {:count, count + 1})
        :ets.insert(commands_received, {count, request["command"]})

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert :ok = Migrator.run_one(conn, Arcadex.TestMigrations.V001CreateUser, :down)

      [{:count, count}] = :ets.lookup(commands_received, :count)
      assert count == 2

      [{0, cmd1}] = :ets.lookup(commands_received, 0)
      [{1, cmd2}] = :ets.lookup(commands_received, 1)

      assert cmd1 == "DROP TYPE TestUser IF EXISTS"
      assert cmd2 =~ "DELETE FROM _migrations"

      :ets.delete(commands_received)
    end

    test "returns error when rollback fails", %{bypass: bypass, conn: conn} do
      # Mock for DROP TYPE - fails
      Bypass.expect_once(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(400, Jason.encode!(%{"error" => "Type not found"}))
      end)

      assert {:error, %Arcadex.Error{message: "Rollback failed"}} =
               Migrator.run_one(conn, Arcadex.TestMigrations.V001CreateUser, :down)
    end
  end

  describe "module_name/1" do
    test "extracts last part of module name" do
      assert Migrator.module_name(Arcadex.TestMigrations.V001CreateUser) == "V001CreateUser"
      assert Migrator.module_name(Arcadex.TestMigrations.V002CreateOrder) == "V002CreateOrder"
    end
  end
end
