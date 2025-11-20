defmodule Arcadex.MigratorIntegrationTest do
  use ExUnit.Case, async: true

  alias Arcadex.Migrator

  # Test migrations for integration testing
  defmodule V001CreateProduct do
    @moduledoc false
    @behaviour Arcadex.Migration

    @impl true
    def version, do: 100

    @impl true
    def up(conn) do
      Arcadex.command!(conn, "CREATE DOCUMENT TYPE IntegrationProduct")
      Arcadex.command!(conn, "CREATE PROPERTY IntegrationProduct.name STRING")
      Arcadex.command!(conn, "CREATE PROPERTY IntegrationProduct.price DECIMAL")
      Arcadex.command!(conn, "CREATE INDEX ON IntegrationProduct (name) UNIQUE")
      :ok
    end

    @impl true
    def down(conn) do
      Arcadex.command!(conn, "DROP TYPE IntegrationProduct IF EXISTS")
      :ok
    end
  end

  defmodule V002CreateCategory do
    @moduledoc false
    @behaviour Arcadex.Migration

    @impl true
    def version, do: 200

    @impl true
    def up(conn) do
      Arcadex.command!(conn, "CREATE DOCUMENT TYPE IntegrationCategory")
      Arcadex.command!(conn, "CREATE PROPERTY IntegrationCategory.name STRING")
      :ok
    end

    @impl true
    def down(conn) do
      Arcadex.command!(conn, "DROP TYPE IntegrationCategory IF EXISTS")
      :ok
    end
  end

  defmodule IntegrationRegistry do
    @moduledoc false
    use Arcadex.MigrationRegistry

    migrations([
      Arcadex.MigratorIntegrationTest.V001CreateProduct,
      Arcadex.MigratorIntegrationTest.V002CreateCategory
    ])
  end

  # Second registry for testing multiple registries
  defmodule V001CreateLog do
    @moduledoc false
    @behaviour Arcadex.Migration

    @impl true
    def version, do: 1000

    @impl true
    def up(conn) do
      Arcadex.command!(conn, "CREATE DOCUMENT TYPE IntegrationLog")
      Arcadex.command!(conn, "CREATE PROPERTY IntegrationLog.message STRING")
      Arcadex.command!(conn, "CREATE PROPERTY IntegrationLog.timestamp DATETIME")
      :ok
    end

    @impl true
    def down(conn) do
      Arcadex.command!(conn, "DROP TYPE IntegrationLog IF EXISTS")
      :ok
    end
  end

  defmodule SecondRegistry do
    @moduledoc false
    use Arcadex.MigrationRegistry

    migrations([
      Arcadex.MigratorIntegrationTest.V001CreateLog
    ])
  end

  setup do
    bypass = Bypass.open()
    conn = Arcadex.connect("http://localhost:#{bypass.port}", "integrationdb")
    {:ok, bypass: bypass, conn: conn}
  end

  describe "ARX003_5A_T1: full workflow migrate → use → rollback" do
    test "complete migration lifecycle", %{bypass: bypass, conn: conn} do
      # Track state of applied migrations
      state = :ets.new(:workflow_state, [:set, :public])
      :ets.insert(state, {:applied, []})

      Bypass.stub(bypass, "POST", "/api/v1/query/integrationdb", fn http_conn ->
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
            [{:applied, applied}] = :ets.lookup(state, :applied)

            result =
              applied
              |> Enum.sort()
              |> Enum.map(&%{"version" => &1})

            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(200, Jason.encode!(%{"result" => result}))

          true ->
            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
        end
      end)

      Bypass.stub(bypass, "POST", "/api/v1/command/integrationdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)
        command = request["command"]

        # Track inserts and deletes to _migrations
        cond do
          command =~ "INSERT INTO _migrations" ->
            # Extract version from params
            version =
              case request["params"] do
                %{"version" => v} -> v
                _ -> nil
              end

            if version do
              [{:applied, applied}] = :ets.lookup(state, :applied)
              :ets.insert(state, {:applied, [version | applied]})
            end

          command =~ "DELETE FROM _migrations" ->
            version =
              case request["params"] do
                %{"version" => v} -> v
                _ -> nil
              end

            if version do
              [{:applied, applied}] = :ets.lookup(state, :applied)
              :ets.insert(state, {:applied, List.delete(applied, version)})
            end

          true ->
            :ok
        end

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => [%{"@rid" => "#10:0"}]}))
      end)

      # Step 1: Migrate all
      assert {:ok, 2} = Migrator.migrate(conn, IntegrationRegistry)

      # Step 2: Check status - all should be applied
      {:ok, status} = Migrator.status(conn, IntegrationRegistry)
      assert length(status) == 2
      assert Enum.all?(status, &(&1.status == :applied))

      # Step 3: Rollback 1
      assert {:ok, 1} = Migrator.rollback(conn, IntegrationRegistry, 1)

      # Step 4: Check status - one pending
      {:ok, status} = Migrator.status(conn, IntegrationRegistry)
      applied_count = Enum.count(status, &(&1.status == :applied))
      pending_count = Enum.count(status, &(&1.status == :pending))
      assert applied_count == 1
      assert pending_count == 1

      # Step 5: Migrate again
      assert {:ok, 1} = Migrator.migrate(conn, IntegrationRegistry)

      # Step 6: Check status - all applied again
      {:ok, status} = Migrator.status(conn, IntegrationRegistry)
      assert Enum.all?(status, &(&1.status == :applied))

      :ets.delete(state)
    end
  end

  describe "ARX003_5A_T2: migration with multiple DDL commands" do
    test "handles migrations with multiple DDL operations", %{bypass: bypass, conn: conn} do
      commands = :ets.new(:multi_ddl_commands, [:ordered_set, :public])
      :ets.insert(commands, {:count, 0})

      Bypass.stub(bypass, "POST", "/api/v1/query/integrationdb", fn http_conn ->
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

      Bypass.stub(bypass, "POST", "/api/v1/command/integrationdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        [{:count, count}] = :ets.lookup(commands, :count)
        :ets.insert(commands, {:count, count + 1})
        :ets.insert(commands, {count, request["command"]})

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => [%{"@rid" => "#10:0"}]}))
      end)

      # Run migration which has 4 DDL commands (CREATE TYPE, 2 PROPERTIES, INDEX)
      assert {:ok, 2} = Migrator.migrate(conn, IntegrationRegistry)

      # Verify all commands were executed
      [{:count, total}] = :ets.lookup(commands, :count)

      # V001: CREATE TYPE + 2 PROPERTIES + INDEX + INSERT = 5
      # V002: CREATE TYPE + 1 PROPERTY + INSERT = 3
      # Total = 8
      assert total == 8

      # Verify V001 commands
      [{0, cmd1}] = :ets.lookup(commands, 0)
      assert cmd1 =~ "CREATE DOCUMENT TYPE IntegrationProduct"

      [{1, cmd2}] = :ets.lookup(commands, 1)
      assert cmd2 =~ "CREATE PROPERTY IntegrationProduct.name"

      [{2, cmd3}] = :ets.lookup(commands, 2)
      assert cmd3 =~ "CREATE PROPERTY IntegrationProduct.price"

      [{3, cmd4}] = :ets.lookup(commands, 3)
      assert cmd4 =~ "CREATE INDEX ON IntegrationProduct"

      # Verify V002 commands
      [{5, cmd6}] = :ets.lookup(commands, 5)
      assert cmd6 =~ "CREATE DOCUMENT TYPE IntegrationCategory"

      :ets.delete(commands)
    end
  end

  describe "ARX003_5A_T3: migration creates working types" do
    test "verifies types can be used after migration", %{bypass: bypass, conn: conn} do
      query_results = :ets.new(:query_results, [:set, :public])
      :ets.insert(query_results, {:has_migrations_table, true})
      :ets.insert(query_results, {:applied_versions, []})

      Bypass.stub(bypass, "POST", "/api/v1/query/integrationdb", fn http_conn ->
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
            [{:applied_versions, versions}] = :ets.lookup(query_results, :applied_versions)

            result =
              versions
              |> Enum.sort()
              |> Enum.map(&%{"version" => &1})

            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(200, Jason.encode!(%{"result" => result}))

          # Query for IntegrationProduct (simulating usage)
          request["command"] =~ "SELECT FROM IntegrationProduct" ->
            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(
              200,
              Jason.encode!(%{
                "result" => [
                  %{"@rid" => "#20:0", "name" => "Test Product", "price" => 99.99}
                ]
              })
            )

          true ->
            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
        end
      end)

      Bypass.stub(bypass, "POST", "/api/v1/command/integrationdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)
        command = request["command"]

        # Track inserts to _migrations
        if command =~ "INSERT INTO _migrations" do
          version =
            case request["params"] do
              %{"version" => v} -> v
              _ -> nil
            end

          if version do
            [{:applied_versions, versions}] = :ets.lookup(query_results, :applied_versions)
            :ets.insert(query_results, {:applied_versions, [version | versions]})
          end
        end

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => [%{"@rid" => "#10:0"}]}))
      end)

      # Step 1: Run migrations
      assert {:ok, 2} = Migrator.migrate(conn, IntegrationRegistry)

      # Step 2: Verify we can query the created type
      {:ok, result} = Arcadex.query(conn, "SELECT FROM IntegrationProduct")
      assert length(result) == 1
      assert hd(result)["name"] == "Test Product"

      :ets.delete(query_results)
    end
  end

  describe "ARX003_5A_T4: rollback cleans up completely" do
    test "rollback removes all created structures", %{bypass: bypass, conn: conn} do
      state = :ets.new(:cleanup_state, [:set, :public])
      :ets.insert(state, {:applied, [100, 200]})

      commands = :ets.new(:cleanup_commands, [:ordered_set, :public])
      :ets.insert(commands, {:count, 0})

      Bypass.stub(bypass, "POST", "/api/v1/query/integrationdb", fn http_conn ->
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
            [{:applied, applied}] = :ets.lookup(state, :applied)

            result =
              applied
              |> Enum.sort()
              |> Enum.map(&%{"version" => &1})

            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(200, Jason.encode!(%{"result" => result}))

          true ->
            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
        end
      end)

      Bypass.stub(bypass, "POST", "/api/v1/command/integrationdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)
        command = request["command"]

        [{:count, count}] = :ets.lookup(commands, :count)
        :ets.insert(commands, {:count, count + 1})
        :ets.insert(commands, {count, command})

        # Track deletes from _migrations
        if command =~ "DELETE FROM _migrations" do
          version =
            case request["params"] do
              %{"version" => v} -> v
              _ -> nil
            end

          if version do
            [{:applied, applied}] = :ets.lookup(state, :applied)
            :ets.insert(state, {:applied, List.delete(applied, version)})
          end
        end

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      # Rollback all migrations
      assert {:ok, 2} = Migrator.rollback(conn, IntegrationRegistry, 2)

      # Verify DROP commands were issued
      [{:count, total}] = :ets.lookup(commands, :count)

      # V002 rollback: DROP TYPE + DELETE = 2
      # V001 rollback: DROP TYPE + DELETE = 2
      # Total = 4
      assert total == 4

      # Verify V002 rolled back first (descending order)
      [{0, cmd1}] = :ets.lookup(commands, 0)
      assert cmd1 =~ "DROP TYPE IntegrationCategory"

      [{1, cmd2}] = :ets.lookup(commands, 1)
      assert cmd2 =~ "DELETE FROM _migrations"

      # Then V001
      [{2, cmd3}] = :ets.lookup(commands, 2)
      assert cmd3 =~ "DROP TYPE IntegrationProduct"

      [{3, cmd4}] = :ets.lookup(commands, 3)
      assert cmd4 =~ "DELETE FROM _migrations"

      # Verify all migrations are now pending
      {:ok, status} = Migrator.status(conn, IntegrationRegistry)
      assert Enum.all?(status, &(&1.status == :pending))

      :ets.delete(state)
      :ets.delete(commands)
    end
  end

  describe "ARX003_5A_T5: different registries for different databases" do
    test "supports multiple registries with different migrations", %{bypass: bypass, conn: conn} do
      # Create a second connection for a different database
      conn2 = Arcadex.connect("http://localhost:#{bypass.port}", "seconddb")

      # Track state for both databases
      state1 = :ets.new(:db1_state, [:set, :public])
      :ets.insert(state1, {:applied, []})

      state2 = :ets.new(:db2_state, [:set, :public])
      :ets.insert(state2, {:applied, []})

      # Handle integrationdb
      Bypass.stub(bypass, "POST", "/api/v1/query/integrationdb", fn http_conn ->
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
            [{:applied, applied}] = :ets.lookup(state1, :applied)

            result =
              applied
              |> Enum.sort()
              |> Enum.map(&%{"version" => &1})

            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(200, Jason.encode!(%{"result" => result}))

          true ->
            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
        end
      end)

      Bypass.stub(bypass, "POST", "/api/v1/command/integrationdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)
        command = request["command"]

        if command =~ "INSERT INTO _migrations" do
          version =
            case request["params"] do
              %{"version" => v} -> v
              _ -> nil
            end

          if version do
            [{:applied, applied}] = :ets.lookup(state1, :applied)
            :ets.insert(state1, {:applied, [version | applied]})
          end
        end

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => [%{"@rid" => "#10:0"}]}))
      end)

      # Handle seconddb
      Bypass.stub(bypass, "POST", "/api/v1/query/seconddb", fn http_conn ->
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
            [{:applied, applied}] = :ets.lookup(state2, :applied)

            result =
              applied
              |> Enum.sort()
              |> Enum.map(&%{"version" => &1})

            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(200, Jason.encode!(%{"result" => result}))

          true ->
            http_conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
        end
      end)

      Bypass.stub(bypass, "POST", "/api/v1/command/seconddb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)
        command = request["command"]

        if command =~ "INSERT INTO _migrations" do
          version =
            case request["params"] do
              %{"version" => v} -> v
              _ -> nil
            end

          if version do
            [{:applied, applied}] = :ets.lookup(state2, :applied)
            :ets.insert(state2, {:applied, [version | applied]})
          end
        end

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => [%{"@rid" => "#10:0"}]}))
      end)

      # Run IntegrationRegistry on conn (integrationdb)
      assert {:ok, 2} = Migrator.migrate(conn, IntegrationRegistry)

      # Run SecondRegistry on conn2 (seconddb)
      assert {:ok, 1} = Migrator.migrate(conn2, SecondRegistry)

      # Verify integrationdb has IntegrationRegistry migrations
      {:ok, status1} = Migrator.status(conn, IntegrationRegistry)
      assert length(status1) == 2
      assert Enum.all?(status1, &(&1.status == :applied))

      # Verify seconddb has SecondRegistry migrations
      {:ok, status2} = Migrator.status(conn2, SecondRegistry)
      assert length(status2) == 1
      assert hd(status2).version == 1000
      assert hd(status2).status == :applied

      # Verify they are independent - integrationdb doesn't have SecondRegistry migrations
      {:ok, status3} = Migrator.status(conn, SecondRegistry)
      assert length(status3) == 1
      assert hd(status3).status == :pending

      :ets.delete(state1)
      :ets.delete(state2)
    end
  end
end
