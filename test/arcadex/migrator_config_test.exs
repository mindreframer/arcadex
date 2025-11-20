defmodule Arcadex.MigratorConfigTest do
  use ExUnit.Case, async: false

  alias Arcadex.Migrator

  setup do
    bypass = Bypass.open()
    conn = Arcadex.connect("http://localhost:#{bypass.port}", "testdb")
    {:ok, bypass: bypass, conn: conn}
  end

  describe "ARX003_4A_T1: migrate/1 uses config registry" do
    test "uses registry from config to run migrations", %{bypass: bypass, conn: conn} do
      # Set config
      Application.put_env(:arcadedb, :migrations, Arcadex.TestMigrations)

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

      Bypass.stub(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => [%{"@rid" => "#10:0"}]}))
      end)

      # Call migrate/1 (no registry parameter)
      assert {:ok, 2} = Migrator.migrate(conn)

      # Clean up
      Application.delete_env(:arcadedb, :migrations)
    end
  end

  describe "ARX003_4A_T2: rollback/1 uses config registry" do
    test "uses registry from config to rollback last migration", %{bypass: bypass, conn: conn} do
      # Set config
      Application.put_env(:arcadedb, :migrations, Arcadex.TestMigrations)

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

      commands = :ets.new(:rollback_config_commands, [:ordered_set, :public])
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

      # Call rollback/1 (no registry parameter) - should rollback 1
      assert {:ok, 1} = Migrator.rollback(conn)

      # Should have 2 commands: DROP and DELETE for V002 only
      [{:count, count}] = :ets.lookup(commands, :count)
      assert count == 2

      # First rollback is V002 (highest version)
      [{0, cmd1}] = :ets.lookup(commands, 0)
      assert cmd1 =~ "DROP TYPE TestOrder"

      :ets.delete(commands)

      # Clean up
      Application.delete_env(:arcadedb, :migrations)
    end

    test "rollback/2 uses registry from config with n parameter", %{bypass: bypass, conn: conn} do
      # Set config
      Application.put_env(:arcadedb, :migrations, Arcadex.TestMigrations)

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

      Bypass.stub(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      # Call rollback/2 with n=2
      assert {:ok, 2} = Migrator.rollback(conn, 2)

      # Clean up
      Application.delete_env(:arcadedb, :migrations)
    end
  end

  describe "ARX003_4A_T3: status/1 uses config registry" do
    test "uses registry from config to get status", %{bypass: bypass, conn: conn} do
      # Set config
      Application.put_env(:arcadedb, :migrations, Arcadex.TestMigrations)

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

      # Call status/1 (no registry parameter)
      assert {:ok, status} = Migrator.status(conn)

      assert length(status) == 2

      [first, second] = status
      assert first.version == 1
      assert first.name == "V001CreateUser"
      assert first.status == :applied

      assert second.version == 2
      assert second.name == "V002CreateOrder"
      assert second.status == :pending

      # Clean up
      Application.delete_env(:arcadedb, :migrations)
    end

    test "reset/1 uses registry from config", %{bypass: bypass, conn: conn} do
      # Set config
      Application.put_env(:arcadedb, :migrations, Arcadex.TestMigrations)

      query_count = :ets.new(:reset_config_queries, [:set, :public])
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

      # Call reset/1 (no registry parameter)
      assert {:ok, 2} = Migrator.reset(conn)

      :ets.delete(query_count)

      # Clean up
      Application.delete_env(:arcadedb, :migrations)
    end
  end

  describe "ARX003_4A_T4: raises when config missing" do
    test "migrate/1 raises when :migrations config not set", %{conn: conn} do
      # Ensure config is not set
      Application.delete_env(:arcadedb, :migrations)

      assert_raise ArgumentError, ~r/could not fetch application environment/, fn ->
        Migrator.migrate(conn)
      end
    end

    test "rollback/1 raises when :migrations config not set", %{conn: conn} do
      # Ensure config is not set
      Application.delete_env(:arcadedb, :migrations)

      assert_raise ArgumentError, ~r/could not fetch application environment/, fn ->
        Migrator.rollback(conn)
      end
    end

    test "rollback/2 raises when :migrations config not set", %{conn: conn} do
      # Ensure config is not set
      Application.delete_env(:arcadedb, :migrations)

      assert_raise ArgumentError, ~r/could not fetch application environment/, fn ->
        Migrator.rollback(conn, 2)
      end
    end

    test "status/1 raises when :migrations config not set", %{conn: conn} do
      # Ensure config is not set
      Application.delete_env(:arcadedb, :migrations)

      assert_raise ArgumentError, ~r/could not fetch application environment/, fn ->
        Migrator.status(conn)
      end
    end

    test "reset/1 raises when :migrations config not set", %{conn: conn} do
      # Ensure config is not set
      Application.delete_env(:arcadedb, :migrations)

      assert_raise ArgumentError, ~r/could not fetch application environment/, fn ->
        Migrator.reset(conn)
      end
    end
  end
end
