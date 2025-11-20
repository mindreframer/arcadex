defmodule Arcadex.IntegrationTest do
  use ExUnit.Case, async: true

  alias Arcadex.{Conn, Error}

  setup do
    bypass = Bypass.open()
    conn = Arcadex.connect("http://localhost:#{bypass.port}", "testdb")
    {:ok, bypass: bypass, conn: conn}
  end

  describe "ARX001_5A_T1: full workflow connect → query → command" do
    @tag :ARX001_5A_T1
    test "executes full workflow through main module", %{bypass: bypass, conn: conn} do
      # Set up query response
      Bypass.expect(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => [%{"count" => 0}]}))
      end)

      # Execute query through main module
      assert {:ok, [%{"count" => 0}]} = Arcadex.query(conn, "SELECT count() FROM User")

      # Set up command response
      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["command"] == "INSERT INTO User SET name = :name"
        assert request["params"] == %{"name" => "John"}

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(
          200,
          Jason.encode!(%{"result" => [%{"@rid" => "#1:0", "name" => "John"}]})
        )
      end)

      # Execute command through main module
      assert {:ok, [%{"@rid" => "#1:0", "name" => "John"}]} =
               Arcadex.command(conn, "INSERT INTO User SET name = :name", %{name: "John"})
    end

    test "connect creates valid connection struct", %{bypass: bypass} do
      conn = Arcadex.connect("http://localhost:#{bypass.port}", "mydb", auth: {"admin", "secret"})

      assert %Conn{} = conn
      assert conn.database == "mydb"
      assert conn.auth == {"admin", "secret"}
      assert conn.session_id == nil
      assert conn.finch_name == Arcadex.Finch
    end
  end

  describe "ARX001_5A_T2: database switching with with_database/2" do
    @tag :ARX001_5A_T2
    test "switches database and queries different database", %{bypass: bypass, conn: conn} do
      # Query original database
      Bypass.expect_once(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => [%{"db" => "testdb"}]}))
      end)

      assert {:ok, [%{"db" => "testdb"}]} = Arcadex.query(conn, "SELECT FROM Info")

      # Switch database
      conn2 = Arcadex.with_database(conn, "otherdb")
      assert conn2.database == "otherdb"
      assert conn2.session_id == nil

      # Query new database
      Bypass.expect_once(bypass, "POST", "/api/v1/query/otherdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => [%{"db" => "otherdb"}]}))
      end)

      assert {:ok, [%{"db" => "otherdb"}]} = Arcadex.query(conn2, "SELECT FROM Info")
    end

    test "clears session_id when switching database", %{bypass: bypass} do
      conn =
        Arcadex.connect("http://localhost:#{bypass.port}", "db1")
        |> Conn.with_session("session-123")

      assert conn.session_id == "session-123"

      conn2 = Arcadex.with_database(conn, "db2")
      assert conn2.session_id == nil
    end
  end

  describe "ARX001_5A_T3: transaction with multiple commands" do
    @tag :ARX001_5A_T3
    test "executes transaction with multiple commands", %{bypass: bypass, conn: conn} do
      session_id = "AS-txn-12345"

      # Begin transaction
      Bypass.expect_once(bypass, "POST", "/api/v1/begin/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => session_id}))
      end)

      # First command in transaction
      Bypass.expect_once(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        assert Plug.Conn.get_req_header(http_conn, "arcadedb-session-id") == [session_id]

        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)
        assert request["command"] == "INSERT INTO User SET name = 'John'"

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(
          200,
          Jason.encode!(%{"result" => [%{"@rid" => "#1:0", "name" => "John"}]})
        )
      end)

      # Commit transaction
      Bypass.expect_once(bypass, "POST", "/api/v1/commit/testdb", fn http_conn ->
        assert Plug.Conn.get_req_header(http_conn, "arcadedb-session-id") == [session_id]

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => "ok"}))
      end)

      # Execute transaction through main module
      assert {:ok, user} =
               Arcadex.transaction(conn, fn tx ->
                 Arcadex.command!(tx, "INSERT INTO User SET name = 'John'")
               end)

      assert user == [%{"@rid" => "#1:0", "name" => "John"}]
    end
  end

  describe "ARX001_5A_T4: transaction rollback preserves data" do
    @tag :ARX001_5A_T4
    test "rolls back transaction on exception", %{bypass: bypass, conn: conn} do
      session_id = "AS-txn-rollback"

      # Begin transaction
      Bypass.expect_once(bypass, "POST", "/api/v1/begin/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => session_id}))
      end)

      # Command in transaction (succeeds)
      Bypass.expect_once(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        assert Plug.Conn.get_req_header(http_conn, "arcadedb-session-id") == [session_id]

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => [%{"@rid" => "#1:0"}]}))
      end)

      # Rollback transaction (after exception)
      Bypass.expect_once(bypass, "POST", "/api/v1/rollback/testdb", fn http_conn ->
        assert Plug.Conn.get_req_header(http_conn, "arcadedb-session-id") == [session_id]

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => "ok"}))
      end)

      # Execute transaction that raises
      assert {:error, %Error{message: "Transaction failed"}} =
               Arcadex.transaction(conn, fn tx ->
                 Arcadex.command!(tx, "INSERT INTO User SET name = 'John'")
                 raise "Something went wrong"
               end)
    end

    test "rolls back transaction on command error", %{bypass: bypass, conn: conn} do
      session_id = "AS-txn-error"

      # Begin transaction
      Bypass.expect_once(bypass, "POST", "/api/v1/begin/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => session_id}))
      end)

      # Command fails
      Bypass.expect_once(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(400, Jason.encode!(%{"error" => "Type does not exist"}))
      end)

      # Rollback
      Bypass.expect_once(bypass, "POST", "/api/v1/rollback/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => "ok"}))
      end)

      # Execute transaction with failing command
      assert {:error, %Error{}} =
               Arcadex.transaction(conn, fn tx ->
                 Arcadex.command!(tx, "INSERT INTO NonExistent SET name = 'John'")
               end)
    end
  end

  describe "ARX001_5A_T5: concurrent requests use connection pool" do
    @tag :ARX001_5A_T5
    test "multiple concurrent requests use same Finch pool", %{bypass: bypass, conn: conn} do
      # Set up bypass to handle multiple concurrent requests
      Bypass.stub(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        # Small delay to ensure concurrent execution
        Process.sleep(10)

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => [%{"id" => :rand.uniform(1000)}]}))
      end)

      # Execute multiple queries concurrently
      tasks =
        for _i <- 1..5 do
          Task.async(fn ->
            Arcadex.query(conn, "SELECT FROM User")
          end)
        end

      # Wait for all tasks to complete
      results = Task.await_many(tasks)

      # All should succeed
      assert Enum.all?(results, fn
               {:ok, _} -> true
               _ -> false
             end)

      assert length(results) == 5

      # Verify all use the same finch_name
      assert conn.finch_name == Arcadex.Finch
    end
  end

  describe "ARX001_5A_T6: create → use → drop database lifecycle" do
    @tag :ARX001_5A_T6
    test "full database lifecycle through main module", %{bypass: bypass, conn: conn} do
      db_name = "lifecycle_test_db"

      # Check database doesn't exist
      Bypass.expect_once(bypass, "GET", "/api/v1/exists/#{db_name}", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => false}))
      end)

      assert Arcadex.database_exists?(conn, db_name) == false

      # Create database
      Bypass.expect_once(bypass, "POST", "/api/v1/server", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)
        assert request["command"] == "create database #{db_name}"

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => "ok"}))
      end)

      assert :ok = Arcadex.create_database!(conn, db_name)

      # Switch to new database and use it
      conn2 = Arcadex.with_database(conn, db_name)
      assert conn2.database == db_name

      # Query the new database
      Bypass.expect_once(bypass, "POST", "/api/v1/query/#{db_name}", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert {:ok, []} = Arcadex.query(conn2, "SELECT FROM V")

      # Drop database
      Bypass.expect_once(bypass, "POST", "/api/v1/server", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)
        assert request["command"] == "drop database #{db_name}"

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => "ok"}))
      end)

      assert :ok = Arcadex.drop_database!(conn, db_name)
    end

    test "create_database returns error on failure", %{bypass: bypass, conn: conn} do
      Bypass.expect_once(bypass, "POST", "/api/v1/server", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(
          400,
          Jason.encode!(%{"error" => "Database 'existing' already exists", "detail" => ""})
        )
      end)

      assert {:error, %Error{message: "Database 'existing' already exists"}} =
               Arcadex.create_database(conn, "existing")
    end
  end

  describe "bang functions" do
    test "query! raises on error", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(400, Jason.encode!(%{"error" => "Syntax error"}))
      end)

      assert_raise Error, "Syntax error", fn ->
        Arcadex.query!(conn, "INVALID SQL")
      end
    end

    test "command! raises on error", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(400, Jason.encode!(%{"error" => "Invalid command"}))
      end)

      assert_raise Error, "Invalid command", fn ->
        Arcadex.command!(conn, "INVALID")
      end
    end
  end
end
