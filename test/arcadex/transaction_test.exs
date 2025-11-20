defmodule Arcadex.TransactionTest do
  use ExUnit.Case, async: true

  alias Arcadex.{Transaction, Query, Conn, Error}

  setup do
    bypass = Bypass.open()
    conn = Conn.new("http://localhost:#{bypass.port}", "testdb")
    {:ok, bypass: bypass, conn: conn}
  end

  describe "begin_tx/1" do
    @tag :ARX001_3A_T1
    test "returns session_id on success", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/begin/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => "AS-1234-5678"}))
      end)

      assert {:ok, "AS-1234-5678"} = Transaction.begin_tx(conn)
    end

    test "returns error on failure", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/begin/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(
          500,
          Jason.encode!(%{"error" => "Database error", "detail" => "Cannot start transaction"})
        )
      end)

      assert {:error, %Error{status: 500, message: "Database error"}} = Transaction.begin_tx(conn)
    end
  end

  describe "commit/1" do
    @tag :ARX001_3A_T2
    test "commits transaction successfully", %{bypass: bypass, conn: conn} do
      conn_with_session = Conn.with_session(conn, "AS-1234-5678")

      Bypass.expect(bypass, "POST", "/api/v1/commit/testdb", fn http_conn ->
        assert Plug.Conn.get_req_header(http_conn, "arcadedb-session-id") == ["AS-1234-5678"]

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => "ok"}))
      end)

      assert :ok = Transaction.commit(conn_with_session)
    end

    test "returns error when no active transaction", %{conn: conn} do
      assert {:error, %Error{message: "No active transaction"}} = Transaction.commit(conn)
    end

    test "returns error on commit failure", %{bypass: bypass, conn: conn} do
      conn_with_session = Conn.with_session(conn, "AS-1234-5678")

      Bypass.expect(bypass, "POST", "/api/v1/commit/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(500, Jason.encode!(%{"error" => "Commit failed"}))
      end)

      assert {:error, %Error{message: "Commit failed"}} = Transaction.commit(conn_with_session)
    end
  end

  describe "rollback/1" do
    @tag :ARX001_3A_T3
    test "rolls back transaction successfully", %{bypass: bypass, conn: conn} do
      conn_with_session = Conn.with_session(conn, "AS-1234-5678")

      Bypass.expect(bypass, "POST", "/api/v1/rollback/testdb", fn http_conn ->
        assert Plug.Conn.get_req_header(http_conn, "arcadedb-session-id") == ["AS-1234-5678"]

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => "ok"}))
      end)

      assert :ok = Transaction.rollback(conn_with_session)
    end

    test "returns :ok when no active transaction", %{conn: conn} do
      assert :ok = Transaction.rollback(conn)
    end

    test "ignores rollback errors", %{bypass: bypass, conn: conn} do
      conn_with_session = Conn.with_session(conn, "AS-1234-5678")

      Bypass.expect(bypass, "POST", "/api/v1/rollback/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(500, Jason.encode!(%{"error" => "Rollback failed"}))
      end)

      assert :ok = Transaction.rollback(conn_with_session)
    end
  end

  describe "transaction/2" do
    @tag :ARX001_3A_T4
    test "commits on success", %{bypass: bypass, conn: conn} do
      # Expect begin
      Bypass.expect(bypass, "POST", "/api/v1/begin/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => "AS-tx-123"}))
      end)

      # Expect commit
      Bypass.expect(bypass, "POST", "/api/v1/commit/testdb", fn http_conn ->
        assert Plug.Conn.get_req_header(http_conn, "arcadedb-session-id") == ["AS-tx-123"]

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => "ok"}))
      end)

      assert {:ok, :done} =
               Transaction.transaction(conn, fn _tx ->
                 :done
               end)
    end

    @tag :ARX001_3A_T5
    test "rolls back on error returned from function", %{bypass: bypass, conn: conn} do
      # Expect begin
      Bypass.expect(bypass, "POST", "/api/v1/begin/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => "AS-tx-123"}))
      end)

      # Expect command that fails
      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(400, Jason.encode!(%{"error" => "Insert failed"}))
      end)

      # Expect rollback
      Bypass.expect(bypass, "POST", "/api/v1/rollback/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => "ok"}))
      end)

      assert {:error, %Error{message: "Transaction failed"}} =
               Transaction.transaction(conn, fn tx ->
                 Query.command!(tx, "INSERT INTO User SET name = 'John'")
               end)
    end

    @tag :ARX001_3A_T6
    test "rolls back on raise", %{bypass: bypass, conn: conn} do
      # Expect begin
      Bypass.expect(bypass, "POST", "/api/v1/begin/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => "AS-tx-123"}))
      end)

      # Expect rollback
      Bypass.expect(bypass, "POST", "/api/v1/rollback/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => "ok"}))
      end)

      assert {:error, %Error{message: "Transaction failed", detail: "oops"}} =
               Transaction.transaction(conn, fn _tx ->
                 raise "oops"
               end)
    end

    @tag :ARX001_3A_T7
    test "returns function result on success", %{bypass: bypass, conn: conn} do
      expected_result = [%{"@rid" => "#1:0", "name" => "John"}]

      # Expect begin
      Bypass.expect(bypass, "POST", "/api/v1/begin/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => "AS-tx-123"}))
      end)

      # Expect command
      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => expected_result}))
      end)

      # Expect commit
      Bypass.expect(bypass, "POST", "/api/v1/commit/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => "ok"}))
      end)

      assert {:ok, ^expected_result} =
               Transaction.transaction(conn, fn tx ->
                 Query.command!(tx, "INSERT INTO User SET name = 'John'")
               end)
    end

    @tag :ARX001_3A_T8
    test "nested commands use same session", %{bypass: bypass, conn: conn} do
      user_result = [%{"@rid" => "#1:0", "name" => "John"}]
      log_result = [%{"@rid" => "#2:0", "action" => "created"}]

      # Expect begin
      Bypass.expect(bypass, "POST", "/api/v1/begin/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => "AS-tx-123"}))
      end)

      # Track session IDs from commands
      command_sessions = Agent.start_link(fn -> [] end) |> elem(1)

      # Expect first command
      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        session = Plug.Conn.get_req_header(http_conn, "arcadedb-session-id")
        Agent.update(command_sessions, &[session | &1])

        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        result =
          if String.contains?(request["command"], "User") do
            user_result
          else
            log_result
          end

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => result}))
      end)

      # Expect commit
      Bypass.expect(bypass, "POST", "/api/v1/commit/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => "ok"}))
      end)

      {:ok, ^user_result} =
        Transaction.transaction(conn, fn tx ->
          user = Query.command!(tx, "INSERT INTO User SET name = 'John'")
          Query.command!(tx, "INSERT INTO Log SET action = 'created'")
          user
        end)

      # Verify both commands used the same session
      sessions = Agent.get(command_sessions, & &1)
      assert length(sessions) == 2
      assert Enum.uniq(sessions) == [["AS-tx-123"]]
    end

    test "rolls back on throw", %{bypass: bypass, conn: conn} do
      # Expect begin
      Bypass.expect(bypass, "POST", "/api/v1/begin/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => "AS-tx-123"}))
      end)

      # Expect rollback
      Bypass.expect(bypass, "POST", "/api/v1/rollback/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => "ok"}))
      end)

      assert {:error, %Error{message: "Transaction aborted", detail: ":abort"}} =
               Transaction.transaction(conn, fn _tx ->
                 throw(:abort)
               end)
    end

    test "returns error when begin fails", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/begin/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(500, Jason.encode!(%{"error" => "Cannot start transaction"}))
      end)

      assert {:error, %Error{message: "Cannot start transaction"}} =
               Transaction.transaction(conn, fn _tx ->
                 :should_not_run
               end)
    end

    test "returns error when commit fails", %{bypass: bypass, conn: conn} do
      # Expect begin
      Bypass.expect(bypass, "POST", "/api/v1/begin/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => "AS-tx-123"}))
      end)

      # Expect commit to fail
      Bypass.expect(bypass, "POST", "/api/v1/commit/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(500, Jason.encode!(%{"error" => "Commit failed"}))
      end)

      assert {:error, %Error{message: "Commit failed"}} =
               Transaction.transaction(conn, fn _tx ->
                 :done
               end)
    end
  end
end
