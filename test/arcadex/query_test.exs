defmodule Arcadex.QueryTest do
  use ExUnit.Case, async: true

  alias Arcadex.{Query, Conn, Error}

  setup do
    bypass = Bypass.open()
    conn = Conn.new("http://localhost:#{bypass.port}", "testdb")
    {:ok, bypass: bypass, conn: conn}
  end

  describe "query/3" do
    @tag :ARX001_2A_T1
    test "executes SELECT and returns results", %{bypass: bypass, conn: conn} do
      expected_result = [%{"@rid" => "#1:0", "name" => "John", "active" => true}]

      Bypass.expect(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "sql"
        assert request["command"] == "SELECT FROM User WHERE active = true"
        refute Map.has_key?(request, "params")

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => expected_result}))
      end)

      assert {:ok, ^expected_result} =
               Query.query(conn, "SELECT FROM User WHERE active = true")
    end

    @tag :ARX001_2A_T2
    test "executes query with parameters", %{bypass: bypass, conn: conn} do
      expected_result = [%{"@rid" => "#1:0", "name" => "John", "age" => 25}]

      Bypass.expect(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "sql"
        assert request["command"] == "SELECT FROM User WHERE age > :age"
        assert request["params"] == %{"age" => 21}

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => expected_result}))
      end)

      assert {:ok, ^expected_result} =
               Query.query(conn, "SELECT FROM User WHERE age > :age", %{age: 21})
    end

    @tag :ARX001_2A_T7
    test "returns {:error, error} on failure", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(
          500,
          Jason.encode!(%{
            "error" => "Database 'testdb' does not exist",
            "detail" => "Cannot open database"
          })
        )
      end)

      assert {:error,
              %Error{
                status: 500,
                message: "Database 'testdb' does not exist",
                detail: "Cannot open database"
              }} = Query.query(conn, "SELECT FROM User")
    end

    test "returns empty list for no results", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert {:ok, []} = Query.query(conn, "SELECT FROM User WHERE 1 = 0")
    end
  end

  describe "query!/3" do
    @tag :ARX001_2A_T3
    test "raises on error", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(400, Jason.encode!(%{"error" => "Syntax error", "detail" => "near 'INVALID'"}))
      end)

      assert_raise Error, "Syntax error: near 'INVALID'", fn ->
        Query.query!(conn, "INVALID SQL")
      end
    end

    test "returns result on success", %{bypass: bypass, conn: conn} do
      expected_result = [%{"@rid" => "#1:0", "name" => "John"}]

      Bypass.expect(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => expected_result}))
      end)

      assert ^expected_result = Query.query!(conn, "SELECT FROM User")
    end
  end

  describe "command/3" do
    @tag :ARX001_2A_T4
    test "executes INSERT and returns result", %{bypass: bypass, conn: conn} do
      expected_result = [%{"@rid" => "#1:0", "name" => "John"}]

      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "sql"
        assert request["command"] == "INSERT INTO User SET name = 'John'"
        refute Map.has_key?(request, "params")

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => expected_result}))
      end)

      assert {:ok, ^expected_result} = Query.command(conn, "INSERT INTO User SET name = 'John'")
    end

    @tag :ARX001_2A_T5
    test "executes DDL command", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "sql"
        assert request["command"] == "CREATE VERTEX TYPE Person"

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert {:ok, []} = Query.command(conn, "CREATE VERTEX TYPE Person")
    end

    test "executes command with parameters", %{bypass: bypass, conn: conn} do
      expected_result = [%{"@rid" => "#1:1", "name" => "Jane", "email" => "jane@example.com"}]

      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "sql"
        assert request["command"] == "INSERT INTO User SET name = :name, email = :email"
        assert request["params"] == %{"name" => "Jane", "email" => "jane@example.com"}

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => expected_result}))
      end)

      assert {:ok, ^expected_result} =
               Query.command(
                 conn,
                 "INSERT INTO User SET name = :name, email = :email",
                 %{name: "Jane", email: "jane@example.com"}
               )
    end

    test "returns error on failure", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(
          400,
          Jason.encode!(%{"error" => "Type 'User' does not exist", "detail" => "Unknown type"})
        )
      end)

      assert {:error,
              %Error{
                status: 400,
                message: "Type 'User' does not exist",
                detail: "Unknown type"
              }} = Query.command(conn, "INSERT INTO User SET name = 'John'")
    end
  end

  describe "command!/3" do
    @tag :ARX001_2A_T6
    test "raises on error", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(400, Jason.encode!(%{"error" => "Invalid command"}))
      end)

      assert_raise Error, "Invalid command", fn ->
        Query.command!(conn, "INVALID COMMAND")
      end
    end

    test "returns result on success", %{bypass: bypass, conn: conn} do
      expected_result = [%{"@rid" => "#1:0", "name" => "John"}]

      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => expected_result}))
      end)

      assert ^expected_result = Query.command!(conn, "INSERT INTO User SET name = 'John'")
    end
  end

  describe "session handling" do
    test "uses session_id from conn for transactions", %{bypass: bypass, conn: conn} do
      conn_with_session = Conn.with_session(conn, "tx-session-123")

      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        assert Plug.Conn.get_req_header(http_conn, "arcadedb-session-id") == ["tx-session-123"]

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert {:ok, []} = Query.command(conn_with_session, "INSERT INTO User SET name = 'John'")
    end
  end
end
