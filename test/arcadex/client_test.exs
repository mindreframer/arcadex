defmodule Arcadex.ClientTest do
  use ExUnit.Case, async: true

  alias Arcadex.{Client, Conn, Error}

  # Use Bypass to mock HTTP responses
  setup do
    bypass = Bypass.open()
    conn = Conn.new("http://localhost:#{bypass.port}", "testdb")
    {:ok, bypass: bypass, conn: conn}
  end

  describe "post/3" do
    @tag :ARX001_1B_T1
    test "sends correct headers with basic auth", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        assert Plug.Conn.get_req_header(http_conn, "authorization") == [
                 "Basic " <> Base.encode64("root:root")
               ]

        assert Plug.Conn.get_req_header(http_conn, "content-type") == ["application/json"]

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert {:ok, %{"result" => []}} =
               Client.post(conn, "/api/v1/query/testdb", %{language: "sql", command: "SELECT 1"})
    end

    @tag :ARX001_1B_T2
    test "includes session_id header when present", %{bypass: bypass, conn: conn} do
      conn_with_session = Conn.with_session(conn, "sess-abc-123")

      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        assert Plug.Conn.get_req_header(http_conn, "arcadedb-session-id") == ["sess-abc-123"]

        assert Plug.Conn.get_req_header(http_conn, "authorization") == [
                 "Basic " <> Base.encode64("root:root")
               ]

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert {:ok, _} =
               Client.post(conn_with_session, "/api/v1/command/testdb", %{
                 language: "sql",
                 command: "INSERT INTO Test SET name = 'foo'"
               })
    end

    @tag :ARX001_1B_T3
    test "handles success response with result", %{bypass: bypass, conn: conn} do
      expected_result = [%{"@rid" => "#1:0", "name" => "test"}]

      Bypass.expect(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => expected_result}))
      end)

      assert {:ok, %{"result" => ^expected_result}} =
               Client.post(conn, "/api/v1/query/testdb", %{
                 language: "sql",
                 command: "SELECT FROM Test"
               })
    end

    @tag :ARX001_1B_T4
    test "handles error response with detail", %{bypass: bypass, conn: conn} do
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
              }} =
               Client.post(conn, "/api/v1/query/testdb", %{language: "sql", command: "SELECT 1"})
    end

    test "handles error response without detail", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(400, Jason.encode!(%{"error" => "Syntax error"}))
      end)

      assert {:error, %Error{status: 400, message: "Syntax error", detail: nil}} =
               Client.post(conn, "/api/v1/query/testdb", %{language: "sql", command: "INVALID"})
    end

    test "handles non-standard error response", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(502, Jason.encode!(%{"unexpected" => "response"}))
      end)

      {:error, error} =
        Client.post(conn, "/api/v1/query/testdb", %{language: "sql", command: "SELECT 1"})

      assert error.status == 502
      assert error.message == "HTTP 502"
    end

    test "handles connection failure", %{conn: conn} do
      # Use a port that's not listening
      conn = %{conn | base_url: "http://localhost:1"}

      {:error, error} =
        Client.post(conn, "/api/v1/query/testdb", %{language: "sql", command: "SELECT 1"})

      assert error.message == "Connection failed"
    end

    test "sends custom auth credentials", %{bypass: bypass, conn: _conn} do
      conn = Conn.new("http://localhost:#{bypass.port}", "testdb", auth: {"admin", "secret123"})

      Bypass.expect(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        assert Plug.Conn.get_req_header(http_conn, "authorization") == [
                 "Basic " <> Base.encode64("admin:secret123")
               ]

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert {:ok, _} =
               Client.post(conn, "/api/v1/query/testdb", %{language: "sql", command: "SELECT 1"})
    end
  end

  describe "get/2" do
    @tag :ARX001_1B_T5
    test "sends auth header", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "GET", "/api/v1/exists/testdb", fn http_conn ->
        assert Plug.Conn.get_req_header(http_conn, "authorization") == [
                 "Basic " <> Base.encode64("root:root")
               ]

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => true}))
      end)

      assert {:ok, %{"result" => true}} = Client.get(conn, "/api/v1/exists/testdb")
    end

    test "handles success response", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "GET", "/api/v1/exists/mydb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => false}))
      end)

      assert {:ok, %{"result" => false}} = Client.get(conn, "/api/v1/exists/mydb")
    end

    test "handles error response", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "GET", "/api/v1/exists/baddb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(
          500,
          Jason.encode!(%{"error" => "Internal error", "detail" => "Something went wrong"})
        )
      end)

      assert {:error,
              %Error{status: 500, message: "Internal error", detail: "Something went wrong"}} =
               Client.get(conn, "/api/v1/exists/baddb")
    end

    test "handles connection failure", %{conn: conn} do
      conn = %{conn | base_url: "http://localhost:1"}

      {:error, error} = Client.get(conn, "/api/v1/exists/testdb")

      assert error.message == "Connection failed"
    end
  end
end
