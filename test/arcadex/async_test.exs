defmodule Arcadex.AsyncTest do
  use ExUnit.Case, async: true

  alias Arcadex.{Query, Conn, Error}

  setup do
    bypass = Bypass.open()
    conn = Conn.new("http://localhost:#{bypass.port}", "testdb")
    {:ok, bypass: bypass, conn: conn}
  end

  describe "command_async/4" do
    @tag :ARX002_4A_T1
    test "returns :ok on success", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert :ok = Query.command_async(conn, "INSERT INTO Log SET event = 'audit'")
    end

    @tag :ARX002_4A_T2
    test "sends awaitResponse false", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "sql"
        assert request["command"] == "INSERT INTO Log SET event = 'audit'"
        assert request["awaitResponse"] == false

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert :ok = Query.command_async(conn, "INSERT INTO Log SET event = 'audit'")
    end

    @tag :ARX002_4A_T3
    test "works with params", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "sql"
        assert request["command"] == "INSERT INTO Log SET event = :event"
        assert request["params"] == %{"event" => "login"}
        assert request["awaitResponse"] == false

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert :ok =
               Query.command_async(
                 conn,
                 "INSERT INTO Log SET event = :event",
                 %{event: "login"}
               )
    end

    @tag :ARX002_4A_T4
    test "returns error on failure", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(
          400,
          Jason.encode!(%{
            "error" => "Syntax error",
            "detail" => "Invalid syntax",
            "exception" => "CommandParsingException"
          })
        )
      end)

      assert {:error, %Error{}} = Query.command_async(conn, "INVALID SQL")
    end

    test "works with options", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "sql"
        assert request["retries"] == 3
        assert request["awaitResponse"] == false

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert :ok =
               Query.command_async(
                 conn,
                 "INSERT INTO Log SET event = 'audit'",
                 %{},
                 retries: 3
               )
    end

    test "omits params when empty", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "sql"
        assert request["command"] == "INSERT INTO Log SET event = 'audit'"
        assert request["awaitResponse"] == false
        refute Map.has_key?(request, "params")

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert :ok = Query.command_async(conn, "INSERT INTO Log SET event = 'audit'")
    end
  end

  describe "main module delegation" do
    test "Arcadex.command_async/4 delegates correctly", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["awaitResponse"] == false

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert :ok = Arcadex.command_async(conn, "INSERT INTO Log SET event = 'audit'")
    end

    test "Arcadex.command_async/4 with params", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["params"] == %{"event" => "login"}
        assert request["awaitResponse"] == false

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert :ok =
               Arcadex.command_async(
                 conn,
                 "INSERT INTO Log SET event = :event",
                 %{event: "login"}
               )
    end

    test "Arcadex.command_async/4 with options", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["retries"] == 3
        assert request["awaitResponse"] == false

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert :ok =
               Arcadex.command_async(
                 conn,
                 "INSERT INTO Log SET event = 'audit'",
                 %{},
                 retries: 3
               )
    end
  end
end
