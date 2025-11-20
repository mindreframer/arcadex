defmodule Arcadex.ServerTest do
  use ExUnit.Case, async: true

  alias Arcadex.{Server, Conn, Error}

  setup do
    bypass = Bypass.open()
    conn = Conn.new("http://localhost:#{bypass.port}", "testdb")
    {:ok, bypass: bypass, conn: conn}
  end

  describe "create_database/2" do
    @tag :ARX001_4A_T1
    test "creates database successfully", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/server", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["command"] == "create database newdb"

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => "ok"}))
      end)

      assert :ok = Server.create_database(conn, "newdb")
    end

    test "returns error when database already exists", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/server", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(
          500,
          Jason.encode!(%{
            "error" => "Database 'existing' already exists",
            "detail" => "Cannot create"
          })
        )
      end)

      assert {:error, %Error{status: 500, message: "Database 'existing' already exists"}} =
               Server.create_database(conn, "existing")
    end
  end

  describe "create_database!/2" do
    @tag :ARX001_4A_T2
    test "raises on error", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/server", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(
          500,
          Jason.encode!(%{
            "error" => "Database 'existing' already exists"
          })
        )
      end)

      assert_raise Error, "Database 'existing' already exists", fn ->
        Server.create_database!(conn, "existing")
      end
    end

    test "returns :ok on success", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/server", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => "ok"}))
      end)

      assert :ok = Server.create_database!(conn, "newdb")
    end
  end

  describe "drop_database/2" do
    @tag :ARX001_4A_T3
    test "drops database successfully", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/server", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["command"] == "drop database olddb"

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => "ok"}))
      end)

      assert :ok = Server.drop_database(conn, "olddb")
    end

    test "returns error when database does not exist", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/server", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(
          500,
          Jason.encode!(%{
            "error" => "Database 'nonexistent' does not exist",
            "detail" => "Cannot drop"
          })
        )
      end)

      assert {:error, %Error{status: 500, message: "Database 'nonexistent' does not exist"}} =
               Server.drop_database(conn, "nonexistent")
    end
  end

  describe "drop_database!/2" do
    @tag :ARX001_4A_T4
    test "raises on error", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/server", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(
          500,
          Jason.encode!(%{
            "error" => "Database 'nonexistent' does not exist"
          })
        )
      end)

      assert_raise Error, "Database 'nonexistent' does not exist", fn ->
        Server.drop_database!(conn, "nonexistent")
      end
    end

    test "returns :ok on success", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/server", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => "ok"}))
      end)

      assert :ok = Server.drop_database!(conn, "olddb")
    end
  end

  describe "database_exists?/2" do
    @tag :ARX001_4A_T5
    test "returns true when database exists", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "GET", "/api/v1/exists/mydb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => true}))
      end)

      assert Server.database_exists?(conn, "mydb") == true
    end

    @tag :ARX001_4A_T6
    test "returns false when database does not exist", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "GET", "/api/v1/exists/nonexistent", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => false}))
      end)

      assert Server.database_exists?(conn, "nonexistent") == false
    end

    test "returns false on error", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "GET", "/api/v1/exists/broken", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(500, Jason.encode!(%{"error" => "Server error"}))
      end)

      assert Server.database_exists?(conn, "broken") == false
    end
  end
end
