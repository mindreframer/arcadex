defmodule Arcadex.QueryOptionsTest do
  use ExUnit.Case, async: true

  alias Arcadex.{Conn, Query}

  setup do
    bypass = Bypass.open()
    conn = Conn.new("http://localhost:#{bypass.port}", "testdb")
    {:ok, bypass: bypass, conn: conn}
  end

  describe "build_body/4" do
    @tag :ARX002_1A_T1
    test "builds body with minimal params" do
      body = Query.build_body("sql", "SELECT FROM User", %{}, [])

      assert body == %{language: "sql", command: "SELECT FROM User"}
    end

    @tag :ARX002_1A_T2
    test "builds body with all options" do
      body =
        Query.build_body("sql", "SELECT FROM User", %{name: "John"},
          limit: 100,
          retries: 3,
          serializer: "graph",
          await_response: false
        )

      assert body == %{
               language: "sql",
               command: "SELECT FROM User",
               params: %{name: "John"},
               limit: 100,
               retries: 3,
               serializer: "graph",
               awaitResponse: false
             }
    end

    @tag :ARX002_1A_T3
    test "omits empty params map" do
      body = Query.build_body("sql", "SELECT FROM User", %{}, limit: 100)

      refute Map.has_key?(body, :params)
      assert body[:limit] == 100
    end

    test "handles different languages" do
      body = Query.build_body("cypher", "MATCH (n) RETURN n", %{}, [])

      assert body[:language] == "cypher"
      assert body[:command] == "MATCH (n) RETURN n"
    end

    test "only adds awaitResponse when explicitly false" do
      # Not set - should not be present
      body1 = Query.build_body("sql", "SELECT 1", %{}, [])
      refute Map.has_key?(body1, :awaitResponse)

      # Set to true - should not be present (server default is true)
      body2 = Query.build_body("sql", "SELECT 1", %{}, await_response: true)
      refute Map.has_key?(body2, :awaitResponse)

      # Set to false - should be present
      body3 = Query.build_body("sql", "SELECT 1", %{}, await_response: false)
      assert body3[:awaitResponse] == false
    end
  end

  describe "query/4 with options" do
    @tag :ARX002_1A_T4
    test "sends limit option in request body", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "sql"
        assert request["command"] == "SELECT FROM User"
        assert request["limit"] == 100

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert {:ok, []} = Query.query(conn, "SELECT FROM User", %{}, limit: 100)
    end

    @tag :ARX002_1A_T6
    test "sends serializer option in request body", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "sql"
        assert request["command"] == "SELECT FROM User"
        assert request["serializer"] == "graph"

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert {:ok, []} = Query.query(conn, "SELECT FROM User", %{}, serializer: "graph")
    end

    test "works with both params and options", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["command"] == "SELECT FROM User WHERE age > :age"
        assert request["params"] == %{"age" => 21}
        assert request["limit"] == 50
        assert request["serializer"] == "record"

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert {:ok, []} =
               Query.query(conn, "SELECT FROM User WHERE age > :age", %{age: 21},
                 limit: 50,
                 serializer: "record"
               )
    end

    test "backward compatible with no options", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        # Should only have language and command
        assert request == %{"language" => "sql", "command" => "SELECT FROM User"}

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert {:ok, []} = Query.query(conn, "SELECT FROM User")
    end
  end

  describe "command/4 with options" do
    @tag :ARX002_1A_T5
    test "sends retries option in request body", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "sql"
        assert request["command"] == "INSERT INTO User SET name = 'John'"
        assert request["retries"] == 3

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert {:ok, []} =
               Query.command(conn, "INSERT INTO User SET name = 'John'", %{}, retries: 3)
    end

    test "sends multiple options in request body", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["limit"] == 10
        assert request["retries"] == 5
        assert request["serializer"] == "studio"

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert {:ok, []} =
               Query.command(conn, "INSERT INTO User SET name = 'John'", %{},
                 limit: 10,
                 retries: 5,
                 serializer: "studio"
               )
    end

    test "backward compatible with no options", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        # Should only have language and command
        assert request == %{
                 "language" => "sql",
                 "command" => "INSERT INTO User SET name = 'John'"
               }

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert {:ok, []} = Query.command(conn, "INSERT INTO User SET name = 'John'")
    end
  end

  describe "query!/4 with options" do
    test "works with options", %{bypass: bypass, conn: conn} do
      expected_result = [%{"@rid" => "#1:0", "name" => "John"}]

      Bypass.expect(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["limit"] == 1

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => expected_result}))
      end)

      assert ^expected_result = Query.query!(conn, "SELECT FROM User", %{}, limit: 1)
    end
  end

  describe "command!/4 with options" do
    test "works with options", %{bypass: bypass, conn: conn} do
      expected_result = [%{"@rid" => "#1:0", "name" => "John"}]

      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["retries"] == 2

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => expected_result}))
      end)

      assert ^expected_result =
               Query.command!(conn, "INSERT INTO User SET name = 'John'", %{}, retries: 2)
    end
  end

  describe "main module delegation" do
    test "Arcadex.query/4 delegates with options", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["limit"] == 25

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert {:ok, []} = Arcadex.query(conn, "SELECT FROM User", %{}, limit: 25)
    end

    test "Arcadex.command/4 delegates with options", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["retries"] == 3

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert {:ok, []} =
               Arcadex.command(conn, "INSERT INTO User SET name = 'John'", %{}, retries: 3)
    end
  end
end
