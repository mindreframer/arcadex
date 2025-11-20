defmodule Arcadex.ExecuteTest do
  use ExUnit.Case, async: true

  alias Arcadex.{Query, Conn, Error}

  setup do
    bypass = Bypass.open()
    conn = Conn.new("http://localhost:#{bypass.port}", "testdb")
    {:ok, bypass: bypass, conn: conn}
  end

  describe "execute/5" do
    @tag :ARX002_3A_T1
    test "executes with sql language", %{bypass: bypass, conn: conn} do
      expected_result = [%{"@rid" => "#1:0", "name" => "John"}]

      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "sql"
        assert request["command"] == "SELECT FROM User"

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => expected_result}))
      end)

      assert {:ok, ^expected_result} = Query.execute(conn, "sql", "SELECT FROM User")
    end

    @tag :ARX002_3A_T2
    test "executes with sqlscript language", %{bypass: bypass, conn: conn} do
      script = "LET x = SELECT 1; RETURN $x"
      expected_result = [1]

      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "sqlscript"
        assert request["command"] == script

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => expected_result}))
      end)

      assert {:ok, ^expected_result} = Query.execute(conn, "sqlscript", script)
    end

    @tag :ARX002_3A_T3
    test "executes with cypher language", %{bypass: bypass, conn: conn} do
      cypher = "MATCH (n:User) RETURN n LIMIT 10"
      expected_result = [%{"n" => %{"name" => "John"}}]

      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "cypher"
        assert request["command"] == cypher

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => expected_result}))
      end)

      assert {:ok, ^expected_result} = Query.execute(conn, "cypher", cypher)
    end

    @tag :ARX002_3A_T5
    test "executes with params", %{bypass: bypass, conn: conn} do
      expected_result = [%{"@rid" => "#1:0", "name" => "John", "age" => 25}]

      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
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
               Query.execute(conn, "sql", "SELECT FROM User WHERE age > :age", %{age: 21})
    end

    @tag :ARX002_3A_T6
    test "executes with options", %{bypass: bypass, conn: conn} do
      expected_result = [%{"@rid" => "#1:0", "name" => "John"}]

      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "sql"
        assert request["limit"] == 100
        assert request["retries"] == 3
        assert request["serializer"] == "graph"

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => expected_result}))
      end)

      assert {:ok, ^expected_result} =
               Query.execute(conn, "sql", "SELECT FROM User", %{},
                 limit: 100,
                 retries: 3,
                 serializer: "graph"
               )
    end

    test "executes with gremlin language", %{bypass: bypass, conn: conn} do
      gremlin = "g.V().hasLabel('User').limit(10)"
      expected_result = [%{"@rid" => "#1:0", "name" => "John"}]

      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "gremlin"
        assert request["command"] == gremlin

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => expected_result}))
      end)

      assert {:ok, ^expected_result} = Query.execute(conn, "gremlin", gremlin)
    end

    test "executes with graphql language", %{bypass: bypass, conn: conn} do
      graphql = "{users(limit: 10) {name email}}"
      expected_result = [%{"users" => [%{"name" => "John", "email" => "john@example.com"}]}]

      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "graphql"
        assert request["command"] == graphql

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => expected_result}))
      end)

      assert {:ok, ^expected_result} = Query.execute(conn, "graphql", graphql)
    end

    test "executes with mongo language", %{bypass: bypass, conn: conn} do
      mongo = ~s({"collection": "User", "query": {"active": true}})
      expected_result = [%{"@rid" => "#1:0", "name" => "John", "active" => true}]

      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "mongo"
        assert request["command"] == mongo

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => expected_result}))
      end)

      assert {:ok, ^expected_result} = Query.execute(conn, "mongo", mongo)
    end

    test "works without params", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "sql"
        assert request["command"] == "SELECT FROM User"
        refute Map.has_key?(request, "params")

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert {:ok, []} = Query.execute(conn, "sql", "SELECT FROM User")
    end

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

      assert {:error, %Error{}} = Query.execute(conn, "sql", "INVALID SQL")
    end
  end

  describe "execute!/5" do
    @tag :ARX002_3A_T4
    test "raises on error", %{bypass: bypass, conn: conn} do
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

      assert_raise Error, fn ->
        Query.execute!(conn, "sql", "INVALID SQL")
      end
    end

    test "returns result on success", %{bypass: bypass, conn: conn} do
      expected_result = [%{"@rid" => "#1:0", "name" => "John"}]

      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => expected_result}))
      end)

      assert ^expected_result = Query.execute!(conn, "sql", "SELECT FROM User")
    end

    test "works with params and options", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "cypher"
        assert request["params"] == %{"name" => "John"}
        assert request["limit"] == 50

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert [] =
               Query.execute!(
                 conn,
                 "cypher",
                 "MATCH (n:User {name: $name}) RETURN n",
                 %{name: "John"},
                 limit: 50
               )
    end
  end

  describe "main module delegation" do
    test "Arcadex.execute/5 delegates correctly", %{bypass: bypass, conn: conn} do
      expected_result = [%{"@rid" => "#1:0", "name" => "John"}]

      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "cypher"

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => expected_result}))
      end)

      assert {:ok, ^expected_result} =
               Arcadex.execute(conn, "cypher", "MATCH (n:User) RETURN n")
    end

    test "Arcadex.execute!/5 delegates correctly", %{bypass: bypass, conn: conn} do
      expected_result = [%{"@rid" => "#1:0", "name" => "John"}]

      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => expected_result}))
      end)

      assert ^expected_result =
               Arcadex.execute!(conn, "gremlin", "g.V().hasLabel('User').limit(10)")
    end

    test "Arcadex.execute/5 with params and opts", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "sql"
        assert request["params"] == %{"age" => 21}
        assert request["limit"] == 10

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert {:ok, []} =
               Arcadex.execute(
                 conn,
                 "sql",
                 "SELECT FROM User WHERE age > :age",
                 %{age: 21},
                 limit: 10
               )
    end
  end
end
