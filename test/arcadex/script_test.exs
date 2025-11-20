defmodule Arcadex.ScriptTest do
  use ExUnit.Case, async: true

  alias Arcadex.{Query, Conn, Error}

  setup do
    bypass = Bypass.open()
    conn = Conn.new("http://localhost:#{bypass.port}", "testdb")
    {:ok, bypass: bypass, conn: conn}
  end

  describe "script/4" do
    @tag :ARX002_2A_T1
    test "sends sqlscript language with LET/RETURN", %{bypass: bypass, conn: conn} do
      script = """
      LET user = SELECT FROM User WHERE name = :name;
      RETURN $user
      """

      expected_result = [%{"@rid" => "#1:0", "name" => "John"}]

      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "sqlscript"
        assert request["command"] == script
        assert request["params"] == %{"name" => "John"}

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => expected_result}))
      end)

      assert {:ok, ^expected_result} = Query.script(conn, script, %{name: "John"})
    end

    @tag :ARX002_2A_T2
    test "works with params", %{bypass: bypass, conn: conn} do
      script = "LET x = SELECT FROM User WHERE age > :age; RETURN $x"

      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "sqlscript"
        assert request["params"] == %{"age" => 21}

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert {:ok, []} = Query.script(conn, script, %{age: 21})
    end

    @tag :ARX002_2A_T4
    test "handles multiple LET statements", %{bypass: bypass, conn: conn} do
      script = """
      LET user = SELECT FROM User WHERE name = :name;
      LET orders = SELECT FROM Order WHERE user = $user[0].@rid;
      LET payments = SELECT FROM Payment WHERE order IN $orders;
      RETURN { user: $user, orders: $orders, payments: $payments }
      """

      expected_result = [
        %{
          "user" => [%{"@rid" => "#1:0", "name" => "John"}],
          "orders" => [%{"@rid" => "#2:0"}],
          "payments" => [%{"@rid" => "#3:0"}]
        }
      ]

      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "sqlscript"
        assert String.contains?(request["command"], "LET user")
        assert String.contains?(request["command"], "LET orders")
        assert String.contains?(request["command"], "LET payments")
        assert String.contains?(request["command"], "RETURN")

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => expected_result}))
      end)

      assert {:ok, ^expected_result} = Query.script(conn, script, %{name: "John"})
    end

    @tag :ARX002_2A_T5
    test "returns RETURN value from script", %{bypass: bypass, conn: conn} do
      script = "LET count = SELECT count(*) as total FROM User; RETURN $count[0].total"

      expected_result = [42]

      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "sqlscript"

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => expected_result}))
      end)

      assert {:ok, [42]} = Query.script(conn, script)
    end

    test "works without params", %{bypass: bypass, conn: conn} do
      script = "LET x = SELECT 1; RETURN $x"

      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "sqlscript"
        assert request["command"] == script
        refute Map.has_key?(request, "params")

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => [1]}))
      end)

      assert {:ok, [1]} = Query.script(conn, script)
    end

    test "works with options", %{bypass: bypass, conn: conn} do
      script = "LET x = SELECT FROM User; RETURN $x"

      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "sqlscript"
        assert request["limit"] == 100
        assert request["retries"] == 3

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert {:ok, []} = Query.script(conn, script, %{}, limit: 100, retries: 3)
    end

    test "returns error on failure", %{bypass: bypass, conn: conn} do
      script = "INVALID SCRIPT"

      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(
          400,
          Jason.encode!(%{
            "error" => "Syntax error",
            "detail" => "Invalid script syntax",
            "exception" => "CommandParsingException"
          })
        )
      end)

      assert {:error, %Error{}} = Query.script(conn, script)
    end
  end

  describe "script!/4" do
    @tag :ARX002_2A_T3
    test "raises on error", %{bypass: bypass, conn: conn} do
      script = "INVALID SCRIPT"

      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(
          400,
          Jason.encode!(%{
            "error" => "Syntax error",
            "detail" => "Invalid script syntax",
            "exception" => "CommandParsingException"
          })
        )
      end)

      assert_raise Error, fn ->
        Query.script!(conn, script)
      end
    end

    test "returns result on success", %{bypass: bypass, conn: conn} do
      script = "LET x = SELECT 1; RETURN $x"
      expected_result = [1]

      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => expected_result}))
      end)

      assert ^expected_result = Query.script!(conn, script)
    end

    test "works with params and options", %{bypass: bypass, conn: conn} do
      script = "LET x = SELECT FROM User WHERE age > :age; RETURN $x"

      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["params"] == %{"age" => 30}
        assert request["limit"] == 50

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert [] = Query.script!(conn, script, %{age: 30}, limit: 50)
    end
  end

  describe "main module delegation" do
    test "Arcadex.script/4 delegates correctly", %{bypass: bypass, conn: conn} do
      script = "LET x = SELECT 1; RETURN $x"

      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "sqlscript"

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => [1]}))
      end)

      assert {:ok, [1]} = Arcadex.script(conn, script)
    end

    test "Arcadex.script!/4 delegates correctly", %{bypass: bypass, conn: conn} do
      script = "LET x = SELECT 1; RETURN $x"

      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => [1]}))
      end)

      assert [1] = Arcadex.script!(conn, script)
    end

    test "Arcadex.script/4 with params and opts", %{bypass: bypass, conn: conn} do
      script = "LET x = SELECT FROM User WHERE name = :name; RETURN $x"

      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["params"] == %{"name" => "John"}
        assert request["limit"] == 10

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert {:ok, []} = Arcadex.script(conn, script, %{name: "John"}, limit: 10)
    end
  end
end
