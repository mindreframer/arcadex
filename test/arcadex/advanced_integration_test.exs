defmodule Arcadex.AdvancedIntegrationTest do
  @moduledoc """
  Integration tests for ARX002 advanced query features.

  These tests verify that script, execute, command_async, and query options
  work correctly together in integrated scenarios.
  """
  use ExUnit.Case, async: true

  alias Arcadex.Error

  setup do
    bypass = Bypass.open()
    conn = Arcadex.connect("http://localhost:#{bypass.port}", "testdb")
    {:ok, bypass: bypass, conn: conn}
  end

  describe "ARX002_5A_T1: script with real database operations" do
    @tag :ARX002_5A_T1
    test "executes script with multiple LET statements and RETURN", %{bypass: bypass, conn: conn} do
      # Simulate a real workflow: create type, insert data, query with script
      script = """
      LET user = SELECT FROM User WHERE name = :name;
      LET orders = SELECT FROM Order WHERE user = $user[0].@rid;
      LET total = SELECT sum(amount) as total FROM Order WHERE user = $user[0].@rid;
      RETURN { user: $user, orders: $orders, total: $total }
      """

      expected_result = [
        %{
          "user" => [%{"@rid" => "#10:0", "name" => "John", "email" => "john@example.com"}],
          "orders" => [
            %{"@rid" => "#11:0", "amount" => 100},
            %{"@rid" => "#11:1", "amount" => 200}
          ],
          "total" => [%{"total" => 300}]
        }
      ]

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

      assert {:ok, result} = Arcadex.script(conn, script, %{name: "John"})
      assert [%{"user" => [user], "orders" => orders, "total" => [%{"total" => 300}]}] = result
      assert user["name"] == "John"
      assert length(orders) == 2
    end

    @tag :ARX002_5A_T1
    test "script handles complex RETURN expressions", %{bypass: bypass, conn: conn} do
      script = """
      LET count = SELECT count(*) as c FROM User;
      LET active = SELECT count(*) as c FROM User WHERE active = true;
      RETURN { total: $count[0].c, active: $active[0].c, inactive: $count[0].c - $active[0].c }
      """

      expected_result = [%{"total" => 100, "active" => 75, "inactive" => 25}]

      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "sqlscript"

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => expected_result}))
      end)

      assert {:ok, [%{"total" => 100, "active" => 75, "inactive" => 25}]} =
               Arcadex.script(conn, script)
    end

    @tag :ARX002_5A_T1
    test "script with options passes limit and retries", %{bypass: bypass, conn: conn} do
      script = "LET x = SELECT FROM User; RETURN $x"

      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "sqlscript"
        assert request["limit"] == 50
        assert request["retries"] == 3

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert {:ok, []} = Arcadex.script(conn, script, %{}, limit: 50, retries: 3)
    end
  end

  describe "ARX002_5A_T2: execute with multiple languages" do
    @tag :ARX002_5A_T2
    test "executes SQL, Cypher, and Gremlin in sequence", %{bypass: bypass, conn: conn} do
      # Track which requests have been made
      agent = start_supervised!({Agent, fn -> [] end})

      Bypass.stub(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        # Record the language used
        Agent.update(agent, fn langs -> [request["language"] | langs] end)

        result =
          case request["language"] do
            "sql" -> [%{"@rid" => "#1:0", "name" => "John"}]
            "cypher" -> [%{"n" => %{"name" => "John"}}]
            "gremlin" -> [%{"name" => "John"}]
            _ -> []
          end

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => result}))
      end)

      # Execute SQL
      assert {:ok, [%{"@rid" => "#1:0", "name" => "John"}]} =
               Arcadex.execute(conn, "sql", "SELECT FROM User WHERE name = :name", %{name: "John"})

      # Execute Cypher
      assert {:ok, [%{"n" => %{"name" => "John"}}]} =
               Arcadex.execute(conn, "cypher", "MATCH (n:User {name: $name}) RETURN n", %{
                 name: "John"
               })

      # Execute Gremlin
      assert {:ok, [%{"name" => "John"}]} =
               Arcadex.execute(conn, "gremlin", "g.V().has('User', 'name', name).valueMap()", %{
                 name: "John"
               })

      # Verify all three languages were used
      languages = Agent.get(agent, & &1) |> Enum.reverse()
      assert languages == ["sql", "cypher", "gremlin"]
    end

    @tag :ARX002_5A_T2
    test "execute with different languages maintains correct params", %{
      bypass: bypass,
      conn: conn
    } do
      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        # Verify params are passed correctly regardless of language
        assert request["language"] == "graphql"
        assert request["params"] == %{"limit" => 10, "offset" => 5}

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(
          200,
          Jason.encode!(%{"result" => [%{"users" => [%{"name" => "John"}]}]})
        )
      end)

      query = "{users(limit: $limit, offset: $offset) {name}}"

      assert {:ok, [%{"users" => [%{"name" => "John"}]}]} =
               Arcadex.execute(conn, "graphql", query, %{limit: 10, offset: 5})
    end

    @tag :ARX002_5A_T2
    test "execute with options works for all languages", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["language"] == "mongo"
        assert request["limit"] == 100
        assert request["serializer"] == "record"

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      mongo_query = ~s({"collection": "User", "query": {"active": true}})

      assert {:ok, []} =
               Arcadex.execute(conn, "mongo", mongo_query, %{}, limit: 100, serializer: "record")
    end
  end

  describe "ARX002_5A_T3: options affect query results" do
    @tag :ARX002_5A_T3
    test "limit option restricts number of results", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        # Verify limit is passed
        limit = request["limit"]
        assert limit == 5

        # Return exactly the limited number of results
        results = for i <- 1..limit, do: %{"@rid" => "#1:#{i}", "name" => "User#{i}"}

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => results}))
      end)

      assert {:ok, users} = Arcadex.query(conn, "SELECT FROM User", %{}, limit: 5)
      assert length(users) == 5
    end

    @tag :ARX002_5A_T3
    test "serializer option changes result format", %{bypass: bypass, conn: conn} do
      # Test graph serializer returns edges and vertices
      Bypass.expect(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["serializer"] == "graph"

        # Return graph-formatted result
        result = %{
          "vertices" => [
            %{"@rid" => "#1:0", "@type" => "User", "name" => "John"},
            %{"@rid" => "#1:1", "@type" => "User", "name" => "Jane"}
          ],
          "edges" => [
            %{"@rid" => "#2:0", "@type" => "FOLLOWS", "in" => "#1:1", "out" => "#1:0"}
          ]
        }

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => [result]}))
      end)

      assert {:ok, [graph]} = Arcadex.query(conn, "SELECT FROM User", %{}, serializer: "graph")
      assert Map.has_key?(graph, "vertices")
      assert Map.has_key?(graph, "edges")
    end

    @tag :ARX002_5A_T3
    test "retries option is passed to server", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        # Verify retries option is set
        assert request["retries"] == 5

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => [%{"@rid" => "#1:0"}]}))
      end)

      assert {:ok, [%{"@rid" => "#1:0"}]} =
               Arcadex.command(conn, "INSERT INTO User SET name = 'John'", %{}, retries: 5)
    end

    @tag :ARX002_5A_T3
    test "multiple options work together", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        # Verify all options are set
        assert request["limit"] == 10
        assert request["retries"] == 3
        assert request["serializer"] == "studio"

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert {:ok, []} =
               Arcadex.command(conn, "INSERT INTO User SET name = 'John'", %{},
                 limit: 10,
                 retries: 3,
                 serializer: "studio"
               )
    end
  end

  describe "ARX002_5A_T4: async command executes on server" do
    @tag :ARX002_5A_T4
    test "command_async sends awaitResponse false and returns :ok", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        # Verify async flag is set
        assert request["awaitResponse"] == false
        assert request["language"] == "sql"
        assert request["command"] == "INSERT INTO Log SET event = 'audit', timestamp = sysdate()"

        # Server returns immediately without result
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert :ok =
               Arcadex.command_async(
                 conn,
                 "INSERT INTO Log SET event = 'audit', timestamp = sysdate()"
               )
    end

    @tag :ARX002_5A_T4
    test "command_async with params maintains awaitResponse false", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        assert request["awaitResponse"] == false
        assert request["params"] == %{"event" => "user_login", "user_id" => "123"}

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => []}))
      end)

      assert :ok =
               Arcadex.command_async(
                 conn,
                 "INSERT INTO Log SET event = :event, user_id = :user_id",
                 %{event: "user_login", user_id: "123"}
               )
    end

    @tag :ARX002_5A_T4
    test "command_async with options preserves awaitResponse false", %{
      bypass: bypass,
      conn: conn
    } do
      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        # Verify awaitResponse is false even with other options
        assert request["awaitResponse"] == false
        assert request["retries"] == 3

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

    @tag :ARX002_5A_T4
    test "command_async returns error on failure", %{bypass: bypass, conn: conn} do
      Bypass.expect(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(
          400,
          Jason.encode!(%{
            "error" => "Type 'Log' not found",
            "detail" => "Type does not exist in schema",
            "exception" => "SchemaException"
          })
        )
      end)

      assert {:error, %Error{message: "Type 'Log' not found"}} =
               Arcadex.command_async(conn, "INSERT INTO Log SET event = 'audit'")
    end
  end

  describe "integrated workflow" do
    test "full workflow: create, query, update, async log", %{bypass: bypass, conn: conn} do
      # Track call order
      agent = start_supervised!({Agent, fn -> [] end})

      Bypass.stub(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        # Record the operation
        op =
          cond do
            String.contains?(request["command"], "INSERT INTO User") -> :create
            String.contains?(request["command"], "UPDATE") -> :update
            String.contains?(request["command"], "INSERT INTO Log") -> :log
            true -> :other
          end

        Agent.update(agent, fn ops -> [op | ops] end)

        result =
          case op do
            :create -> [%{"@rid" => "#1:0", "name" => "John"}]
            :update -> [%{"@rid" => "#1:0", "name" => "John", "active" => true}]
            :log -> []
            _ -> []
          end

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => result}))
      end)

      Bypass.stub(bypass, "POST", "/api/v1/query/testdb", fn http_conn ->
        Agent.update(agent, fn ops -> [:query | ops] end)

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(
          200,
          Jason.encode!(%{"result" => [%{"@rid" => "#1:0", "name" => "John"}]})
        )
      end)

      # 1. Create user
      assert {:ok, [user]} = Arcadex.command(conn, "INSERT INTO User SET name = 'John'")
      assert user["name"] == "John"

      # 2. Query user
      assert {:ok, [found]} =
               Arcadex.query(conn, "SELECT FROM User WHERE name = :name", %{name: "John"})

      assert found["name"] == "John"

      # 3. Update user
      assert {:ok, [updated]} =
               Arcadex.command(conn, "UPDATE User SET active = true WHERE @rid = :rid", %{
                 rid: "#1:0"
               })

      assert updated["active"] == true

      # 4. Log action asynchronously
      assert :ok = Arcadex.command_async(conn, "INSERT INTO Log SET event = 'user_updated'")

      # Verify operations executed in order
      ops = Agent.get(agent, & &1) |> Enum.reverse()
      assert ops == [:create, :query, :update, :log]
    end

    test "script and execute work together in workflow", %{bypass: bypass, conn: conn} do
      call_count = start_supervised!({Agent, fn -> 0 end})

      Bypass.stub(bypass, "POST", "/api/v1/command/testdb", fn http_conn ->
        {:ok, body, http_conn} = Plug.Conn.read_body(http_conn)
        request = Jason.decode!(body)

        Agent.update(call_count, &(&1 + 1))

        result =
          case request["language"] do
            "sqlscript" ->
              [%{"total" => 100, "active" => 75}]

            "cypher" ->
              [%{"n" => %{"name" => "John"}}]

            _ ->
              []
          end

        http_conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, Jason.encode!(%{"result" => result}))
      end)

      # Use script for complex aggregation
      script = """
      LET total = SELECT count(*) as c FROM User;
      LET active = SELECT count(*) as c FROM User WHERE active = true;
      RETURN { total: $total[0].c, active: $active[0].c }
      """

      assert {:ok, [%{"total" => 100, "active" => 75}]} = Arcadex.script(conn, script)

      # Use execute for Cypher graph traversal
      assert {:ok, [%{"n" => %{"name" => "John"}}]} =
               Arcadex.execute(
                 conn,
                 "cypher",
                 "MATCH (n:User {name: $name}) RETURN n",
                 %{name: "John"}
               )

      # Both calls completed
      assert Agent.get(call_count, & &1) == 2
    end
  end
end
