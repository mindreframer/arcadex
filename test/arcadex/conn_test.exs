defmodule Arcadex.ConnTest do
  use ExUnit.Case, async: true

  alias Arcadex.Conn

  describe "ARX001_1A: Conn struct tests" do
    test "ARX001_1A_T1: new/3 creates struct with defaults" do
      conn = Conn.new("http://localhost:2480", "mydb")

      assert conn.base_url == "http://localhost:2480"
      assert conn.database == "mydb"
      assert conn.auth == {"root", "root"}
      assert conn.session_id == nil
      assert conn.finch_name == Arcadex.Finch
    end

    test "ARX001_1A_T1: new/3 trims trailing slash from base_url" do
      conn = Conn.new("http://localhost:2480/", "mydb")

      assert conn.base_url == "http://localhost:2480"
    end

    test "ARX001_1A_T2: new/3 with custom auth and finch" do
      conn =
        Conn.new("http://localhost:2480", "mydb",
          auth: {"admin", "secret"},
          finch: MyApp.CustomFinch
        )

      assert conn.auth == {"admin", "secret"}
      assert conn.finch_name == MyApp.CustomFinch
    end

    test "ARX001_1A_T3: with_database/2 returns new conn with different database" do
      conn = Conn.new("http://localhost:2480", "db1", auth: {"user", "pass"})
      conn2 = Conn.with_database(conn, "db2")

      # Database changed
      assert conn2.database == "db2"
      # Other fields preserved
      assert conn2.base_url == "http://localhost:2480"
      assert conn2.auth == {"user", "pass"}
      assert conn2.finch_name == Arcadex.Finch
      # Session cleared
      assert conn2.session_id == nil
    end

    test "ARX001_1A_T3: with_database/2 clears session_id" do
      conn = Conn.new("http://localhost:2480", "db1")
      conn = %{conn | session_id: "existing-session"}
      conn2 = Conn.with_database(conn, "db2")

      assert conn2.session_id == nil
    end

    test "ARX001_1A_T4: with_session/2 adds session_id" do
      conn = Conn.new("http://localhost:2480", "mydb")
      conn_with_session = Conn.with_session(conn, "session-123")

      assert conn_with_session.session_id == "session-123"
      # Other fields preserved
      assert conn_with_session.base_url == "http://localhost:2480"
      assert conn_with_session.database == "mydb"
    end
  end
end
