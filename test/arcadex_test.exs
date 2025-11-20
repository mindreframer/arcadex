defmodule ArcadexTest do
  use ExUnit.Case

  describe "connect/3" do
    test "creates connection with defaults" do
      conn = Arcadex.connect("http://localhost:2480", "mydb")

      assert conn.base_url == "http://localhost:2480"
      assert conn.database == "mydb"
      assert conn.auth == {"root", "root"}
      assert conn.session_id == nil
      assert conn.finch_name == Arcadex.Finch
    end

    test "creates connection with custom options" do
      conn =
        Arcadex.connect("http://localhost:2480", "mydb",
          auth: {"admin", "secret"},
          finch: MyApp.Finch
        )

      assert conn.auth == {"admin", "secret"}
      assert conn.finch_name == MyApp.Finch
    end
  end

  describe "with_database/2" do
    test "switches database" do
      conn = Arcadex.connect("http://localhost:2480", "db1")
      conn2 = Arcadex.with_database(conn, "db2")

      assert conn2.database == "db2"
      assert conn2.base_url == conn.base_url
      assert conn2.auth == conn.auth
    end
  end
end
