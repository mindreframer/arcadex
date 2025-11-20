defmodule Arcadex.Integration.ServerTest do
  @moduledoc """
  Integration tests for server-level operations.

  These tests run sequentially (async: false) because they perform
  server-level operations like creating and dropping databases that
  could conflict with other tests.
  """
  use Arcadex.IntegrationCase, async: false

  describe "database_exists?" do
    @tag :sequential
    test "returns true for existing database", %{conn: _conn, db_name: db_name} do
      # The module's database was created in setup_all
      # We need a connection without a specific database to check existence
      server_conn = create_connection()
      assert Arcadex.database_exists?(server_conn, db_name) == true
    end

    @tag :sequential
    test "returns false for non-existent database", %{conn: _conn} do
      server_conn = create_connection()
      assert Arcadex.database_exists?(server_conn, "nonexistent_db_12345") == false
    end
  end

  describe "create and drop database" do
    @tag :sequential
    @tag :fresh_db
    test "creates and drops database successfully", %{db_name: db_name} do
      server_conn = create_connection()

      # Create a new database with a unique name
      new_db = "#{db_name}_manual_#{:erlang.unique_integer([:positive])}"

      # Should not exist initially
      assert Arcadex.database_exists?(server_conn, new_db) == false

      # Create the database
      :ok = Arcadex.create_database!(server_conn, new_db)
      assert Arcadex.database_exists?(server_conn, new_db) == true

      # Verify we can connect and query the new database
      db_conn = Arcadex.with_database(server_conn, new_db)
      result = Arcadex.query!(db_conn, "SELECT 1 as value")
      assert [%{"value" => 1}] = result

      # Drop the database
      :ok = Arcadex.drop_database!(server_conn, new_db)
      assert Arcadex.database_exists?(server_conn, new_db) == false
    end
  end
end
