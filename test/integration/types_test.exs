defmodule Arcadex.Integration.TypesTest do
  @moduledoc """
  Integration tests for type creation operations against ArcadeDB.
  """
  use Arcadex.IntegrationCase, async: true

  describe "document types" do
    @tag :fresh_db
    test "create document type", %{conn: conn} do
      Arcadex.command!(conn, "CREATE DOCUMENT TYPE MyDoc")

      [type] =
        Arcadex.query!(
          conn,
          "SELECT FROM schema:types WHERE name = 'MyDoc'"
        )

      assert type["name"] == "MyDoc"
    end

    @tag :fresh_db
    test "create document type with properties", %{conn: conn} do
      Arcadex.command!(conn, "CREATE DOCUMENT TYPE Customer")
      Arcadex.command!(conn, "CREATE PROPERTY Customer.name STRING")
      Arcadex.command!(conn, "CREATE PROPERTY Customer.age INTEGER")
      Arcadex.command!(conn, "CREATE PROPERTY Customer.active BOOLEAN")

      [type] =
        Arcadex.query!(
          conn,
          "SELECT FROM schema:types WHERE name = 'Customer'"
        )

      assert type["name"] == "Customer"
      properties = type["properties"]
      property_names = Enum.map(properties, & &1["name"])

      assert "name" in property_names
      assert "age" in property_names
      assert "active" in property_names
    end
  end

  describe "vertex types" do
    @tag :fresh_db
    test "create vertex type", %{conn: conn} do
      Arcadex.command!(conn, "CREATE VERTEX TYPE Person")

      [type] =
        Arcadex.query!(
          conn,
          "SELECT FROM schema:types WHERE name = 'Person'"
        )

      assert type["name"] == "Person"
      assert type["type"] == "vertex"
    end

    @tag :fresh_db
    test "create vertex and insert", %{conn: conn} do
      Arcadex.command!(conn, "CREATE VERTEX TYPE City")
      Arcadex.command!(conn, "CREATE PROPERTY City.name STRING")

      [city] =
        Arcadex.command!(
          conn,
          "CREATE VERTEX City SET name = 'Tokyo'"
        )

      assert city["name"] == "Tokyo"
      assert city["@rid"]
      assert city["@type"] == "City"
    end
  end

  describe "edge types" do
    @tag :fresh_db
    test "create edge type", %{conn: conn} do
      Arcadex.command!(conn, "CREATE VERTEX TYPE Person")
      Arcadex.command!(conn, "CREATE EDGE TYPE Knows")

      [type] =
        Arcadex.query!(
          conn,
          "SELECT FROM schema:types WHERE name = 'Knows'"
        )

      assert type["name"] == "Knows"
      assert type["type"] == "edge"
    end

    @tag :fresh_db
    test "create edge between vertices", %{conn: conn} do
      Arcadex.command!(conn, "CREATE VERTEX TYPE Person")
      Arcadex.command!(conn, "CREATE PROPERTY Person.name STRING")
      Arcadex.command!(conn, "CREATE EDGE TYPE Knows")
      Arcadex.command!(conn, "CREATE PROPERTY Knows.since INTEGER")

      [alice] =
        Arcadex.command!(
          conn,
          "CREATE VERTEX Person SET name = 'Alice'"
        )

      [bob] =
        Arcadex.command!(
          conn,
          "CREATE VERTEX Person SET name = 'Bob'"
        )

      alice_rid = alice["@rid"]
      bob_rid = bob["@rid"]

      [edge] =
        Arcadex.command!(
          conn,
          "CREATE EDGE Knows FROM #{alice_rid} TO #{bob_rid} SET since = 2020"
        )

      assert edge["@type"] == "Knows"
      assert edge["since"] == 2020
      assert edge["@in"] == bob_rid
      assert edge["@out"] == alice_rid
    end
  end

  describe "drop types" do
    @tag :fresh_db
    test "drop type", %{conn: conn} do
      Arcadex.command!(conn, "CREATE DOCUMENT TYPE TempDoc")

      # Verify type exists
      [type] =
        Arcadex.query!(
          conn,
          "SELECT FROM schema:types WHERE name = 'TempDoc'"
        )

      assert type["name"] == "TempDoc"

      # Drop the type
      Arcadex.command!(conn, "DROP TYPE TempDoc")

      # Verify type no longer exists
      result =
        Arcadex.query!(
          conn,
          "SELECT FROM schema:types WHERE name = 'TempDoc'"
        )

      assert result == []
    end

    @tag :fresh_db
    test "drop type with UNSAFE when data exists", %{conn: conn} do
      Arcadex.command!(conn, "CREATE DOCUMENT TYPE TempData")
      Arcadex.command!(conn, "INSERT INTO TempData SET value = 1")

      # Drop with UNSAFE to force deletion even with data
      Arcadex.command!(conn, "DROP TYPE TempData UNSAFE")

      # Verify type no longer exists
      result =
        Arcadex.query!(
          conn,
          "SELECT FROM schema:types WHERE name = 'TempData'"
        )

      assert result == []
    end
  end
end
