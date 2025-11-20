defmodule Arcadex.Integration.IndexesTest do
  @moduledoc """
  Integration tests for index operations against ArcadeDB.
  """
  use Arcadex.IntegrationCase, async: true

  describe "unique indexes" do
    @tag :fresh_db
    test "create unique index", %{conn: conn} do
      Arcadex.command!(conn, "CREATE DOCUMENT TYPE Product")
      Arcadex.command!(conn, "CREATE PROPERTY Product.sku STRING")
      Arcadex.command!(conn, "CREATE INDEX ON Product (sku) UNIQUE")

      # Verify index exists by querying schema
      [type] =
        Arcadex.query!(
          conn,
          "SELECT FROM schema:types WHERE name = 'Product'"
        )

      indexes = type["indexes"] || []
      index_properties = Enum.flat_map(indexes, & &1["properties"])

      assert "sku" in index_properties
    end

    @tag :fresh_db
    test "unique index prevents duplicates", %{conn: conn} do
      Arcadex.command!(conn, "CREATE DOCUMENT TYPE Product")
      Arcadex.command!(conn, "CREATE PROPERTY Product.sku STRING")
      Arcadex.command!(conn, "CREATE INDEX ON Product (sku) UNIQUE")

      # Insert first record
      Arcadex.command!(
        conn,
        "INSERT INTO Product SET sku = 'ABC123'"
      )

      # Attempt to insert duplicate should fail
      assert_raise Arcadex.Error, fn ->
        Arcadex.command!(
          conn,
          "INSERT INTO Product SET sku = 'ABC123'"
        )
      end
    end

    @tag :fresh_db
    test "unique index allows different values", %{conn: conn} do
      Arcadex.command!(conn, "CREATE DOCUMENT TYPE Product")
      Arcadex.command!(conn, "CREATE PROPERTY Product.sku STRING")
      Arcadex.command!(conn, "CREATE INDEX ON Product (sku) UNIQUE")

      # Insert multiple records with different SKUs
      Arcadex.command!(conn, "INSERT INTO Product SET sku = 'SKU001'")
      Arcadex.command!(conn, "INSERT INTO Product SET sku = 'SKU002'")
      Arcadex.command!(conn, "INSERT INTO Product SET sku = 'SKU003'")

      result = Arcadex.query!(conn, "SELECT FROM Product")
      assert length(result) == 3
    end
  end

  describe "composite indexes" do
    @tag :fresh_db
    test "create composite index", %{conn: conn} do
      Arcadex.command!(conn, "CREATE DOCUMENT TYPE OrderLine")
      Arcadex.command!(conn, "CREATE PROPERTY OrderLine.order_id STRING")
      Arcadex.command!(conn, "CREATE PROPERTY OrderLine.product_id STRING")
      Arcadex.command!(conn, "CREATE INDEX ON OrderLine (order_id, product_id) UNIQUE")

      # Verify index exists
      [type] =
        Arcadex.query!(
          conn,
          "SELECT FROM schema:types WHERE name = 'OrderLine'"
        )

      indexes = type["indexes"] || []
      # Find the composite index
      composite_index = Enum.find(indexes, fn idx -> length(idx["properties"]) == 2 end)

      assert composite_index
      assert "order_id" in composite_index["properties"]
      assert "product_id" in composite_index["properties"]
    end

    @tag :fresh_db
    test "composite index prevents duplicate combinations", %{conn: conn} do
      Arcadex.command!(conn, "CREATE DOCUMENT TYPE OrderLine")
      Arcadex.command!(conn, "CREATE PROPERTY OrderLine.order_id STRING")
      Arcadex.command!(conn, "CREATE PROPERTY OrderLine.product_id STRING")
      Arcadex.command!(conn, "CREATE INDEX ON OrderLine (order_id, product_id) UNIQUE")

      # Insert first combination
      Arcadex.command!(
        conn,
        "INSERT INTO OrderLine SET order_id = 'O1', product_id = 'P1'"
      )

      # Same combination should fail
      assert_raise Arcadex.Error, fn ->
        Arcadex.command!(
          conn,
          "INSERT INTO OrderLine SET order_id = 'O1', product_id = 'P1'"
        )
      end
    end

    @tag :fresh_db
    test "composite index allows same values in different positions", %{conn: conn} do
      Arcadex.command!(conn, "CREATE DOCUMENT TYPE OrderLine")
      Arcadex.command!(conn, "CREATE PROPERTY OrderLine.order_id STRING")
      Arcadex.command!(conn, "CREATE PROPERTY OrderLine.product_id STRING")
      Arcadex.command!(conn, "CREATE INDEX ON OrderLine (order_id, product_id) UNIQUE")

      # These should all succeed - different combinations
      Arcadex.command!(conn, "INSERT INTO OrderLine SET order_id = 'O1', product_id = 'P1'")
      Arcadex.command!(conn, "INSERT INTO OrderLine SET order_id = 'O1', product_id = 'P2'")
      Arcadex.command!(conn, "INSERT INTO OrderLine SET order_id = 'O2', product_id = 'P1'")

      result = Arcadex.query!(conn, "SELECT FROM OrderLine")
      assert length(result) == 3
    end
  end

  describe "non-unique indexes" do
    @tag :fresh_db
    test "create non-unique index", %{conn: conn} do
      Arcadex.command!(conn, "CREATE DOCUMENT TYPE Event")
      Arcadex.command!(conn, "CREATE PROPERTY Event.category STRING")
      Arcadex.command!(conn, "CREATE INDEX ON Event (category) NOTUNIQUE")

      # Verify index exists
      [type] =
        Arcadex.query!(
          conn,
          "SELECT FROM schema:types WHERE name = 'Event'"
        )

      indexes = type["indexes"] || []
      index_properties = Enum.flat_map(indexes, & &1["properties"])

      assert "category" in index_properties
    end

    @tag :fresh_db
    test "non-unique index allows duplicates", %{conn: conn} do
      Arcadex.command!(conn, "CREATE DOCUMENT TYPE Event")
      Arcadex.command!(conn, "CREATE PROPERTY Event.category STRING")
      Arcadex.command!(conn, "CREATE INDEX ON Event (category) NOTUNIQUE")

      # Insert multiple records with same category
      Arcadex.command!(conn, "INSERT INTO Event SET category = 'sales'")
      Arcadex.command!(conn, "INSERT INTO Event SET category = 'sales'")
      Arcadex.command!(conn, "INSERT INTO Event SET category = 'sales'")

      result = Arcadex.query!(conn, "SELECT FROM Event WHERE category = 'sales'")
      assert length(result) == 3
    end
  end
end
