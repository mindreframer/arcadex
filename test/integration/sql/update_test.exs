defmodule Arcadex.Integration.SQL.UpdateTest do
  @moduledoc """
  Integration tests for UPDATE SQL features against ArcadeDB.

  Tests UPDATE SET, ADD, REMOVE operations and various WHERE conditions.
  """
  use Arcadex.IntegrationCase, async: true

  setup_all %{conn: conn} do
    # Create Product type with properties for testing
    Arcadex.command!(conn, "CREATE DOCUMENT TYPE Product")
    Arcadex.command!(conn, "CREATE PROPERTY Product.uid STRING")
    Arcadex.command!(conn, "CREATE PROPERTY Product.name STRING")
    Arcadex.command!(conn, "CREATE PROPERTY Product.price DECIMAL")
    Arcadex.command!(conn, "CREATE PROPERTY Product.quantity INTEGER")
    Arcadex.command!(conn, "CREATE PROPERTY Product.category STRING")
    Arcadex.command!(conn, "CREATE PROPERTY Product.tags LIST")
    Arcadex.command!(conn, "CREATE PROPERTY Product.metadata MAP")
    Arcadex.command!(conn, "CREATE PROPERTY Product.active BOOLEAN")
    Arcadex.command!(conn, "CREATE INDEX ON Product (uid) UNIQUE")

    :ok
  end

  describe "UPDATE SET single/multiple fields" do
    test "UPDATE SET single field", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Product SET uid = :uid, name = 'Original', price = 10.0",
        %{uid: uid}
      )

      Arcadex.command!(
        conn,
        "UPDATE Product SET name = 'Updated' WHERE uid = :uid",
        %{uid: uid}
      )

      [product] =
        Arcadex.query!(
          conn,
          "SELECT FROM Product WHERE uid = :uid",
          %{uid: uid}
        )

      assert product["name"] == "Updated"
      assert product["price"] == 10.0
    end

    test "UPDATE SET multiple fields", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Product SET uid = :uid, name = 'Old', price = 10.0, quantity = 5",
        %{uid: uid}
      )

      Arcadex.command!(
        conn,
        "UPDATE Product SET name = 'New', price = 20.0, quantity = 10 WHERE uid = :uid",
        %{uid: uid}
      )

      [product] =
        Arcadex.query!(
          conn,
          "SELECT FROM Product WHERE uid = :uid",
          %{uid: uid}
        )

      assert product["name"] == "New"
      assert product["price"] == 20.0
      assert product["quantity"] == 10
    end

    test "UPDATE SET with parameters", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Product SET uid = :uid, name = 'Param Test', price = 5.0",
        %{uid: uid}
      )

      Arcadex.command!(
        conn,
        "UPDATE Product SET name = :name, price = :price WHERE uid = :uid",
        %{uid: uid, name: "Parameterized", price: 25.99}
      )

      [product] =
        Arcadex.query!(
          conn,
          "SELECT FROM Product WHERE uid = :uid",
          %{uid: uid}
        )

      assert product["name"] == "Parameterized"
      assert product["price"] == 25.99
    end

    test "UPDATE SET with computed expression", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Product SET uid = :uid, name = 'Computed', price = 10.0, quantity = 5",
        %{uid: uid}
      )

      # Double the price
      Arcadex.command!(
        conn,
        "UPDATE Product SET price = price * 2 WHERE uid = :uid",
        %{uid: uid}
      )

      [product] =
        Arcadex.query!(
          conn,
          "SELECT FROM Product WHERE uid = :uid",
          %{uid: uid}
        )

      assert product["price"] == 20.0
    end

    test "UPDATE SET with increment", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Product SET uid = :uid, name = 'Counter', quantity = 10",
        %{uid: uid}
      )

      Arcadex.command!(
        conn,
        "UPDATE Product SET quantity = quantity + 5 WHERE uid = :uid",
        %{uid: uid}
      )

      [product] =
        Arcadex.query!(
          conn,
          "SELECT FROM Product WHERE uid = :uid",
          %{uid: uid}
        )

      assert product["quantity"] == 15
    end
  end

  describe "UPDATE with WHERE conditions" do
    test "UPDATE with simple WHERE", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Product SET uid = :uid, name = 'Target', category = 'test'",
        %{uid: uid}
      )

      [result] =
        Arcadex.command!(
          conn,
          "UPDATE Product SET name = 'Found' WHERE uid = :uid",
          %{uid: uid}
        )

      assert result["count"] == 1

      [product] =
        Arcadex.query!(
          conn,
          "SELECT FROM Product WHERE uid = :uid",
          %{uid: uid}
        )

      assert product["name"] == "Found"
    end

    test "UPDATE with AND condition", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Product SET uid = :uid, name = 'Multi', category = 'electronics', price = 100.0",
        %{uid: uid}
      )

      [result] =
        Arcadex.command!(
          conn,
          "UPDATE Product SET name = 'Updated Multi' WHERE uid = :uid AND category = 'electronics'",
          %{uid: uid}
        )

      assert result["count"] == 1
    end

    test "UPDATE with OR condition", %{conn: conn} do
      uid1 = generate_uid()
      uid2 = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Product SET uid = :uid, name = 'First', category = 'a'",
        %{uid: uid1}
      )

      Arcadex.command!(
        conn,
        "INSERT INTO Product SET uid = :uid, name = 'Second', category = 'b'",
        %{uid: uid2}
      )

      [result] =
        Arcadex.command!(
          conn,
          "UPDATE Product SET category = 'updated' WHERE uid = :uid1 OR uid = :uid2",
          %{uid1: uid1, uid2: uid2}
        )

      assert result["count"] == 2
    end

    test "UPDATE with IN clause", %{conn: conn} do
      uids =
        for i <- 1..3 do
          uid = generate_uid()

          Arcadex.command!(
            conn,
            "INSERT INTO Product SET uid = :uid, name = 'Item #{i}', category = 'old'",
            %{uid: uid}
          )

          uid
        end

      [result] =
        Arcadex.command!(
          conn,
          "UPDATE Product SET category = 'new' WHERE uid IN :uids",
          %{uids: uids}
        )

      assert result["count"] == 3
    end

    test "UPDATE with comparison operators", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Product SET uid = :uid, name = 'Cheap', price = 5.0",
        %{uid: uid}
      )

      [result] =
        Arcadex.command!(
          conn,
          "UPDATE Product SET name = 'Still Cheap' WHERE uid = :uid AND price < 10.0",
          %{uid: uid}
        )

      assert result["count"] == 1
    end

    test "UPDATE with no matching records", %{conn: conn} do
      [result] =
        Arcadex.command!(
          conn,
          "UPDATE Product SET name = 'Ghost' WHERE uid = 'nonexistent_uid_12345'"
        )

      assert result["count"] == 0
    end
  end

  describe "UPDATE returns modified count" do
    test "UPDATE returns count of modified records", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Product SET uid = :uid, name = 'Count Test', category = 'count'",
        %{uid: uid}
      )

      [result] =
        Arcadex.command!(
          conn,
          "UPDATE Product SET name = 'Counted' WHERE uid = :uid",
          %{uid: uid}
        )

      assert result["count"] == 1
    end

    test "UPDATE returns count for multiple records", %{conn: conn} do
      category = generate_uid()

      # Insert 5 products with same category
      for i <- 1..5 do
        uid = generate_uid()

        Arcadex.command!(
          conn,
          "INSERT INTO Product SET uid = :uid, name = 'Bulk #{i}', category = :category",
          %{uid: uid, category: category}
        )
      end

      [result] =
        Arcadex.command!(
          conn,
          "UPDATE Product SET name = 'Bulk Updated' WHERE category = :category",
          %{category: category}
        )

      assert result["count"] == 5
    end

    test "UPDATE returns zero for no matches", %{conn: conn} do
      [result] =
        Arcadex.command!(
          conn,
          "UPDATE Product SET name = 'No Match' WHERE uid = 'definitely_not_found_xyz'"
        )

      assert result["count"] == 0
    end
  end

  describe "UPDATE with lists and embedded documents" do
    test "UPDATE SET list field", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Product SET uid = :uid, name = 'Tagged', tags = ['original']",
        %{uid: uid}
      )

      Arcadex.command!(
        conn,
        "UPDATE Product SET tags = :tags WHERE uid = :uid",
        %{uid: uid, tags: ["new", "updated", "fresh"]}
      )

      [product] =
        Arcadex.query!(
          conn,
          "SELECT FROM Product WHERE uid = :uid",
          %{uid: uid}
        )

      assert product["tags"] == ["new", "updated", "fresh"]
    end

    test "UPDATE SET embedded document", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Product SET uid = :uid, name = 'With Meta', metadata = :meta",
        %{uid: uid, meta: %{version: 1}}
      )

      Arcadex.command!(
        conn,
        "UPDATE Product SET metadata = :meta WHERE uid = :uid",
        %{uid: uid, meta: %{version: 2, updated: true}}
      )

      [product] =
        Arcadex.query!(
          conn,
          "SELECT FROM Product WHERE uid = :uid",
          %{uid: uid}
        )

      assert product["metadata"]["version"] == 2
      assert product["metadata"]["updated"] == true
    end

    test "UPDATE SET nested field in embedded document", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Product SET uid = :uid, name = 'Nested', metadata = :meta",
        %{uid: uid, meta: %{info: %{level: 1}}}
      )

      Arcadex.command!(
        conn,
        "UPDATE Product SET metadata.info.level = 2 WHERE uid = :uid",
        %{uid: uid}
      )

      [product] =
        Arcadex.query!(
          conn,
          "SELECT FROM Product WHERE uid = :uid",
          %{uid: uid}
        )

      assert product["metadata"]["info"]["level"] == 2
    end
  end

  describe "UPDATE edge cases" do
    test "UPDATE SET to null", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Product SET uid = :uid, name = 'Nullable', price = 10.0",
        %{uid: uid}
      )

      Arcadex.command!(
        conn,
        "UPDATE Product SET price = null WHERE uid = :uid",
        %{uid: uid}
      )

      [product] =
        Arcadex.query!(
          conn,
          "SELECT FROM Product WHERE uid = :uid",
          %{uid: uid}
        )

      assert is_nil(product["price"])
    end

    test "UPDATE SET boolean field", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Product SET uid = :uid, name = 'Toggle', active = false",
        %{uid: uid}
      )

      Arcadex.command!(
        conn,
        "UPDATE Product SET active = true WHERE uid = :uid",
        %{uid: uid}
      )

      [product] =
        Arcadex.query!(
          conn,
          "SELECT FROM Product WHERE uid = :uid",
          %{uid: uid}
        )

      assert product["active"] == true
    end

    test "UPDATE with LIMIT", %{conn: conn} do
      category = generate_uid()

      # Insert 5 products with same category
      for i <- 1..5 do
        uid = generate_uid()

        Arcadex.command!(
          conn,
          "INSERT INTO Product SET uid = :uid, name = 'Limited #{i}', category = :category",
          %{uid: uid, category: category}
        )
      end

      [result] =
        Arcadex.command!(
          conn,
          "UPDATE Product SET name = 'Limited Updated' WHERE category = :category LIMIT 2",
          %{category: category}
        )

      assert result["count"] == 2

      # Verify only 2 were updated
      updated =
        Arcadex.query!(
          conn,
          "SELECT FROM Product WHERE category = :category AND name = 'Limited Updated'",
          %{category: category}
        )

      assert length(updated) == 2
    end

    test "UPDATE RETURN AFTER returns updated document", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Product SET uid = :uid, name = 'Before', price = 10.0",
        %{uid: uid}
      )

      results =
        Arcadex.command!(
          conn,
          "UPDATE Product SET name = 'After', price = 20.0 RETURN AFTER WHERE uid = :uid",
          %{uid: uid}
        )

      assert length(results) == 1
      [updated] = results
      assert updated["name"] == "After"
      assert updated["price"] == 20.0
    end
  end
end
