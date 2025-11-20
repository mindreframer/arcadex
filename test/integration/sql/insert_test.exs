defmodule Arcadex.Integration.SQL.InsertTest do
  @moduledoc """
  Integration tests for INSERT SQL features against ArcadeDB.

  Tests INSERT with RETURN, batch inserts, and content expressions.
  """
  use Arcadex.IntegrationCase, async: true

  setup_all %{conn: conn} do
    # Create Item type with properties for testing
    Arcadex.command!(conn, "CREATE DOCUMENT TYPE Item")
    Arcadex.command!(conn, "CREATE PROPERTY Item.uid STRING")
    Arcadex.command!(conn, "CREATE PROPERTY Item.name STRING")
    Arcadex.command!(conn, "CREATE PROPERTY Item.quantity INTEGER")
    Arcadex.command!(conn, "CREATE PROPERTY Item.price DECIMAL")
    Arcadex.command!(conn, "CREATE PROPERTY Item.tags LIST")
    Arcadex.command!(conn, "CREATE PROPERTY Item.metadata MAP")
    Arcadex.command!(conn, "CREATE INDEX ON Item (uid) UNIQUE")

    :ok
  end

  describe "INSERT returns created record" do
    test "INSERT returns the created record with all fields", %{conn: conn} do
      uid = generate_uid()

      [item] =
        Arcadex.command!(
          conn,
          """
          INSERT INTO Item SET
            uid = :uid,
            name = :name,
            quantity = :quantity,
            price = :price
          """,
          %{uid: uid, name: "Widget", quantity: 10, price: 19.99}
        )

      assert item["uid"] == uid
      assert item["name"] == "Widget"
      assert item["quantity"] == 10
      assert item["price"] == 19.99
      assert item["@rid"]
      assert item["@type"] == "Item"
    end

    test "INSERT returns record with @rid", %{conn: conn} do
      uid = generate_uid()

      [item] =
        Arcadex.command!(
          conn,
          "INSERT INTO Item SET uid = :uid, name = 'Test'",
          %{uid: uid}
        )

      rid = item["@rid"]
      assert is_binary(rid)
      assert String.starts_with?(rid, "#")
    end

    test "INSERT with CONTENT clause", %{conn: conn} do
      uid = generate_uid()

      [item] =
        Arcadex.command!(
          conn,
          "INSERT INTO Item CONTENT :content",
          %{
            content: %{
              uid: uid,
              name: "Content Item",
              quantity: 5,
              price: 9.99
            }
          }
        )

      assert item["uid"] == uid
      assert item["name"] == "Content Item"
      assert item["quantity"] == 5
      assert item["price"] == 9.99
    end

    test "INSERT with embedded list", %{conn: conn} do
      uid = generate_uid()

      [item] =
        Arcadex.command!(
          conn,
          "INSERT INTO Item SET uid = :uid, name = 'Tagged', tags = :tags",
          %{uid: uid, tags: ["electronics", "sale", "new"]}
        )

      assert item["uid"] == uid
      assert item["tags"] == ["electronics", "sale", "new"]
    end

    test "INSERT with embedded map", %{conn: conn} do
      uid = generate_uid()

      [item] =
        Arcadex.command!(
          conn,
          "INSERT INTO Item SET uid = :uid, name = 'With Metadata', metadata = :metadata",
          %{uid: uid, metadata: %{color: "red", size: "large", weight: 1.5}}
        )

      assert item["uid"] == uid
      assert item["metadata"]["color"] == "red"
      assert item["metadata"]["size"] == "large"
      assert item["metadata"]["weight"] == 1.5
    end
  end

  describe "INSERT multiple records" do
    test "INSERT multiple records in sequence", %{conn: conn} do
      uids =
        for i <- 1..3 do
          uid = generate_uid()

          Arcadex.command!(
            conn,
            "INSERT INTO Item SET uid = :uid, name = :name, quantity = :quantity",
            %{uid: uid, name: "Batch Item #{i}", quantity: i * 10}
          )

          uid
        end

      # Verify all records were created
      for {uid, i} <- Enum.with_index(uids, 1) do
        [item] =
          Arcadex.query!(
            conn,
            "SELECT FROM Item WHERE uid = :uid",
            %{uid: uid}
          )

        assert item["name"] == "Batch Item #{i}"
        assert item["quantity"] == i * 10
      end
    end

    test "INSERT with subquery values", %{conn: conn} do
      # Create source record
      source_uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Item SET uid = :uid, name = 'Source', quantity = 100, price = 50.0",
        %{uid: source_uid}
      )

      # Insert with computed value from subquery using script
      copy_uid = generate_uid()

      [_result] =
        Arcadex.script!(
          conn,
          """
          LET source = SELECT name, quantity, price FROM Item WHERE uid = :source_uid;
          LET copy = INSERT INTO Item SET
            uid = :copy_uid,
            name = $source[0].name + ' Copy',
            quantity = $source[0].quantity,
            price = $source[0].price;
          RETURN $copy
          """,
          %{source_uid: source_uid, copy_uid: copy_uid}
        )

      # Verify the copy was created
      [copy] =
        Arcadex.query!(
          conn,
          "SELECT FROM Item WHERE uid = :uid",
          %{uid: copy_uid}
        )

      assert copy["name"] == "Source Copy"
      assert copy["quantity"] == 100
      assert copy["price"] == 50.0
    end

    test "INSERT with default values", %{conn: conn} do
      uid = generate_uid()

      # Insert with only required fields
      [item] =
        Arcadex.command!(
          conn,
          "INSERT INTO Item SET uid = :uid, name = 'Minimal'",
          %{uid: uid}
        )

      assert item["uid"] == uid
      assert item["name"] == "Minimal"
      # Other fields should be nil/absent
      refute Map.has_key?(item, "quantity") or item["quantity"] != nil
    end

    test "INSERT with computed expression", %{conn: conn} do
      uid = generate_uid()

      [item] =
        Arcadex.command!(
          conn,
          """
          INSERT INTO Item SET
            uid = :uid,
            name = 'Computed',
            quantity = 10,
            price = 5.0 * 2
          """,
          %{uid: uid}
        )

      assert item["uid"] == uid
      assert item["price"] == 10.0
    end
  end

  describe "INSERT edge cases" do
    test "INSERT with null values", %{conn: conn} do
      uid = generate_uid()

      [item] =
        Arcadex.command!(
          conn,
          "INSERT INTO Item SET uid = :uid, name = 'Nullable', quantity = null",
          %{uid: uid}
        )

      assert item["uid"] == uid
      assert item["name"] == "Nullable"
      assert is_nil(item["quantity"])
    end

    test "INSERT with empty string", %{conn: conn} do
      uid = generate_uid()

      [item] =
        Arcadex.command!(
          conn,
          "INSERT INTO Item SET uid = :uid, name = ''",
          %{uid: uid}
        )

      assert item["uid"] == uid
      assert item["name"] == ""
    end

    test "INSERT with zero values", %{conn: conn} do
      uid = generate_uid()

      [item] =
        Arcadex.command!(
          conn,
          "INSERT INTO Item SET uid = :uid, name = 'Zero', quantity = 0, price = 0.0",
          %{uid: uid}
        )

      assert item["uid"] == uid
      assert item["quantity"] == 0
      assert item["price"] == 0.0
    end

    test "INSERT with negative values", %{conn: conn} do
      uid = generate_uid()

      [item] =
        Arcadex.command!(
          conn,
          "INSERT INTO Item SET uid = :uid, name = 'Negative', quantity = -5, price = -10.50",
          %{uid: uid}
        )

      assert item["uid"] == uid
      assert item["quantity"] == -5
      assert item["price"] == -10.50
    end
  end
end
