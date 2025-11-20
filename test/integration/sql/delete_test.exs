defmodule Arcadex.Integration.SQL.DeleteTest do
  @moduledoc """
  Integration tests for DELETE SQL features against ArcadeDB.

  Tests DELETE with WHERE, LIMIT, and count returns.
  """
  use Arcadex.IntegrationCase, async: true

  setup_all %{conn: conn} do
    # Create Record type with properties for testing
    Arcadex.command!(conn, "CREATE DOCUMENT TYPE Record")
    Arcadex.command!(conn, "CREATE PROPERTY Record.uid STRING")
    Arcadex.command!(conn, "CREATE PROPERTY Record.name STRING")
    Arcadex.command!(conn, "CREATE PROPERTY Record.category STRING")
    Arcadex.command!(conn, "CREATE PROPERTY Record.priority INTEGER")
    Arcadex.command!(conn, "CREATE PROPERTY Record.active BOOLEAN")
    Arcadex.command!(conn, "CREATE INDEX ON Record (uid) UNIQUE")

    :ok
  end

  describe "DELETE with WHERE" do
    test "DELETE single record with WHERE clause", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Record SET uid = :uid, name = 'ToDelete', category = 'test'",
        %{uid: uid}
      )

      # Verify record exists
      [_record] =
        Arcadex.query!(
          conn,
          "SELECT FROM Record WHERE uid = :uid",
          %{uid: uid}
        )

      # Delete it
      [result] =
        Arcadex.command!(
          conn,
          "DELETE FROM Record WHERE uid = :uid",
          %{uid: uid}
        )

      assert result["count"] == 1

      # Verify it's gone
      records =
        Arcadex.query!(
          conn,
          "SELECT FROM Record WHERE uid = :uid",
          %{uid: uid}
        )

      assert records == []
    end

    test "DELETE multiple records with WHERE clause", %{conn: conn} do
      category = generate_uid()

      # Insert 5 records with same category
      for i <- 1..5 do
        uid = generate_uid()

        Arcadex.command!(
          conn,
          "INSERT INTO Record SET uid = :uid, name = 'Bulk #{i}', category = :category",
          %{uid: uid, category: category}
        )
      end

      # Delete all with that category
      [result] =
        Arcadex.command!(
          conn,
          "DELETE FROM Record WHERE category = :category",
          %{category: category}
        )

      assert result["count"] == 5

      # Verify all are gone
      records =
        Arcadex.query!(
          conn,
          "SELECT FROM Record WHERE category = :category",
          %{category: category}
        )

      assert records == []
    end

    test "DELETE with AND condition", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Record SET uid = :uid, name = 'Conditional', category = 'multi', priority = 1",
        %{uid: uid}
      )

      [result] =
        Arcadex.command!(
          conn,
          "DELETE FROM Record WHERE uid = :uid AND category = 'multi'",
          %{uid: uid}
        )

      assert result["count"] == 1
    end

    test "DELETE with OR condition", %{conn: conn} do
      uid1 = generate_uid()
      uid2 = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Record SET uid = :uid, name = 'First', category = 'or_test'",
        %{uid: uid1}
      )

      Arcadex.command!(
        conn,
        "INSERT INTO Record SET uid = :uid, name = 'Second', category = 'or_test'",
        %{uid: uid2}
      )

      [result] =
        Arcadex.command!(
          conn,
          "DELETE FROM Record WHERE uid = :uid1 OR uid = :uid2",
          %{uid1: uid1, uid2: uid2}
        )

      assert result["count"] == 2
    end

    test "DELETE with IN clause", %{conn: conn} do
      uids =
        for i <- 1..3 do
          uid = generate_uid()

          Arcadex.command!(
            conn,
            "INSERT INTO Record SET uid = :uid, name = 'In Item #{i}', category = 'in_test'",
            %{uid: uid}
          )

          uid
        end

      [result] =
        Arcadex.command!(
          conn,
          "DELETE FROM Record WHERE uid IN :uids",
          %{uids: uids}
        )

      assert result["count"] == 3
    end

    test "DELETE with comparison operators", %{conn: conn} do
      category = generate_uid()

      # Insert records with different priorities
      for i <- 1..5 do
        uid = generate_uid()

        Arcadex.command!(
          conn,
          "INSERT INTO Record SET uid = :uid, name = 'Priority #{i}', category = :category, priority = :priority",
          %{uid: uid, category: category, priority: i}
        )
      end

      # Delete records with priority > 3
      [result] =
        Arcadex.command!(
          conn,
          "DELETE FROM Record WHERE category = :category AND priority > 3",
          %{category: category}
        )

      assert result["count"] == 2

      # Verify remaining records
      remaining =
        Arcadex.query!(
          conn,
          "SELECT FROM Record WHERE category = :category ORDER BY priority",
          %{category: category}
        )

      assert length(remaining) == 3
      priorities = Enum.map(remaining, & &1["priority"])
      assert priorities == [1, 2, 3]
    end

    test "DELETE with no matching records", %{conn: conn} do
      [result] =
        Arcadex.command!(
          conn,
          "DELETE FROM Record WHERE uid = 'nonexistent_uid_xyz_12345'"
        )

      assert result["count"] == 0
    end
  end

  describe "DELETE with LIMIT" do
    test "DELETE with LIMIT restricts deleted count", %{conn: conn} do
      category = generate_uid()

      # Insert 5 records
      for i <- 1..5 do
        uid = generate_uid()

        Arcadex.command!(
          conn,
          "INSERT INTO Record SET uid = :uid, name = 'Limited #{i}', category = :category",
          %{uid: uid, category: category}
        )
      end

      # Delete only 2
      [result] =
        Arcadex.command!(
          conn,
          "DELETE FROM Record WHERE category = :category LIMIT 2",
          %{category: category}
        )

      assert result["count"] == 2

      # Verify 3 remain
      remaining =
        Arcadex.query!(
          conn,
          "SELECT FROM Record WHERE category = :category",
          %{category: category}
        )

      assert length(remaining) == 3
    end

    test "DELETE with LIMIT larger than available records", %{conn: conn} do
      category = generate_uid()

      # Insert 3 records
      for i <- 1..3 do
        uid = generate_uid()

        Arcadex.command!(
          conn,
          "INSERT INTO Record SET uid = :uid, name = 'Over #{i}', category = :category",
          %{uid: uid, category: category}
        )
      end

      # Try to delete 10 (more than available)
      [result] =
        Arcadex.command!(
          conn,
          "DELETE FROM Record WHERE category = :category LIMIT 10",
          %{category: category}
        )

      assert result["count"] == 3
    end

    test "DELETE with LIMIT 1", %{conn: conn} do
      category = generate_uid()

      # Insert 3 records
      for i <- 1..3 do
        uid = generate_uid()

        Arcadex.command!(
          conn,
          "INSERT INTO Record SET uid = :uid, name = 'Single #{i}', category = :category",
          %{uid: uid, category: category}
        )
      end

      # Delete only 1
      [result] =
        Arcadex.command!(
          conn,
          "DELETE FROM Record WHERE category = :category LIMIT 1",
          %{category: category}
        )

      assert result["count"] == 1

      # Verify 2 remain
      remaining =
        Arcadex.query!(
          conn,
          "SELECT FROM Record WHERE category = :category",
          %{category: category}
        )

      assert length(remaining) == 2
    end
  end

  describe "DELETE returns count" do
    test "DELETE returns count of deleted records", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Record SET uid = :uid, name = 'Count Test', category = 'count'",
        %{uid: uid}
      )

      [result] =
        Arcadex.command!(
          conn,
          "DELETE FROM Record WHERE uid = :uid",
          %{uid: uid}
        )

      assert result["count"] == 1
    end

    test "DELETE returns correct count for multiple records", %{conn: conn} do
      category = generate_uid()

      # Insert 7 records
      for i <- 1..7 do
        uid = generate_uid()

        Arcadex.command!(
          conn,
          "INSERT INTO Record SET uid = :uid, name = 'Multi #{i}', category = :category",
          %{uid: uid, category: category}
        )
      end

      [result] =
        Arcadex.command!(
          conn,
          "DELETE FROM Record WHERE category = :category",
          %{category: category}
        )

      assert result["count"] == 7
    end

    test "DELETE returns zero for no matches", %{conn: conn} do
      [result] =
        Arcadex.command!(
          conn,
          "DELETE FROM Record WHERE uid = 'definitely_not_found_abc_789'"
        )

      assert result["count"] == 0
    end

    test "DELETE with RETURN BEFORE returns deleted records", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Record SET uid = :uid, name = 'Before Test', category = 'return', priority = 42",
        %{uid: uid}
      )

      results =
        Arcadex.command!(
          conn,
          "DELETE FROM Record RETURN BEFORE WHERE uid = :uid",
          %{uid: uid}
        )

      assert length(results) == 1
      [deleted] = results
      assert deleted["uid"] == uid
      assert deleted["name"] == "Before Test"
      assert deleted["priority"] == 42
    end
  end

  describe "DELETE edge cases" do
    test "DELETE all from type with WHERE true", %{conn: conn} do
      category = generate_uid()

      # Insert records
      for i <- 1..3 do
        uid = generate_uid()

        Arcadex.command!(
          conn,
          "INSERT INTO Record SET uid = :uid, name = 'All #{i}', category = :category",
          %{uid: uid, category: category}
        )
      end

      # Delete all with this category
      [result] =
        Arcadex.command!(
          conn,
          "DELETE FROM Record WHERE category = :category",
          %{category: category}
        )

      assert result["count"] == 3
    end

    test "DELETE with boolean field condition", %{conn: conn} do
      category = generate_uid()

      # Insert active and inactive records
      for i <- 1..4 do
        uid = generate_uid()

        Arcadex.command!(
          conn,
          "INSERT INTO Record SET uid = :uid, name = 'Boolean #{i}', category = :category, active = :active",
          %{uid: uid, category: category, active: rem(i, 2) == 0}
        )
      end

      # Delete only inactive ones
      [result] =
        Arcadex.command!(
          conn,
          "DELETE FROM Record WHERE category = :category AND active = false",
          %{category: category}
        )

      assert result["count"] == 2

      # Verify only active ones remain
      remaining =
        Arcadex.query!(
          conn,
          "SELECT FROM Record WHERE category = :category",
          %{category: category}
        )

      assert length(remaining) == 2
      assert Enum.all?(remaining, & &1["active"])
    end

    test "DELETE with null field condition", %{conn: conn} do
      category = generate_uid()

      # Insert records with and without priority
      uid1 = generate_uid()
      uid2 = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Record SET uid = :uid, name = 'With Priority', category = :category, priority = 1",
        %{uid: uid1, category: category}
      )

      Arcadex.command!(
        conn,
        "INSERT INTO Record SET uid = :uid, name = 'No Priority', category = :category",
        %{uid: uid2, category: category}
      )

      # Delete records without priority
      [result] =
        Arcadex.command!(
          conn,
          "DELETE FROM Record WHERE category = :category AND priority IS NULL",
          %{category: category}
        )

      assert result["count"] == 1

      # Verify only one with priority remains
      [remaining] =
        Arcadex.query!(
          conn,
          "SELECT FROM Record WHERE category = :category",
          %{category: category}
        )

      assert remaining["priority"] == 1
    end
  end
end
