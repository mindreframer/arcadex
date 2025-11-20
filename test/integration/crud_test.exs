defmodule Arcadex.Integration.CrudTest do
  @moduledoc """
  Integration tests for basic CRUD operations against ArcadeDB.
  """
  use Arcadex.IntegrationCase, async: true

  setup_all %{conn: conn} do
    Arcadex.command!(conn, "CREATE DOCUMENT TYPE User")
    Arcadex.command!(conn, "CREATE PROPERTY User.uid STRING")
    Arcadex.command!(conn, "CREATE PROPERTY User.name STRING")
    Arcadex.command!(conn, "CREATE PROPERTY User.email STRING")
    Arcadex.command!(conn, "CREATE INDEX ON User (uid) UNIQUE")
    :ok
  end

  describe "insert" do
    test "insert document", %{conn: conn} do
      uid = generate_uid()

      [user] =
        Arcadex.command!(
          conn,
          "INSERT INTO User SET uid = :uid, name = 'John'",
          %{uid: uid}
        )

      assert user["uid"] == uid
      assert user["name"] == "John"
      assert user["@rid"]
    end

    test "insert with all fields", %{conn: conn} do
      uid = generate_uid()

      [user] =
        Arcadex.command!(
          conn,
          "INSERT INTO User SET uid = :uid, name = :name, email = :email",
          %{uid: uid, name: "Jane", email: "jane@example.com"}
        )

      assert user["uid"] == uid
      assert user["name"] == "Jane"
      assert user["email"] == "jane@example.com"
    end
  end

  describe "select" do
    test "select with WHERE", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO User SET uid = :uid, name = 'Jane'",
        %{uid: uid}
      )

      [user] =
        Arcadex.query!(
          conn,
          "SELECT FROM User WHERE uid = :uid",
          %{uid: uid}
        )

      assert user["name"] == "Jane"
      assert user["uid"] == uid
    end

    test "select with multiple conditions", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO User SET uid = :uid, name = 'Test', email = 'test@example.com'",
        %{uid: uid}
      )

      [user] =
        Arcadex.query!(
          conn,
          "SELECT FROM User WHERE uid = :uid AND name = 'Test'",
          %{uid: uid}
        )

      assert user["email"] == "test@example.com"
    end

    test "select returns empty list when not found", %{conn: conn} do
      result =
        Arcadex.query!(
          conn,
          "SELECT FROM User WHERE uid = :uid",
          %{uid: "nonexistent_uid"}
        )

      assert result == []
    end
  end

  describe "update" do
    test "update document", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO User SET uid = :uid, name = 'Old'",
        %{uid: uid}
      )

      Arcadex.command!(
        conn,
        "UPDATE User SET name = 'New' WHERE uid = :uid",
        %{uid: uid}
      )

      [user] =
        Arcadex.query!(
          conn,
          "SELECT FROM User WHERE uid = :uid",
          %{uid: uid}
        )

      assert user["name"] == "New"
    end

    test "update multiple fields", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO User SET uid = :uid, name = 'Original', email = 'old@example.com'",
        %{uid: uid}
      )

      Arcadex.command!(
        conn,
        "UPDATE User SET name = 'Updated', email = 'new@example.com' WHERE uid = :uid",
        %{uid: uid}
      )

      [user] =
        Arcadex.query!(
          conn,
          "SELECT FROM User WHERE uid = :uid",
          %{uid: uid}
        )

      assert user["name"] == "Updated"
      assert user["email"] == "new@example.com"
    end
  end

  describe "delete" do
    test "delete document", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO User SET uid = :uid, name = 'ToDelete'",
        %{uid: uid}
      )

      Arcadex.command!(
        conn,
        "DELETE FROM User WHERE uid = :uid",
        %{uid: uid}
      )

      result =
        Arcadex.query!(
          conn,
          "SELECT FROM User WHERE uid = :uid",
          %{uid: uid}
        )

      assert result == []
    end

    test "delete returns count", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO User SET uid = :uid, name = 'ToDelete'",
        %{uid: uid}
      )

      [result] =
        Arcadex.command!(
          conn,
          "DELETE FROM User WHERE uid = :uid",
          %{uid: uid}
        )

      # ArcadeDB returns the count of deleted records
      assert result["count"] == 1
    end
  end

  describe "upsert" do
    test "upsert creates new record", %{conn: conn} do
      uid = generate_uid()

      [result] =
        Arcadex.command!(
          conn,
          "UPDATE User SET uid = :uid, name = 'Upserted' UPSERT WHERE uid = :uid",
          %{uid: uid}
        )

      # UPSERT returns count of affected records
      assert result["count"] == 1

      [found] =
        Arcadex.query!(
          conn,
          "SELECT FROM User WHERE uid = :uid",
          %{uid: uid}
        )

      assert found["name"] == "Upserted"
    end

    test "upsert updates existing record", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO User SET uid = :uid, name = 'Original'",
        %{uid: uid}
      )

      [result] =
        Arcadex.command!(
          conn,
          "UPDATE User SET name = 'Upserted' UPSERT WHERE uid = :uid",
          %{uid: uid}
        )

      # UPSERT returns count of affected records
      assert result["count"] == 1

      # Verify only one record exists
      result =
        Arcadex.query!(
          conn,
          "SELECT FROM User WHERE uid = :uid",
          %{uid: uid}
        )

      assert length(result) == 1
      assert hd(result)["name"] == "Upserted"
    end
  end
end
