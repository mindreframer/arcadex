defmodule Arcadex.Integration.TransactionTest do
  @moduledoc """
  Integration tests for transaction operations against ArcadeDB.
  """
  use Arcadex.IntegrationCase, async: true

  setup_all %{conn: conn} do
    Arcadex.command!(conn, "CREATE DOCUMENT TYPE Account")
    Arcadex.command!(conn, "CREATE PROPERTY Account.uid STRING")
    Arcadex.command!(conn, "CREATE PROPERTY Account.balance INTEGER")
    Arcadex.command!(conn, "CREATE INDEX ON Account (uid) UNIQUE")
    :ok
  end

  describe "transaction commit" do
    test "transaction commits on success", %{conn: conn} do
      uid = generate_uid()

      {:ok, [account]} =
        Arcadex.transaction(conn, fn tx ->
          Arcadex.command!(
            tx,
            "INSERT INTO Account SET uid = :uid, balance = 100",
            %{uid: uid}
          )
        end)

      assert account["uid"] == uid
      assert account["balance"] == 100

      # Verify data persisted
      [found] =
        Arcadex.query!(
          conn,
          "SELECT FROM Account WHERE uid = :uid",
          %{uid: uid}
        )

      assert found["balance"] == 100
    end

    test "transaction returns function result", %{conn: conn} do
      uid = generate_uid()

      {:ok, result} =
        Arcadex.transaction(conn, fn tx ->
          [account] =
            Arcadex.command!(
              tx,
              "INSERT INTO Account SET uid = :uid, balance = 200",
              %{uid: uid}
            )

          %{rid: account["@rid"], balance: account["balance"]}
        end)

      assert result.balance == 200
      assert result.rid
    end
  end

  describe "transaction rollback" do
    test "transaction rolls back on error", %{conn: conn} do
      uid = generate_uid()

      result =
        Arcadex.transaction(conn, fn tx ->
          Arcadex.command!(
            tx,
            "INSERT INTO Account SET uid = :uid, balance = 100",
            %{uid: uid}
          )

          # Force an error with invalid SQL
          Arcadex.command!(tx, "INVALID SQL SYNTAX")
        end)

      assert {:error, _error} = result

      # Verify data was NOT persisted
      found =
        Arcadex.query!(
          conn,
          "SELECT FROM Account WHERE uid = :uid",
          %{uid: uid}
        )

      assert found == []
    end

    test "transaction rolls back on raise", %{conn: conn} do
      uid = generate_uid()

      result =
        Arcadex.transaction(conn, fn tx ->
          Arcadex.command!(
            tx,
            "INSERT INTO Account SET uid = :uid, balance = 100",
            %{uid: uid}
          )

          raise "Intentional error"
        end)

      assert {:error, _error} = result

      # Verify data was NOT persisted
      found =
        Arcadex.query!(
          conn,
          "SELECT FROM Account WHERE uid = :uid",
          %{uid: uid}
        )

      assert found == []
    end
  end

  describe "multiple operations in transaction" do
    test "multiple commands in single transaction", %{conn: conn} do
      uid1 = generate_uid()
      uid2 = generate_uid()

      {:ok, _} =
        Arcadex.transaction(conn, fn tx ->
          Arcadex.command!(
            tx,
            "INSERT INTO Account SET uid = :uid, balance = 100",
            %{uid: uid1}
          )

          Arcadex.command!(
            tx,
            "INSERT INTO Account SET uid = :uid, balance = 200",
            %{uid: uid2}
          )
        end)

      # Verify both accounts exist
      [account1] =
        Arcadex.query!(
          conn,
          "SELECT FROM Account WHERE uid = :uid",
          %{uid: uid1}
        )

      [account2] =
        Arcadex.query!(
          conn,
          "SELECT FROM Account WHERE uid = :uid",
          %{uid: uid2}
        )

      assert account1["balance"] == 100
      assert account2["balance"] == 200
    end

    test "transfer between accounts in transaction", %{conn: conn} do
      uid1 = generate_uid()
      uid2 = generate_uid()

      # Create initial accounts
      Arcadex.command!(
        conn,
        "INSERT INTO Account SET uid = :uid, balance = 100",
        %{uid: uid1}
      )

      Arcadex.command!(
        conn,
        "INSERT INTO Account SET uid = :uid, balance = 50",
        %{uid: uid2}
      )

      # Transfer 30 from account1 to account2
      {:ok, _} =
        Arcadex.transaction(conn, fn tx ->
          Arcadex.command!(
            tx,
            "UPDATE Account SET balance = balance - 30 WHERE uid = :uid",
            %{uid: uid1}
          )

          Arcadex.command!(
            tx,
            "UPDATE Account SET balance = balance + 30 WHERE uid = :uid",
            %{uid: uid2}
          )
        end)

      # Verify balances
      [account1] =
        Arcadex.query!(
          conn,
          "SELECT FROM Account WHERE uid = :uid",
          %{uid: uid1}
        )

      [account2] =
        Arcadex.query!(
          conn,
          "SELECT FROM Account WHERE uid = :uid",
          %{uid: uid2}
        )

      assert account1["balance"] == 70
      assert account2["balance"] == 80
    end

    test "all operations rollback on failure", %{conn: conn} do
      uid1 = generate_uid()
      uid2 = generate_uid()

      # Create initial account
      Arcadex.command!(
        conn,
        "INSERT INTO Account SET uid = :uid, balance = 100",
        %{uid: uid1}
      )

      # Attempt transaction that will fail
      result =
        Arcadex.transaction(conn, fn tx ->
          # First operation succeeds
          Arcadex.command!(
            tx,
            "UPDATE Account SET balance = balance - 50 WHERE uid = :uid",
            %{uid: uid1}
          )

          # Second operation succeeds
          Arcadex.command!(
            tx,
            "INSERT INTO Account SET uid = :uid, balance = 50",
            %{uid: uid2}
          )

          # Third operation fails
          raise "Transaction aborted"
        end)

      assert {:error, _} = result

      # Verify first account balance unchanged
      [account1] =
        Arcadex.query!(
          conn,
          "SELECT FROM Account WHERE uid = :uid",
          %{uid: uid1}
        )

      assert account1["balance"] == 100

      # Verify second account was not created
      found =
        Arcadex.query!(
          conn,
          "SELECT FROM Account WHERE uid = :uid",
          %{uid: uid2}
        )

      assert found == []
    end
  end
end
