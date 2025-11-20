defmodule Arcadex.Integration.ScriptTest do
  @moduledoc """
  Integration tests for SQL script operations against ArcadeDB.
  """
  use Arcadex.IntegrationCase, async: true

  setup_all %{conn: conn} do
    Arcadex.command!(conn, "CREATE DOCUMENT TYPE Order")
    Arcadex.command!(conn, "CREATE DOCUMENT TYPE OrderItem")
    Arcadex.command!(conn, "CREATE PROPERTY Order.uid STRING")
    Arcadex.command!(conn, "CREATE PROPERTY Order.status STRING")
    Arcadex.command!(conn, "CREATE PROPERTY OrderItem.order_uid STRING")
    Arcadex.command!(conn, "CREATE PROPERTY OrderItem.name STRING")
    Arcadex.command!(conn, "CREATE PROPERTY OrderItem.quantity INTEGER")
    :ok
  end

  describe "script with LET and RETURN" do
    test "script with single LET and RETURN", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Order SET uid = :uid, status = 'pending'",
        %{uid: uid}
      )

      result =
        Arcadex.script!(
          conn,
          """
          LET order = SELECT FROM Order WHERE uid = :uid;
          RETURN $order
          """,
          %{uid: uid}
        )

      # Script returns the result of RETURN directly
      assert length(result) == 1
      [order] = result
      assert order["uid"] == uid
      assert order["status"] == "pending"
    end

    test "script with multiple LET statements", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Order SET uid = :uid, status = 'active'",
        %{uid: uid}
      )

      Arcadex.command!(
        conn,
        "INSERT INTO OrderItem SET order_uid = :uid, name = 'Item1', quantity = 2",
        %{uid: uid}
      )

      Arcadex.command!(
        conn,
        "INSERT INTO OrderItem SET order_uid = :uid, name = 'Item2', quantity = 3",
        %{uid: uid}
      )

      [result] =
        Arcadex.script!(
          conn,
          """
          LET order = SELECT FROM Order WHERE uid = :uid;
          LET items = SELECT FROM OrderItem WHERE order_uid = :uid;
          RETURN { order: $order, items: $items }
          """,
          %{uid: uid}
        )

      # When returning a map, ArcadeDB wraps it in a "value" key
      value = result["value"]
      assert length(value["order"]) == 1
      assert length(value["items"]) == 2
    end

    test "script with parameters", %{conn: conn} do
      uid = generate_uid()
      status = "completed"

      Arcadex.command!(
        conn,
        "INSERT INTO Order SET uid = :uid, status = :status",
        %{uid: uid, status: status}
      )

      result =
        Arcadex.script!(
          conn,
          """
          LET orders = SELECT FROM Order WHERE uid = :uid AND status = :status;
          RETURN $orders
          """,
          %{uid: uid, status: status}
        )

      # Script returns the result directly
      assert length(result) == 1
      [order] = result
      assert order["status"] == status
    end
  end

  describe "script with computations" do
    test "script with count computation", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Order SET uid = :uid, status = 'active'",
        %{uid: uid}
      )

      for i <- 1..3 do
        Arcadex.command!(
          conn,
          "INSERT INTO OrderItem SET order_uid = :uid, name = :name, quantity = :qty",
          %{uid: uid, name: "Item#{i}", qty: i}
        )
      end

      [result] =
        Arcadex.script!(
          conn,
          """
          LET items = SELECT FROM OrderItem WHERE order_uid = :uid;
          LET count = SELECT count(*) as total FROM OrderItem WHERE order_uid = :uid;
          RETURN { items: $items, count: $count }
          """,
          %{uid: uid}
        )

      # When returning a map, ArcadeDB wraps it in a "value" key
      value = result["value"]
      assert length(value["items"]) == 3
      [count_result] = value["count"]
      assert count_result["total"] == 3
    end

    test "script with aggregation", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Order SET uid = :uid, status = 'active'",
        %{uid: uid}
      )

      for i <- 1..3 do
        Arcadex.command!(
          conn,
          "INSERT INTO OrderItem SET order_uid = :uid, name = :name, quantity = :qty",
          %{uid: uid, name: "Item#{i}", qty: i * 10}
        )
      end

      result =
        Arcadex.script!(
          conn,
          """
          LET total = SELECT sum(quantity) as total_qty FROM OrderItem WHERE order_uid = :uid;
          RETURN $total
          """,
          %{uid: uid}
        )

      # Script returns the result directly
      [total] = result
      # 10 + 20 + 30 = 60
      assert total["total_qty"] == 60
    end
  end
end
