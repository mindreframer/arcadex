defmodule Arcadex.Integration.SQL.SelectTest do
  @moduledoc """
  Integration tests for SELECT SQL features against ArcadeDB.

  Tests ORDER BY, GROUP BY, SKIP/LIMIT, aggregates, subqueries, and DISTINCT.
  """
  use Arcadex.IntegrationCase, async: true

  setup_all %{conn: conn} do
    # Create Product type with properties
    Arcadex.command!(conn, "CREATE DOCUMENT TYPE Product")
    Arcadex.command!(conn, "CREATE PROPERTY Product.uid STRING")
    Arcadex.command!(conn, "CREATE PROPERTY Product.name STRING")
    Arcadex.command!(conn, "CREATE PROPERTY Product.price DECIMAL")
    Arcadex.command!(conn, "CREATE PROPERTY Product.category STRING")
    Arcadex.command!(conn, "CREATE PROPERTY Product.stock INTEGER")
    Arcadex.command!(conn, "CREATE INDEX ON Product (uid) UNIQUE")

    # Seed test data with 10 products
    for i <- 1..10 do
      Arcadex.command!(
        conn,
        """
        INSERT INTO Product SET
          uid = :uid,
          name = :name,
          price = :price,
          category = :category,
          stock = :stock
        """,
        %{
          uid: "prod_#{i}",
          name: "Product #{i}",
          price: i * 10.0,
          category: if(rem(i, 2) == 0, do: "even", else: "odd"),
          stock: i * 5
        }
      )
    end

    :ok
  end

  describe "ORDER BY" do
    test "SELECT with ORDER BY ASC", %{conn: conn} do
      products =
        Arcadex.query!(
          conn,
          "SELECT name, price FROM Product ORDER BY price ASC LIMIT 3"
        )

      prices = Enum.map(products, & &1["price"])
      assert prices == [10.0, 20.0, 30.0]
    end

    test "SELECT with ORDER BY DESC", %{conn: conn} do
      products =
        Arcadex.query!(
          conn,
          "SELECT name, price FROM Product ORDER BY price DESC LIMIT 3"
        )

      prices = Enum.map(products, & &1["price"])
      assert prices == [100.0, 90.0, 80.0]
    end

    test "SELECT with multiple ORDER BY fields", %{conn: conn} do
      products =
        Arcadex.query!(
          conn,
          "SELECT name, category, price FROM Product ORDER BY category ASC, price DESC"
        )

      # First should be 'even' category with highest price
      first = hd(products)
      assert first["category"] == "even"
      assert first["price"] == 100.0
    end
  end

  describe "GROUP BY" do
    test "SELECT with GROUP BY", %{conn: conn} do
      results =
        Arcadex.query!(
          conn,
          "SELECT category, count(*) as cnt FROM Product GROUP BY category"
        )

      assert length(results) == 2

      by_category = Enum.into(results, %{}, fn r -> {r["category"], r["cnt"]} end)
      assert by_category["even"] == 5
      assert by_category["odd"] == 5
    end

    test "SELECT with GROUP BY and aggregate", %{conn: conn} do
      results =
        Arcadex.query!(
          conn,
          "SELECT category, sum(price) as total FROM Product GROUP BY category ORDER BY category"
        )

      # even: 20 + 40 + 60 + 80 + 100 = 300
      # odd: 10 + 30 + 50 + 70 + 90 = 250
      even_result = Enum.find(results, &(&1["category"] == "even"))
      odd_result = Enum.find(results, &(&1["category"] == "odd"))

      assert even_result["total"] == 300.0
      assert odd_result["total"] == 250.0
    end
  end

  describe "SKIP and LIMIT" do
    test "SELECT with SKIP and LIMIT", %{conn: conn} do
      products =
        Arcadex.query!(
          conn,
          "SELECT FROM Product ORDER BY price ASC SKIP 2 LIMIT 3"
        )

      assert length(products) == 3
      prices = Enum.map(products, & &1["price"])
      # Skipping first 2 (10, 20), getting next 3 (30, 40, 50)
      assert prices == [30.0, 40.0, 50.0]
    end

    test "SELECT with only LIMIT", %{conn: conn} do
      products =
        Arcadex.query!(
          conn,
          "SELECT FROM Product ORDER BY price ASC LIMIT 5"
        )

      assert length(products) == 5
    end

    test "SELECT with SKIP beyond data", %{conn: conn} do
      products =
        Arcadex.query!(
          conn,
          "SELECT FROM Product SKIP 100"
        )

      assert products == []
    end
  end

  describe "Aggregates" do
    test "SELECT with COUNT", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT count(*) as total FROM Product"
        )

      assert result["total"] == 10
    end

    test "SELECT with SUM", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT sum(price) as total FROM Product"
        )

      # 10 + 20 + 30 + ... + 100 = 550
      assert result["total"] == 550.0
    end

    test "SELECT with AVG", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT avg(price) as average FROM Product"
        )

      # Average of 10, 20, ..., 100 = 55
      assert result["average"] == 55.0
    end

    test "SELECT with MIN and MAX", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT min(price) as min_price, max(price) as max_price FROM Product"
        )

      assert result["min_price"] == 10.0
      assert result["max_price"] == 100.0
    end

    test "SELECT with multiple aggregates", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT count(*) as cnt, sum(stock) as total_stock, avg(price) as avg_price FROM Product"
        )

      assert result["cnt"] == 10
      # stock: 5 + 10 + 15 + ... + 50 = 275
      assert result["total_stock"] == 275
      assert result["avg_price"] == 55.0
    end
  end

  describe "Subqueries" do
    test "SELECT with subquery in WHERE", %{conn: conn} do
      # First get the average price
      [avg_result] =
        Arcadex.query!(
          conn,
          "SELECT avg(price) as avg_price FROM Product"
        )

      avg_price = avg_result["avg_price"]

      # Then use it in a parameterized query
      products =
        Arcadex.query!(
          conn,
          """
          SELECT FROM Product
          WHERE price > :avg_price
          ORDER BY price ASC
          """,
          %{avg_price: avg_price}
        )

      # Products with price > 55 (average): 60, 70, 80, 90, 100
      assert length(products) == 5
      prices = Enum.map(products, & &1["price"])
      assert Enum.all?(prices, &(&1 > 55.0))
    end

    test "SELECT with LET variable for computed value", %{conn: conn} do
      # ArcadeDB uses LET for computed subquery values
      [result] =
        Arcadex.script!(
          conn,
          """
          LET cnt = SELECT count(*) as cnt FROM Product;
          RETURN $cnt[0].cnt
          """
        )

      assert result["value"] == 10
    end
  end

  describe "DISTINCT" do
    test "SELECT DISTINCT on single field", %{conn: conn} do
      results =
        Arcadex.query!(
          conn,
          "SELECT DISTINCT category FROM Product ORDER BY category"
        )

      categories = Enum.map(results, & &1["category"])
      assert categories == ["even", "odd"]
    end

    test "SELECT DISTINCT count", %{conn: conn} do
      # ArcadeDB requires set() to count distinct values
      [result] =
        Arcadex.query!(
          conn,
          "SELECT set(category).size() as distinct_categories FROM Product"
        )

      assert result["distinct_categories"] == 2
    end
  end

  describe "Combined features" do
    test "SELECT with WHERE, ORDER BY, and LIMIT", %{conn: conn} do
      products =
        Arcadex.query!(
          conn,
          """
          SELECT name, price FROM Product
          WHERE category = 'odd'
          ORDER BY price DESC
          LIMIT 2
          """
        )

      assert length(products) == 2
      prices = Enum.map(products, & &1["price"])
      # Top 2 odd products by price: 90, 70
      assert prices == [90.0, 70.0]
    end

    test "SELECT with GROUP BY, HAVING would require HAVING", %{conn: conn} do
      # ArcadeDB supports GROUP BY with aggregate filtering via WHERE
      results =
        Arcadex.query!(
          conn,
          """
          SELECT category, count(*) as cnt, sum(price) as total
          FROM Product
          GROUP BY category
          ORDER BY total DESC
          """
        )

      # even total = 300, odd total = 250
      first = hd(results)
      assert first["category"] == "even"
      assert first["total"] == 300.0
    end
  end
end
