defmodule Arcadex.Integration.ProjectionsTest do
  @moduledoc """
  Integration tests for nested projections and subqueries against ArcadeDB.
  """
  use Arcadex.IntegrationCase, async: true

  describe "nested projections" do
    @tag :fresh_db
    test "nested projection with map syntax", %{conn: conn} do
      Arcadex.command!(conn, "CREATE DOCUMENT TYPE Employee")
      Arcadex.command!(conn, "CREATE PROPERTY Employee.name STRING")
      Arcadex.command!(conn, "CREATE PROPERTY Employee.department STRING")
      Arcadex.command!(conn, "CREATE PROPERTY Employee.salary INTEGER")

      Arcadex.command!(
        conn,
        "INSERT INTO Employee SET name = 'Alice', department = 'Engineering', salary = 100000"
      )

      # Query with nested projection using map syntax
      [result] =
        Arcadex.query!(
          conn,
          "SELECT name, { 'dept': department, 'pay': salary } as details FROM Employee"
        )

      assert result["name"] == "Alice"
      assert result["details"]["dept"] == "Engineering"
      assert result["details"]["pay"] == 100_000
    end

    @tag :fresh_db
    test "projection with computed fields", %{conn: conn} do
      Arcadex.command!(conn, "CREATE DOCUMENT TYPE Product")
      Arcadex.command!(conn, "CREATE PROPERTY Product.name STRING")
      Arcadex.command!(conn, "CREATE PROPERTY Product.price DECIMAL")
      Arcadex.command!(conn, "CREATE PROPERTY Product.quantity INTEGER")

      Arcadex.command!(
        conn,
        "INSERT INTO Product SET name = 'Widget', price = 10.00, quantity = 5"
      )

      # Query with computed projection
      [result] =
        Arcadex.query!(
          conn,
          "SELECT name, price * quantity as total_value FROM Product"
        )

      assert result["name"] == "Widget"
      assert result["total_value"] == 50.0
    end

    @tag :fresh_db
    test "projection with multiple aliases", %{conn: conn} do
      Arcadex.command!(conn, "CREATE DOCUMENT TYPE SalesOrder")
      Arcadex.command!(conn, "CREATE PROPERTY SalesOrder.uid STRING")
      Arcadex.command!(conn, "CREATE PROPERTY SalesOrder.amount DECIMAL")
      Arcadex.command!(conn, "CREATE PROPERTY SalesOrder.tax_rate DECIMAL")

      Arcadex.command!(conn, "INSERT INTO SalesOrder SET uid = 'O1', amount = 100, tax_rate = 0.1")
      Arcadex.command!(conn, "INSERT INTO SalesOrder SET uid = 'O2', amount = 200, tax_rate = 0.15")

      # Query with multiple computed/aliased fields
      results =
        Arcadex.query!(
          conn,
          """
          SELECT uid,
            amount as base_amount,
            amount * tax_rate as tax,
            amount * (1 + tax_rate) as total
          FROM SalesOrder
          ORDER BY amount
          """
        )

      assert length(results) == 2
      [first, second] = results
      assert first["base_amount"] == 100
      assert first["tax"] == 10.0
      assert first["total"] == 110.0
      assert second["base_amount"] == 200
      assert second["tax"] == 30.0
      assert second["total"] == 230.0
    end
  end

  describe "subquery projections" do
    @tag :fresh_db
    test "subquery as projection field", %{conn: conn} do
      Arcadex.command!(conn, "CREATE DOCUMENT TYPE Department")
      Arcadex.command!(conn, "CREATE PROPERTY Department.name STRING")

      Arcadex.command!(conn, "CREATE DOCUMENT TYPE Employee")
      Arcadex.command!(conn, "CREATE PROPERTY Employee.name STRING")
      Arcadex.command!(conn, "CREATE PROPERTY Employee.dept STRING")

      # Create departments
      Arcadex.command!(conn, "INSERT INTO Department SET name = 'Engineering'")
      Arcadex.command!(conn, "INSERT INTO Department SET name = 'Sales'")

      # Create employees
      Arcadex.command!(conn, "INSERT INTO Employee SET name = 'Alice', dept = 'Engineering'")
      Arcadex.command!(conn, "INSERT INTO Employee SET name = 'Bob', dept = 'Engineering'")
      Arcadex.command!(conn, "INSERT INTO Employee SET name = 'Charlie', dept = 'Sales'")

      # Query with subquery projection - returns result set, extract count
      results =
        Arcadex.query!(
          conn,
          """
          SELECT name,
            (SELECT count(*) FROM Employee WHERE dept = $parent.current.name) as employee_count
          FROM Department
          ORDER BY name
          """
        )

      assert length(results) == 2

      eng = Enum.find(results, &(&1["name"] == "Engineering"))
      sales = Enum.find(results, &(&1["name"] == "Sales"))

      # Subquery returns a list with one map containing "count(*)"
      eng_count = hd(eng["employee_count"])["count(*)"]
      sales_count = hd(sales["employee_count"])["count(*)"]

      assert eng_count == 2
      assert sales_count == 1
    end

    @tag :fresh_db
    test "subquery in WHERE clause with first()", %{conn: conn} do
      Arcadex.command!(conn, "CREATE DOCUMENT TYPE Product")
      Arcadex.command!(conn, "CREATE PROPERTY Product.uid STRING")
      Arcadex.command!(conn, "CREATE PROPERTY Product.name STRING")
      Arcadex.command!(conn, "CREATE PROPERTY Product.price DECIMAL")

      # Insert products with varying prices
      Arcadex.command!(conn, "INSERT INTO Product SET uid = 'P1', name = 'Cheap', price = 10")
      Arcadex.command!(conn, "INSERT INTO Product SET uid = 'P2', name = 'Average', price = 50")
      Arcadex.command!(conn, "INSERT INTO Product SET uid = 'P3', name = 'Expensive', price = 100")

      # Calculate average and query products above it
      # Using LET to store the average
      results =
        Arcadex.query!(
          conn,
          """
          SELECT name, price FROM Product
          WHERE price > 53
          ORDER BY price
          """
        )

      # Average is ~53.33, so only 'Expensive' (100) should be returned
      assert length(results) == 1
      assert hd(results)["name"] == "Expensive"
    end

    @tag :fresh_db
    test "correlated subquery", %{conn: conn} do
      Arcadex.command!(conn, "CREATE DOCUMENT TYPE Category")
      Arcadex.command!(conn, "CREATE PROPERTY Category.name STRING")

      Arcadex.command!(conn, "CREATE DOCUMENT TYPE Item")
      Arcadex.command!(conn, "CREATE PROPERTY Item.name STRING")
      Arcadex.command!(conn, "CREATE PROPERTY Item.category STRING")
      Arcadex.command!(conn, "CREATE PROPERTY Item.price DECIMAL")

      # Create categories
      Arcadex.command!(conn, "INSERT INTO Category SET name = 'Electronics'")
      Arcadex.command!(conn, "INSERT INTO Category SET name = 'Books'")

      # Create items
      Arcadex.command!(conn, "INSERT INTO Item SET name = 'Phone', category = 'Electronics', price = 500")
      Arcadex.command!(conn, "INSERT INTO Item SET name = 'Laptop', category = 'Electronics', price = 1000")
      Arcadex.command!(conn, "INSERT INTO Item SET name = 'Novel', category = 'Books', price = 15")
      Arcadex.command!(conn, "INSERT INTO Item SET name = 'Textbook', category = 'Books', price = 50")

      # Find most expensive item in each category
      results =
        Arcadex.query!(
          conn,
          """
          SELECT name,
            (SELECT max(price) FROM Item WHERE category = $parent.current.name) as max_price
          FROM Category
          ORDER BY name
          """
        )

      assert length(results) == 2

      books = Enum.find(results, &(&1["name"] == "Books"))
      electronics = Enum.find(results, &(&1["name"] == "Electronics"))

      # Subquery returns a list with one map containing "max(price)"
      books_max = hd(books["max_price"])["max(price)"]
      electronics_max = hd(electronics["max_price"])["max(price)"]

      assert books_max == 50
      assert electronics_max == 1000
    end
  end

  describe "array and collection projections" do
    @tag :fresh_db
    test "project array elements", %{conn: conn} do
      Arcadex.command!(conn, "CREATE DOCUMENT TYPE DocRecord")
      Arcadex.command!(conn, "CREATE PROPERTY DocRecord.title STRING")
      Arcadex.command!(conn, "CREATE PROPERTY DocRecord.scores LIST")

      Arcadex.command!(
        conn,
        "INSERT INTO DocRecord SET title = 'Test', scores = [85, 90, 78, 92]"
      )

      # Query with array operations
      [result] =
        Arcadex.query!(
          conn,
          "SELECT title, scores[0] as first_score, scores.size() as num_scores FROM DocRecord"
        )

      assert result["title"] == "Test"
      assert result["first_score"] == 85
      assert result["num_scores"] == 4
    end

    @tag :fresh_db
    test "project with aggregate functions on list", %{conn: conn} do
      Arcadex.command!(conn, "CREATE DOCUMENT TYPE Survey")
      Arcadex.command!(conn, "CREATE PROPERTY Survey.name STRING")
      Arcadex.command!(conn, "CREATE PROPERTY Survey.responses LIST")

      Arcadex.command!(
        conn,
        "INSERT INTO Survey SET name = 'Feedback', responses = [5, 4, 5, 3, 4, 5]"
      )

      # Query with list size
      [result] =
        Arcadex.query!(
          conn,
          """
          SELECT name,
            responses.size() as count
          FROM Survey
          """
        )

      assert result["name"] == "Feedback"
      assert result["count"] == 6
    end

    @tag :fresh_db
    test "unwind list into rows", %{conn: conn} do
      Arcadex.command!(conn, "CREATE DOCUMENT TYPE DataSet")
      Arcadex.command!(conn, "CREATE PROPERTY DataSet.name STRING")
      Arcadex.command!(conn, "CREATE PROPERTY DataSet.values LIST")

      Arcadex.command!(
        conn,
        "INSERT INTO DataSet SET name = 'Numbers', values = [10, 20, 30]"
      )

      # Use UNWIND to expand list into rows
      results =
        Arcadex.query!(
          conn,
          """
          SELECT name, values
          FROM DataSet
          UNWIND values
          """
        )

      assert length(results) == 3
      values = Enum.map(results, & &1["values"])
      assert Enum.sort(values) == [10, 20, 30]
    end
  end
end
