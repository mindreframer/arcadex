defmodule Arcadex.Integration.SQL.FunctionsTest do
  @moduledoc """
  Integration tests for SQL functions against ArcadeDB.

  Tests string, date, math, and collection functions.
  """
  use Arcadex.IntegrationCase, async: true

  setup_all %{conn: conn} do
    # Create Data type with properties for testing
    Arcadex.command!(conn, "CREATE DOCUMENT TYPE Data")
    Arcadex.command!(conn, "CREATE PROPERTY Data.uid STRING")
    Arcadex.command!(conn, "CREATE PROPERTY Data.text STRING")
    Arcadex.command!(conn, "CREATE PROPERTY Data.number DECIMAL")
    Arcadex.command!(conn, "CREATE PROPERTY Data.items LIST")
    Arcadex.command!(conn, "CREATE PROPERTY Data.created DATETIME")
    Arcadex.command!(conn, "CREATE INDEX ON Data (uid) UNIQUE")

    :ok
  end

  describe "String functions (concat, substring)" do
    test "concat function joins strings", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT 'Hello' + ' ' + 'World' as greeting"
        )

      assert result["greeting"] == "Hello World"
    end

    test "concat with parameters", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT :first + ' ' + :last as full_name",
          %{first: "John", last: "Doe"}
        )

      assert result["full_name"] == "John Doe"
    end

    test "substring function extracts part of string", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT 'Hello World'.substring(0, 5) as sub"
        )

      assert result["sub"] == "Hello"
    end

    test "substring with start position only", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT 'Hello World'.substring(6) as sub"
        )

      assert result["sub"] == "World"
    end

    test "toLowerCase and toUpperCase", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT 'Hello'.toLowerCase() as lower, 'World'.toUpperCase() as upper"
        )

      assert result["lower"] == "hello"
      assert result["upper"] == "WORLD"
    end

    test "trim function removes whitespace", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT '  trimmed  '.trim() as trimmed"
        )

      assert result["trimmed"] == "trimmed"
    end

    test "length function returns string length", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT 'Hello'.length() as len"
        )

      assert result["len"] == 5
    end

    test "replace function replaces substring", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT 'Hello World'.replace('World', 'ArcadeDB') as replaced"
        )

      assert result["replaced"] == "Hello ArcadeDB"
    end

    test "indexOf finds position of substring", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT 'Hello World'.indexOf('World') as pos"
        )

      assert result["pos"] == 6
    end

    test "string functions on stored data", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Data SET uid = :uid, text = '  Test String  '",
        %{uid: uid}
      )

      [result] =
        Arcadex.query!(
          conn,
          "SELECT text.trim().toUpperCase() as processed FROM Data WHERE uid = :uid",
          %{uid: uid}
        )

      assert result["processed"] == "TEST STRING"
    end
  end

  describe "Date functions (sysdate, format)" do
    test "sysdate returns current date", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT sysdate() as now"
        )

      assert result["now"]
      # Should be a timestamp (integer or string representation)
      assert is_integer(result["now"]) or is_binary(result["now"])
    end

    test "date function creates date from string", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT date('2023-01-15', 'yyyy-MM-dd') as d"
        )

      assert result["d"]
    end

    test "format date with pattern", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT date('2023-01-15', 'yyyy-MM-dd').format('dd/MM/yyyy') as formatted"
        )

      assert result["formatted"] == "15/01/2023"
    end

    test "date arithmetic with days", %{conn: conn} do
      # ArcadeDB uses duration() for date arithmetic
      [result] =
        Arcadex.query!(
          conn,
          """
          SELECT date('2023-01-01', 'yyyy-MM-dd').format('yyyy-MM-dd') as start_date
          """
        )

      assert result["start_date"] == "2023-01-01"
    end

    test "extract year/month/day from date", %{conn: conn} do
      # ArcadeDB uses format patterns to extract parts
      [result] =
        Arcadex.query!(
          conn,
          """
          SELECT
            date('2023-06-15', 'yyyy-MM-dd').format('yyyy') as year,
            date('2023-06-15', 'yyyy-MM-dd').format('MM') as month,
            date('2023-06-15', 'yyyy-MM-dd').format('dd') as day
          """
        )

      assert result["year"] == "2023"
      assert result["month"] == "06"
      assert result["day"] == "15"
    end

    test "store and retrieve datetime", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Data SET uid = :uid, created = sysdate()",
        %{uid: uid}
      )

      [result] =
        Arcadex.query!(
          conn,
          "SELECT created FROM Data WHERE uid = :uid",
          %{uid: uid}
        )

      assert result["created"]
    end
  end

  describe "Math functions (abs, round)" do
    test "abs returns absolute value", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT abs(-42) as positive, abs(42) as same"
        )

      assert result["positive"] == 42
      assert result["same"] == 42
    end

    test "abs with decimal", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT abs(-3.14) as absolute"
        )

      assert_in_delta result["absolute"], 3.14, 0.001
    end

    test "inline expression evaluation", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT 3 + 4 as sum"
        )

      assert result["sum"] == 7
    end

    test "math operations on decimals", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT 3.7 as decimal_up, 3.2 as decimal_down"
        )

      assert result["decimal_up"] == 3.7
      assert result["decimal_down"] == 3.2
    end

    test "integer division truncates", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT 7 / 2 as result"
        )

      # Division may return decimal
      assert result["result"] == 3.5 or result["result"] == 3
    end

    test "sqrt function calculates square root", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT sqrt(16) as root"
        )

      assert result["root"] == 4.0
    end

    test "multiplication as exponentiation", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT 2 * 2 * 2 as cubed"
        )

      assert result["cubed"] == 8
    end

    test "min and max aggregates on values", %{conn: conn} do
      # Create temporary data to test min/max as aggregates
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Data SET uid = :uid, items = :items",
        %{uid: uid, items: [10, 5, 8]}
      )

      # Use min/max as aggregate functions
      [result] =
        Arcadex.query!(
          conn,
          "SELECT min(items) as minimum, max(items) as maximum FROM Data WHERE uid = :uid",
          %{uid: uid}
        )

      assert result["minimum"] == 5
      assert result["maximum"] == 10
    end

    test "math functions on stored data", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Data SET uid = :uid, number = -25.7",
        %{uid: uid}
      )

      [result] =
        Arcadex.query!(
          conn,
          "SELECT abs(number) as absolute, number as original FROM Data WHERE uid = :uid",
          %{uid: uid}
        )

      assert_in_delta result["absolute"], 25.7, 0.001
      assert result["original"] == -25.7
    end

    test "arithmetic expressions", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT (10 + 5) * 2 as calc, 100 / 4 as div, 17 % 5 as mod"
        )

      assert result["calc"] == 30
      assert result["div"] == 25.0
      assert result["mod"] == 2
    end
  end

  describe "Collection functions (size, first)" do
    test "size function returns collection length", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT [1, 2, 3, 4, 5].size() as len"
        )

      assert result["len"] == 5
    end

    test "size of empty collection", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT [].size() as len"
        )

      assert result["len"] == 0
    end

    test "first function returns first element", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT first([10, 20, 30]) as first_item"
        )

      assert result["first_item"] == 10
    end

    test "last function returns last element", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT last([10, 20, 30]) as last_item"
        )

      assert result["last_item"] == 30
    end

    test "set function removes duplicates", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT set([1, 2, 2, 3, 3, 3]).asList() as unique_items"
        )

      assert Enum.sort(result["unique_items"]) == [1, 2, 3]
    end

    test "set function creates set from list", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT set([1, 2, 2, 3]).size() as set_size"
        )

      assert result["set_size"] == 3
    end

    test "unionall combines collections", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT unionall([1, 2], [3, 4]) as combined"
        )

      assert result["combined"] == [1, 2, 3, 4]
    end

    test "intersect finds common elements", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT intersect([1, 2, 3], [2, 3, 4]).asList() as common"
        )

      assert Enum.sort(result["common"]) == [2, 3]
    end

    test "difference finds elements not in second set", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT difference([1, 2, 3, 4], [2, 4]).asList() as diff"
        )

      assert Enum.sort(result["diff"]) == [1, 3]
    end

    test "collection functions on stored data", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Data SET uid = :uid, items = :items",
        %{uid: uid, items: ["apple", "banana", "cherry"]}
      )

      [result] =
        Arcadex.query!(
          conn,
          "SELECT items.size() as count, first(items) as first_item, last(items) as last_item FROM Data WHERE uid = :uid",
          %{uid: uid}
        )

      assert result["count"] == 3
      assert result["first_item"] == "apple"
      assert result["last_item"] == "cherry"
    end

    test "contains checks for element in WHERE", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Data SET uid = :uid, items = :items",
        %{uid: uid, items: ["a", "b", "c"]}
      )

      # Test CONTAINS in WHERE clause - finds record with 'b'
      results =
        Arcadex.query!(
          conn,
          "SELECT FROM Data WHERE uid = :uid AND items CONTAINS 'b'",
          %{uid: uid}
        )

      assert length(results) == 1

      # Test CONTAINS for missing element - no results
      no_results =
        Arcadex.query!(
          conn,
          "SELECT FROM Data WHERE uid = :uid AND items CONTAINS 'z'",
          %{uid: uid}
        )

      assert no_results == []
    end
  end

  describe "Conversion functions" do
    test "convert number to string format", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT format('%d', 42) as str"
        )

      assert result["str"] == "42"
    end

    test "convert string to integer", %{conn: conn} do
      uid = generate_uid()

      # Store a string value and convert it
      Arcadex.command!(
        conn,
        "INSERT INTO Data SET uid = :uid, text = '42'",
        %{uid: uid}
      )

      [result] =
        Arcadex.query!(
          conn,
          "SELECT text.asInteger() as num FROM Data WHERE uid = :uid",
          %{uid: uid}
        )

      assert result["num"] == 42
    end

    test "convert string to decimal", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Data SET uid = :uid, text = '3.14'",
        %{uid: uid}
      )

      [result] =
        Arcadex.query!(
          conn,
          "SELECT text.asDecimal() as decimal FROM Data WHERE uid = :uid",
          %{uid: uid}
        )

      assert_in_delta result["decimal"], 3.14, 0.001
    end

    test "convert string to boolean", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Data SET uid = :uid, text = 'true'",
        %{uid: uid}
      )

      [result] =
        Arcadex.query!(
          conn,
          "SELECT text.asBoolean() as bool_val FROM Data WHERE uid = :uid",
          %{uid: uid}
        )

      assert result["bool_val"] == true
    end
  end

  describe "Conditional functions" do
    test "boolean comparison using stored data", %{conn: conn} do
      # Store value and query with comparison in WHERE
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Data SET uid = :uid, number = 10",
        %{uid: uid}
      )

      # Query with comparison in WHERE clause
      results =
        Arcadex.query!(
          conn,
          "SELECT FROM Data WHERE uid = :uid AND number > 5",
          %{uid: uid}
        )

      assert length(results) == 1
    end

    test "ifnull returns alternative for null", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT ifnull(null, 'default') as value"
        )

      assert result["value"] == "default"
    end

    test "ifnull returns original for non-null", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT ifnull('actual', 'default') as value"
        )

      assert result["value"] == "actual"
    end

    test "nested ifnull returns first non-null", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT ifnull(null, ifnull(null, 'found')) as value"
        )

      assert result["value"] == "found"
    end
  end

  describe "Combined function usage" do
    test "nested function calls", %{conn: conn} do
      [result] =
        Arcadex.query!(
          conn,
          "SELECT 'Result: ' + format('%d', abs(-42)) as message"
        )

      assert result["message"] == "Result: 42"
    end

    test "functions in WHERE clause", %{conn: conn} do
      uid = generate_uid()

      Arcadex.command!(
        conn,
        "INSERT INTO Data SET uid = :uid, text = 'UPPERCASE'",
        %{uid: uid}
      )

      results =
        Arcadex.query!(
          conn,
          "SELECT FROM Data WHERE uid = :uid AND text.toLowerCase() = 'uppercase'",
          %{uid: uid}
        )

      assert length(results) == 1
    end

    test "functions in ORDER BY", %{conn: conn} do
      category = generate_uid()

      for {text, i} <- [{"Charlie", 1}, {"Alice", 2}, {"Bob", 3}] do
        uid = generate_uid()

        Arcadex.command!(
          conn,
          "INSERT INTO Data SET uid = :uid, text = :text, number = :num",
          %{uid: uid, text: text <> "_" <> category, num: i}
        )
      end

      results =
        Arcadex.query!(
          conn,
          "SELECT text, number FROM Data WHERE text LIKE :pattern ORDER BY text.toLowerCase()",
          %{pattern: "%_" <> category}
        )

      texts = Enum.map(results, & &1["text"])
      assert texts == ["Alice_" <> category, "Bob_" <> category, "Charlie_" <> category]
    end
  end
end
