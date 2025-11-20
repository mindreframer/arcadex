defmodule Arcadex.Integration.InfrastructureTest do
  @moduledoc """
  Smoke tests to verify the IntegrationCase infrastructure works correctly.
  """
  use Arcadex.IntegrationCase, async: true

  describe "IntegrationCase setup" do
    test "creates database for module", %{conn: conn, db_name: db_name} do
      # Verify the database was created and we can query it
      result = Arcadex.query!(conn, "SELECT 1 as value")
      assert [%{"value" => 1}] = result

      # Verify database name follows expected pattern
      assert String.starts_with?(db_name, "arx_test_infrastructure_test_")
    end

    test "connection has correct database", %{conn: conn, db_name: db_name} do
      assert conn.database == db_name
    end

    test "multiple tests share same database", %{db_name: db_name} do
      # This test should have the same db_name as other tests in this module
      # Just verify the pattern - actual sharing is verified by running tests
      assert String.starts_with?(db_name, "arx_test_infrastructure_test_")
    end
  end

  describe "fresh_db tag" do
    @tag :fresh_db
    test "creates separate database", %{conn: conn, db_name: db_name} do
      # This test gets its own database
      result = Arcadex.query!(conn, "SELECT 1 as value")
      assert [%{"value" => 1}] = result

      # Database name should have an additional unique integer suffix
      # Pattern: arx_test_infrastructure_test_<int>_<int>
      parts = String.split(db_name, "_")
      assert length(parts) >= 5
    end
  end

  describe "generate_uid helper" do
    test "produces unique values" do
      uid1 = generate_uid()
      uid2 = generate_uid()

      # UIDs should be 16-character hex strings
      assert String.length(uid1) == 16
      assert String.length(uid2) == 16

      # UIDs should be different
      refute uid1 == uid2
    end

    test "produces hex strings" do
      uid = generate_uid()

      # Should only contain valid hex characters
      assert Regex.match?(~r/^[0-9a-f]+$/, uid)
    end
  end
end
