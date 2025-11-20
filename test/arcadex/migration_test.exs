defmodule Arcadex.MigrationTest do
  use ExUnit.Case, async: true

  alias Arcadex.TestMigrations

  describe "ARX003_1A: Migration behaviour tests" do
    test "ARX003_1A_T1: migration module implements behaviour" do
      # Ensure module is loaded before checking exports
      Code.ensure_loaded!(TestMigrations.V001CreateUser)

      # The test migration module compiles and implements the behaviour
      assert function_exported?(TestMigrations.V001CreateUser, :version, 0)
      assert function_exported?(TestMigrations.V001CreateUser, :up, 1)
      assert function_exported?(TestMigrations.V001CreateUser, :down, 1)
    end

    test "ARX003_1A_T2: migration module defines version/0 returning pos_integer" do
      assert TestMigrations.V001CreateUser.version() == 1
      assert TestMigrations.V002CreateOrder.version() == 2
      assert TestMigrations.V003Failing.version() == 3
    end
  end
end
