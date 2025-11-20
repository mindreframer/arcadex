defmodule Arcadex.MigrationRegistryTest do
  use ExUnit.Case, async: true

  describe "ARX003_1A: Migration registry tests" do
    test "ARX003_1A_T3: registry module with migrations macro" do
      # Ensure module is loaded before checking exports
      Code.ensure_loaded!(Arcadex.TestMigrations)

      # The test registry module compiles and implements the behaviour
      assert function_exported?(Arcadex.TestMigrations, :migrations, 0)
    end

    test "ARX003_1A_T4: registry.migrations/0 returns list of modules" do
      migrations = Arcadex.TestMigrations.migrations()

      assert is_list(migrations)
      assert length(migrations) == 2
      assert Arcadex.TestMigrations.V001CreateUser in migrations
      assert Arcadex.TestMigrations.V002CreateOrder in migrations
    end

    test "ARX003_1A_T4: registry.migrations/0 returns modules in declared order" do
      migrations = Arcadex.TestMigrations.migrations()

      assert migrations == [
               Arcadex.TestMigrations.V001CreateUser,
               Arcadex.TestMigrations.V002CreateOrder
             ]
    end

    test "ARX003_1A_T4: different registry returns different list" do
      migrations = Arcadex.TestMigrationsWithFailure.migrations()

      assert migrations == [
               Arcadex.TestMigrations.V001CreateUser,
               Arcadex.TestMigrations.V003Failing
             ]
    end
  end
end
