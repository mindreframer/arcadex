defmodule Arcadex.TestMigrations.V001CreateUser do
  @moduledoc false
  @behaviour Arcadex.Migration

  @impl true
  def version, do: 1

  @impl true
  def up(conn) do
    Arcadex.command!(conn, "CREATE DOCUMENT TYPE TestUser")
    Arcadex.command!(conn, "CREATE PROPERTY TestUser.name STRING")
    :ok
  end

  @impl true
  def down(conn) do
    Arcadex.command!(conn, "DROP TYPE TestUser IF EXISTS")
    :ok
  end
end

defmodule Arcadex.TestMigrations.V002CreateOrder do
  @moduledoc false
  @behaviour Arcadex.Migration

  @impl true
  def version, do: 2

  @impl true
  def up(conn) do
    Arcadex.command!(conn, "CREATE DOCUMENT TYPE TestOrder")
    :ok
  end

  @impl true
  def down(conn) do
    Arcadex.command!(conn, "DROP TYPE TestOrder IF EXISTS")
    :ok
  end
end

defmodule Arcadex.TestMigrations.V003Failing do
  @moduledoc false
  @behaviour Arcadex.Migration

  @impl true
  def version, do: 3

  @impl true
  def up(_conn) do
    raise "Intentional failure"
  end

  @impl true
  def down(_conn), do: :ok
end

defmodule Arcadex.TestMigrations do
  @moduledoc false
  use Arcadex.MigrationRegistry

  migrations([
    Arcadex.TestMigrations.V001CreateUser,
    Arcadex.TestMigrations.V002CreateOrder
  ])
end

defmodule Arcadex.TestMigrationsWithFailure do
  @moduledoc false
  use Arcadex.MigrationRegistry

  migrations([
    Arcadex.TestMigrations.V001CreateUser,
    Arcadex.TestMigrations.V003Failing
  ])
end
