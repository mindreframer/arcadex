defmodule Arcadex.Migrator do
  @moduledoc """
  Migration runner for ArcadeDB.

  Tracks applied migrations in a `_migrations` document type and provides
  functions to migrate, rollback, check status, and reset the database schema.

  ## Example

      # Run all pending migrations
      {:ok, count} = Arcadex.Migrator.migrate(conn, MyApp.ArcMigrations)

      # Check migration status
      {:ok, status} = Arcadex.Migrator.status(conn, MyApp.ArcMigrations)

      # Rollback last 2 migrations
      {:ok, count} = Arcadex.Migrator.rollback(conn, MyApp.ArcMigrations, 2)

      # Reset (rollback all, then migrate all)
      {:ok, count} = Arcadex.Migrator.reset(conn, MyApp.ArcMigrations)

  """

  alias Arcadex.Conn

  # Core Functions (Phase ARX003_2A)

  @doc """
  Ensure the _migrations document type exists.

  Creates the type with version, name, and applied_at properties
  if it doesn't already exist. Returns :ok.
  """
  @spec ensure_migrations_table(Conn.t()) :: :ok
  def ensure_migrations_table(%Conn{} = conn) do
    {:ok, types} =
      Arcadex.query(
        conn,
        "SELECT FROM schema:types WHERE name = '_migrations'"
      )

    if types == [] do
      Arcadex.command!(conn, "CREATE DOCUMENT TYPE _migrations")
      Arcadex.command!(conn, "CREATE PROPERTY _migrations.version LONG")
      Arcadex.command!(conn, "CREATE PROPERTY _migrations.name STRING")
      Arcadex.command!(conn, "CREATE PROPERTY _migrations.applied_at DATETIME")
      Arcadex.command!(conn, "CREATE INDEX ON _migrations (version) UNIQUE")
    end

    :ok
  end

  @doc """
  Get list of applied migration versions.

  Returns a list of version integers sorted in ascending order.
  """
  @spec get_applied_versions(Conn.t()) :: [pos_integer()]
  def get_applied_versions(%Conn{} = conn) do
    {:ok, rows} =
      Arcadex.query(
        conn,
        "SELECT version FROM _migrations ORDER BY version"
      )

    Enum.map(rows, & &1["version"])
  end

  @doc """
  Get pending migrations from registry.

  Returns list of migration modules that haven't been applied yet,
  sorted by version ascending.
  """
  @spec get_pending_migrations(module(), [pos_integer()]) :: [module()]
  def get_pending_migrations(registry, applied) when is_atom(registry) and is_list(applied) do
    registry.migrations()
    |> Enum.filter(fn mod -> mod.version() not in applied end)
    |> Enum.sort_by(& &1.version())
  end

  @doc """
  Get migrations to rollback.

  Returns list of migration modules that have been applied,
  sorted by version descending, limited to n.
  """
  @spec get_rollback_migrations(module(), [pos_integer()], pos_integer()) :: [module()]
  def get_rollback_migrations(registry, applied, n)
      when is_atom(registry) and is_list(applied) and is_integer(n) and n > 0 do
    registry.migrations()
    |> Enum.filter(fn mod -> mod.version() in applied end)
    |> Enum.sort_by(& &1.version(), :desc)
    |> Enum.take(n)
  end

  @doc """
  Run a single migration in the given direction.

  For :up direction, calls mod.up(conn) and inserts into _migrations.
  For :down direction, calls mod.down(conn) and deletes from _migrations.

  Returns :ok on success or {:error, error} on failure.
  """
  @spec run_one(Conn.t(), module(), :up | :down) :: :ok | {:error, Arcadex.Error.t()}
  def run_one(%Conn{} = conn, mod, :up) do
    version = mod.version()
    name = module_name(mod)

    try do
      :ok = mod.up(conn)

      Arcadex.command!(
        conn,
        """
        INSERT INTO _migrations SET
          version = :version,
          name = :name,
          applied_at = sysdate()
        """,
        %{version: version, name: name}
      )

      :ok
    rescue
      e ->
        {:error, %Arcadex.Error{message: "Migration failed", detail: Exception.message(e)}}
    end
  end

  def run_one(%Conn{} = conn, mod, :down) do
    version = mod.version()

    try do
      :ok = mod.down(conn)

      Arcadex.command!(
        conn,
        "DELETE FROM _migrations WHERE version = :version",
        %{version: version}
      )

      :ok
    rescue
      e ->
        {:error, %Arcadex.Error{message: "Rollback failed", detail: Exception.message(e)}}
    end
  end

  @doc """
  Extract the short module name.

  Returns the last part of a module name (e.g., "V001CreateUser" from
  Arcadex.TestMigrations.V001CreateUser).
  """
  @spec module_name(module()) :: String.t()
  def module_name(mod) do
    mod
    |> Module.split()
    |> List.last()
  end
end
