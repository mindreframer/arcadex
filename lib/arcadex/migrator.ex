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

  # Public API (Phase ARX003_3A)

  @doc """
  Run all pending migrations.

  Returns `{:ok, count}` where count is the number of migrations run,
  or `{:error, error}` if a migration fails.

  ## Example

      {:ok, 2} = Arcadex.Migrator.migrate(conn, MyApp.ArcMigrations)

  """
  @spec migrate(Conn.t(), module()) :: {:ok, non_neg_integer()} | {:error, Arcadex.Error.t()}
  def migrate(%Conn{} = conn, registry) when is_atom(registry) do
    ensure_migrations_table(conn)

    applied = get_applied_versions(conn)
    pending = get_pending_migrations(registry, applied)

    if pending == [] do
      {:ok, 0}
    else
      run_migrations(pending, conn, :up)
    end
  end

  @doc """
  Rollback last n migrations.

  Returns `{:ok, count}` where count is the number of migrations rolled back,
  or `{:error, error}` if a rollback fails.

  ## Example

      {:ok, 1} = Arcadex.Migrator.rollback(conn, MyApp.ArcMigrations, 1)

  """
  @spec rollback(Conn.t(), module(), pos_integer()) ::
          {:ok, non_neg_integer()} | {:error, Arcadex.Error.t()}
  def rollback(%Conn{} = conn, registry, n \\ 1)
      when is_atom(registry) and is_integer(n) and n > 0 do
    ensure_migrations_table(conn)

    applied = get_applied_versions(conn)
    to_rollback = get_rollback_migrations(registry, applied, n)

    if to_rollback == [] do
      {:ok, 0}
    else
      run_migrations(to_rollback, conn, :down)
    end
  end

  @doc """
  Get migration status.

  Returns `{:ok, status_list}` where status_list contains a map for each
  migration with version, name, and status (:applied or :pending).

  ## Example

      {:ok, status} = Arcadex.Migrator.status(conn, MyApp.ArcMigrations)
      # [
      #   %{version: 1, name: "V001InitialSetup", status: :applied},
      #   %{version: 2, name: "V002AddTTSSettings", status: :pending}
      # ]

  """
  @spec status(Conn.t(), module()) ::
          {:ok, [%{version: pos_integer(), name: String.t(), status: :applied | :pending}]}
  def status(%Conn{} = conn, registry) when is_atom(registry) do
    ensure_migrations_table(conn)

    applied = get_applied_versions(conn)
    all_migrations = registry.migrations()

    status_list =
      Enum.map(all_migrations, fn mod ->
        version = mod.version()

        %{
          version: version,
          name: module_name(mod),
          status: if(version in applied, do: :applied, else: :pending)
        }
      end)

    {:ok, status_list}
  end

  @doc """
  Rollback all migrations, then migrate all.

  Returns `{:ok, count}` where count is the number of migrations applied,
  or `{:error, error}` if any operation fails.

  ## Example

      {:ok, 3} = Arcadex.Migrator.reset(conn, MyApp.ArcMigrations)

  """
  @spec reset(Conn.t(), module()) :: {:ok, non_neg_integer()} | {:error, Arcadex.Error.t()}
  def reset(%Conn{} = conn, registry) when is_atom(registry) do
    ensure_migrations_table(conn)

    applied = get_applied_versions(conn)

    # Rollback all in reverse order
    if length(applied) > 0 do
      case rollback(conn, registry, length(applied)) do
        {:ok, _} -> :ok
        {:error, error} -> {:error, error}
      end
    end

    # Migrate all
    migrate(conn, registry)
  end

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

  # Private helper for running multiple migrations

  @spec run_migrations([module()], Conn.t(), :up | :down) ::
          {:ok, non_neg_integer()} | {:error, Arcadex.Error.t()}
  defp run_migrations(migrations, conn, direction) do
    Enum.reduce_while(migrations, {:ok, 0}, fn mod, {:ok, count} ->
      case run_one(conn, mod, direction) do
        :ok -> {:cont, {:ok, count + 1}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end
end
