defmodule Arcadex.Migration do
  @moduledoc """
  Behaviour for ArcadeDB migrations.

  Migrations define schema changes that can be applied (up) and rolled back (down).

  ## Example

      defmodule MyApp.ArcMigrations.V001InitialSetup do
        @behaviour Arcadex.Migration

        @impl true
        def version, do: 1

        @impl true
        def up(conn) do
          Arcadex.command!(conn, "CREATE DOCUMENT TYPE User")
          Arcadex.command!(conn, "CREATE PROPERTY User.uid STRING")
          Arcadex.command!(conn, "CREATE INDEX ON User (uid) UNIQUE")
          :ok
        end

        @impl true
        def down(conn) do
          Arcadex.command!(conn, "DROP TYPE User IF EXISTS")
          :ok
        end
      end

  """

  @doc "Return unique version number (integer, typically timestamp-based)"
  @callback version() :: pos_integer()

  @doc "Apply migration"
  @callback up(Arcadex.Conn.t()) :: :ok

  @doc "Rollback migration"
  @callback down(Arcadex.Conn.t()) :: :ok
end
