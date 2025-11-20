defmodule Arcadex.MigrationRegistry do
  @moduledoc """
  Behaviour and macro for defining migration registries.

  A migration registry holds an ordered list of migration modules.

  ## Example

      defmodule MyApp.ArcMigrations do
        use Arcadex.MigrationRegistry

        migrations [
          MyApp.ArcMigrations.V001InitialSetup,
          MyApp.ArcMigrations.V002AddTTSSettings,
          MyApp.ArcMigrations.V003AddUserPreferences
        ]
      end

  """

  @doc "Return ordered list of migration modules"
  @callback migrations() :: [module()]

  @doc false
  defmacro __using__(_opts) do
    quote do
      @behaviour Arcadex.MigrationRegistry

      import Arcadex.MigrationRegistry, only: [migrations: 1]

      Module.register_attribute(__MODULE__, :migrations_list, accumulate: false)
      @before_compile Arcadex.MigrationRegistry
    end
  end

  @doc """
  Define the list of migration modules.

  ## Example

      migrations [
        MyApp.ArcMigrations.V001InitialSetup,
        MyApp.ArcMigrations.V002AddTTSSettings
      ]

  """
  defmacro migrations(list) when is_list(list) do
    quote do
      @migrations_list unquote(list)
    end
  end

  @doc false
  defmacro __before_compile__(_env) do
    quote do
      @impl true
      def migrations, do: @migrations_list
    end
  end
end
