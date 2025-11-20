defmodule Arcadex.Application do
  @moduledoc """
  Application module for Arcadex.

  Starts the default Finch connection pool for HTTP requests to ArcadeDB.
  """

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Finch, name: Arcadex.Finch}
    ]

    opts = [strategy: :one_for_one, name: Arcadex.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
