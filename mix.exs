defmodule Arcadex.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/mindreframer/arcadex"

  def project do
    [
      app: :arcadex,
      version: @version,
      elixir: "~> 1.17",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
      name: "Arcadex",
      source_url: @source_url
    ]
  end

  defp description do
    "A lean Elixir wrapper for ArcadeDB's REST API with connection pooling, transactions, and database switching."
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url},
      maintainers: ["Roman Heinrich"]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support", "test/integration/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Arcadex.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:req, "~> 0.5.0"},
      {:finch, "~> 0.18"},
      {:bypass, "~> 2.1", only: :test},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end
end
