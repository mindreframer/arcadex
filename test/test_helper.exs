ExUnit.start()

# Exclude integration tests by default
ExUnit.configure(exclude: [:integration])

# Check if ArcadeDB is available and enable integration tests
arcadedb_url = System.get_env("ARCADEDB_URL", "http://localhost:2480")

case Req.get("#{arcadedb_url}/api/v1/ready") do
  {:ok, %{status: status}} when status in [200, 204] ->
    ExUnit.configure(exclude: [], include: [:integration])
    IO.puts("✓ ArcadeDB running - integration tests enabled")

  _ ->
    IO.puts("⚠ ArcadeDB not available - skipping integration tests")
end
