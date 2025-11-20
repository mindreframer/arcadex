ExUnit.start()

# Start Finch for tests
{:ok, _} = Finch.start_link(name: Arcadex.Finch)
