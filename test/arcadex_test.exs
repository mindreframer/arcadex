defmodule ArcadexTest do
  use ExUnit.Case
  doctest Arcadex

  test "greets the world" do
    assert Arcadex.hello() == :world
  end
end
