defmodule RedixClusteredTest do
  use ExUnit.Case
  doctest RedixClustered

  test "greets the world" do
    assert RedixClustered.hello() == :world
  end
end
