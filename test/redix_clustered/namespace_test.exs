defmodule RedixClustered.OptionsTest do
  use ExUnit.Case, async: true

  import RedixClustered.Namespace

  @name :redis_clustered_namespace_test

  test "namespaces commands" do
    start_supervised!({RedixClustered, [name: @name, namespace: "ns"]})

    assert add(@name, ["GET", "foo"]) == ["GET", "ns:foo"]
    assert add(@name, ["set", "foo", "bar"]) == ["set", "ns:foo", "bar"]
    assert add(@name, ["hSeT", "a", "b", "c"]) == ["hSeT", "ns:a", "b", "c"]
    assert add(@name, ["del", "one", "two", "three"]) == ["del", "ns:one", "ns:two", "ns:three"]
    assert add(@name, ["MSET", "a", "b", "c", "d"]) == ["MSET", "ns:a", "b", "ns:c", "d"]
    assert add(@name, ["cluster info"]) == ["cluster info"]
    assert add(@name, ["anything"]) == ["anything"]
  end

  test "handles lack of namespace" do
    start_supervised!({RedixClustered, [name: @name]})

    assert add(@name, ["GET", "foo"]) == ["GET", "foo"]
    assert add(@name, ["set", "foo", "bar"]) == ["set", "foo", "bar"]
    assert add(@name, ["hSeT", "a", "b", "c"]) == ["hSeT", "a", "b", "c"]
    assert add(@name, ["del", "one", "two", "three"]) == ["del", "one", "two", "three"]
    assert add(@name, ["MSET", "a", "b", "c", "d"]) == ["MSET", "a", "b", "c", "d"]
    assert add(@name, ["cluster info"]) == ["cluster info"]
    assert add(@name, ["anything"]) == ["anything"]
  end
end
