defmodule RedixClustered.NamespaceTest do
  use ExUnit.Case, async: true

  import RedixClustered.Namespace

  @name :redis_clustered_namespace_test

  test "prefixes keys with namespace" do
    start_supervised!({RedixClustered, [name: @name, namespace: "ns"]})

    assert prefix(@name, "foo") == "ns:foo"
    assert prefix(@name, "ns:foo") == "ns:ns:foo"
    assert prefix(@name, ["foo", "ns:foo"]) == ["ns:foo", "ns:ns:foo"]

    assert unprefix(@name, "ns:ns:foo") == "ns:foo"
    assert unprefix(@name, "ns:foo") == "foo"
    assert unprefix(@name, "foo") == "foo"
    assert unprefix(@name, ["ns:ns:foo", "ns:foo", "foo"]) == ["ns:foo", "foo", "foo"]
  end

  test "prefixes keys without namespace" do
    start_supervised!({RedixClustered, [name: @name]})

    assert prefix(@name, "foo") == "foo"
    assert prefix(@name, ["foo", "bar"]) == ["foo", "bar"]

    assert unprefix(@name, "foo") == "foo"
    assert unprefix(@name, ["foo", "bar"]) == ["foo", "bar"]
  end

  test "namespaces commands" do
    start_supervised!({RedixClustered, [name: @name, namespace: "ns"]})

    assert prefix(@name, "foo") == "ns:foo"
    assert prefix(@name, "ns:foo") == "ns:ns:foo"
    assert unprefix(@name, "ns:ns:foo") == "ns:foo"
    assert unprefix(@name, "ns:foo") == "foo"
    assert unprefix(@name, "foo") == "foo"

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
