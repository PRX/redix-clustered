defmodule RedixClustered.CloneTest do
  use ExUnit.Case, async: true

  import RedixClustered

  alias RedixClustered.Clone

  @name :redix_clustered_clone_test
  @host System.get_env("REDIS_HOST", "127.0.0.1")
  @port System.get_env("REDIS_PORT", "6379")
  @opts [name: @name, host: @host, port: @port, namespace: "ns1"]
  @opts_with_clone [
    name: @name,
    host: @host,
    port: @port,
    namespace: "ns1",
    clone: [host: @host, port: @port, namespace: "ns2"]
  ]

  setup %{case: case_name, test: test_name} do
    [key: String.downcase("#{case_name}:#{test_name}") |> String.replace(" ", ".")]
  end

  test "recognizes commands that need cloning" do
    refute Clone.needs_cloning?(["anything"])
    refute Clone.needs_cloning?(["GET", "foo"])
    refute Clone.needs_cloning?(["get", "foo"])

    assert Clone.needs_cloning?(["sEt", "foo", "bar"])
    assert Clone.needs_cloning?(["del", "foo"])
  end

  test "recognizes pipelines that need cloning" do
    refute Clone.needs_cloning?([["anything"], ["GET", "foo"], ["TTL", "foo"]])
    assert Clone.needs_cloning?([["anything"], ["set", "foo"], ["TTL", "foo"]])
  end

  test "no-ops uncloned clusters", %{key: key} do
    start_supervised!({RedixClustered, @opts})

    assert {:disabled} = Clone.clone_command(@name, ["get", key])
    assert {:disabled} = Clone.clone_command(@name, ["set", key, "v1"])
    assert {:disabled} = Clone.clone_pipeline(@name, [["set", key, "v2"]])
  end

  test "no-ops readonly commands", %{key: key} do
    start_supervised!({RedixClustered, @opts_with_clone})

    assert {:readonly} = Clone.clone_command(@name, ["get", key])
    assert {:readonly} = Clone.clone_command(@name, ["ttl", key])
    assert {:readonly} = Clone.clone_pipeline(@name, [["get", key], ["anything"]])
  end

  test "clones write commands", %{key: key} do
    start_supervised!({RedixClustered, @opts_with_clone})

    primary_key = "ns1:#{key}"
    clone_key = "ns2:#{key}"

    assert {:ok, _} = command(@name, ["del", key])

    assert {:ok, nil} = command(@name, ["get", key])
    assert {:ok, nil} = command(@name, ["GET", primary_key], namespace: false)
    assert {:ok, nil} = command(@name, ["GET", clone_key], namespace: false)

    assert {:ok, _} = command(@name, ["setex", key, 100, "value"])
    assert {:ok, "value"} = command(@name, ["get", key])
    assert {:ok, "value"} = command(@name, ["GET", primary_key], namespace: false)
    assert {:ok, "value"} = command(@name, ["GET", clone_key], namespace: false)

    assert {:ok, _} = command(@name, ["SET", primary_key, "value2"], namespace: false)
    assert {:ok, "value2"} = command(@name, ["get", key])
    assert {:ok, "value2"} = command(@name, ["GET", primary_key], namespace: false)
    assert {:ok, "value"} = command(@name, ["GET", clone_key], namespace: false)
    #
    assert {:ok, _} = command(@name, ["del", key])
  end
end
