defmodule RedixClusteredCloneTest do
  use ExUnit.Case, async: true

  import RedixClusteredClone

  alias RedixClustered.Conn

  @name :redix_clustered_clone_test
  @host System.get_env("REDIS_HOST", "127.0.0.1")
  @port System.get_env("REDIS_PORT", "6379")

  setup_all do
    clone = [host: @host, port: @port, namespace: "ns2"]
    opts = [name: @name, host: @host, port: @port, namespace: "ns1", clone: clone]
    start_supervised!({RedixClusteredClone, opts})
    :ok
  end

  setup %{case: case_name, test: test_name} do
    [key: String.downcase("#{case_name}:#{test_name}") |> String.replace(" ", ".")]
  end

  test "gets cluster names" do
    assert supervisor_name([]) == :redix_clustered_super
    assert cloned_cluster_name([]) == :clone

    assert supervisor_name(name: :my_redis) == :redix_clustered_my_redis_super
    assert cloned_cluster_name(name: :my_redis) == :my_redis_clone

    assert supervisor_name(:my_redis) == :redix_clustered_my_redis_super
    assert cloned_cluster_name(:my_redis) == :my_redis_clone
  end

  test "recognizes commands that need cloning" do
    refute needs_cloning?(["anything"])
    refute needs_cloning?(["GET", "foo"])
    refute needs_cloning?(["get", "foo"])

    assert needs_cloning?(["sEt", "foo", "bar"])
    assert needs_cloning?(["del", "foo"])
  end

  test "recognizes pipelines that need cloning" do
    refute needs_cloning?([["anything"], ["GET", "foo"], ["TTL", "foo"]])
    assert needs_cloning?([["anything"], ["set", "foo"], ["TTL", "foo"]])
  end

  test "clones write commands", %{key: key} do
    clone_name = cloned_cluster_name(@name)
    clone_key = "ns2:#{key}"
    primary_key = "ns1:#{key}"

    assert {:ok, _} = command(@name, ["del", key])

    assert {:ok, nil} = command(@name, ["get", key])
    assert {:ok, nil} = Conn.command(@name, ["GET", primary_key])
    assert {:ok, nil} = Conn.command(clone_name, ["GET", clone_key])

    assert {:ok, _} = command(@name, ["setex", key, 100, "value"])
    assert {:ok, "value"} = command(@name, ["get", key])
    assert {:ok, "value"} = Conn.command(@name, ["GET", primary_key])
    assert {:ok, "value"} = Conn.command(clone_name, ["GET", clone_key])

    assert {:ok, _} = Conn.command(@name, ["SET", primary_key, "value2"])
    assert {:ok, "value2"} = command(@name, ["get", key])
    assert {:ok, "value2"} = Conn.command(@name, ["GET", primary_key])
    assert {:ok, "value"} = Conn.command(clone_name, ["GET", clone_key])

    assert {:ok, _} = command(@name, ["del", key])
  end
end
