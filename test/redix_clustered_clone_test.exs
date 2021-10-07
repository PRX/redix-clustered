defmodule RedixClusteredCloneTest do
  use ExUnit.Case, async: true

  import RedixClusteredClone

  alias RedixClustered.Conn

  @name :redix_clustered_clone_test
  @prefix "primary"
  @clone_prefix "cloned"

  @host System.get_env("REDIS_HOST", "127.0.0.1")
  @port System.get_env("REDIS_PORT", "6379")

  setup_all do
    clone = [host: @host, port: @port, prefix: @clone_prefix]
    opts = [name: @name, host: @host, port: @port, prefix: @prefix, clone: clone]
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

  test "clones write commands", %{key: key} do
    clone_name = cloned_cluster_name(@name)
    clone_key = "#{@clone_prefix}:#{key}"
    primary_key = "#{@prefix}:#{key}"

    assert {:ok, _} = del(@name, key)

    assert {:ok, nil} = get(@name, key)
    assert {:ok, nil} = Conn.command(@name, ["GET", primary_key])
    assert {:ok, nil} = Conn.command(clone_name, ["GET", clone_key])

    assert {:ok, _} = setex(@name, key, 100, "value")
    assert {:ok, "value"} = get(@name, key)
    assert {:ok, "value"} = Conn.command(@name, ["GET", primary_key])
    assert {:ok, "value"} = Conn.command(clone_name, ["GET", clone_key])

    assert {:ok, _} = Conn.command(@name, ["SET", primary_key, "value2"])
    assert {:ok, "value2"} = get(@name, key)
    assert {:ok, "value2"} = Conn.command(@name, ["GET", primary_key])
    assert {:ok, "value"} = Conn.command(clone_name, ["GET", clone_key])

    assert {:ok, _} = del(@name, key)
  end
end
