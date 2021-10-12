defmodule RedixClustered.RegistryTest do
  use ExUnit.Case, async: true

  alias RedixClustered.Registry

  @name :registry_test
  @host System.get_env("REDIS_HOST", "127.0.0.1")
  @port System.get_env("REDIS_PORT", "6379")
  @default_node "#{@host}:#{@port}"

  setup do
    opts = [name: @name, host: @host, port: @port, pool_size: 2]
    start_supervised!({RedixClustered, opts})
    :ok
  end

  test "opens a default connection" do
    assert Registry.nodes(@name) == []
    pid = Registry.connect(@name)
    assert Registry.nodes(@name) == [@default_node]
    assert {:ok, "PONG"} = Redix.command(pid, ["PING"])
  end

  test "pools connections to a specific node" do
    assert Registry.pool(@name) == []

    pid1 = Registry.connect(@name, @default_node)
    pid2 = Registry.connect(@name, @default_node)
    assert pid1 != pid2

    for _i <- 0..100 do
      pid = Registry.connect(@name, @default_node)
      assert pid == pid1 || pid == pid2
    end

    assert Registry.nodes(@name) == [@default_node, @default_node]
  end

  test "reopens connections" do
    pid1 = Registry.connect(@name, @default_node)
    assert Registry.nodes(@name) == [@default_node]

    Redix.stop(pid1)
    Registry.tick(@name)
    assert Process.alive?(pid1) == false
    assert Registry.nodes(@name) == []

    pid2 = Registry.connect(@name, @default_node)
    assert Registry.nodes(@name) == [@default_node]
    assert pid2 != pid1
    assert {:ok, "PONG"} = Redix.command(pid2, ["PING"])
  end

  test "gets a connection for a redis key" do
    assert Registry.nodes(@name) == []
    _pid = Registry.lookup(@name, "some-key")
    assert Registry.nodes(@name) == [@default_node]
  end
end
