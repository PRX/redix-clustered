defmodule RedixClustered.RegistryTest do
  use ExUnit.Case, async: true

  alias RedixClustered.Registry

  @name :registry_test
  @host System.get_env("REDIS_HOST", "127.0.0.1")
  @port System.get_env("REDIS_PORT", "6379")
  @default_node "#{@host}:#{@port}"

  setup do
    opts = [name: @name, host: @host, port: @port]
    start_supervised!({RedixClustered, opts})
    :ok
  end

  test "opens a default connection" do
    assert Registry.nodes(@name) == []
    pid = Registry.connect(@name)
    assert Registry.nodes(@name) == [@default_node]
    assert {:ok, "PONG"} = Redix.command(pid, ["PING"])
  end

  test "connects to a specific node" do
    pid = Registry.connect(@default_node, @name)

    for _i <- 0..100 do
      assert Registry.connect(@default_node, @name) == pid
    end

    assert Registry.nodes(@name) == [@default_node]
  end

  test "reopens connections" do
    pid1 = Registry.connect(@default_node, @name)
    assert Registry.nodes(@name) == [@default_node]

    Redix.stop(pid1)
    assert Process.alive?(pid1) == false
    assert Registry.nodes(@name) == []

    pid2 = Registry.connect(@default_node, @name)
    assert Registry.nodes(@name) == [@default_node]
    assert pid2 != pid1
    assert {:ok, "PONG"} = Redix.command(pid2, ["PING"])
  end

  test "gets a connection for a redis key" do
    assert Registry.nodes(@name) == []
    _pid = Registry.lookup("some-key", @name)
    assert Registry.nodes(@name) == [@default_node]
  end
end
