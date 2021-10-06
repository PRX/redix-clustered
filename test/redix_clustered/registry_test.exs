defmodule RedixClustered.RegistryTest do
  use ExUnit.Case, async: true

  @name :registry_test
  @host System.get_env("REDIS_HOST", "127.0.0.1")
  @port System.get_env("REDIS_PORT", "6379")
  @default_node "#{@host}:#{@port}"

  setup do
    opts = [name: @name, host: @host, port: @port]
    start_supervised!({RedixClustered, opts})
    %{registry: RedixClustered.registry_name(opts)}
  end

  test "opens a default connection", %{registry: registry} do
    assert GenServer.call(registry, {:nodes}) == []
    pid = GenServer.call(registry, {:connect})
    assert GenServer.call(registry, {:nodes}) == [@default_node]
    assert {:ok, "PONG"} = Redix.command(pid, ["PING"])
  end

  test "connects to a specific node", %{registry: registry} do
    pid = GenServer.call(registry, {:connect, @default_node})

    for _i <- 0..100 do
      assert GenServer.call(registry, {:connect, @default_node}) == pid
    end

    assert GenServer.call(registry, {:nodes}) == [@default_node]
  end

  test "reopens connections", %{registry: registry} do
    pid1 = GenServer.call(registry, {:connect, @default_node})
    assert GenServer.call(registry, {:nodes}) == [@default_node]

    Redix.stop(pid1)
    assert Process.alive?(pid1) == false
    assert GenServer.call(registry, {:nodes}) == []

    pid2 = GenServer.call(registry, {:connect, @default_node})
    assert GenServer.call(registry, {:nodes}) == [@default_node]
    assert pid2 != pid1
    assert {:ok, "PONG"} = Redix.command(pid2, ["PING"])
  end

  test "gets a connection for a redis key", %{registry: registry} do
    assert GenServer.call(registry, {:nodes}) == []
    _pid = GenServer.call(registry, {:lookup, "some-key"})
    assert GenServer.call(registry, {:nodes}) == [@default_node]
  end
end
