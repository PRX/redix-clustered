defmodule RedixClusteredTest do
  use ExUnit.Case, async: true

  import RedixClustered

  @name :redis_clustered_test
  @host System.get_env("REDIS_HOST", "127.0.0.1")
  @port System.get_env("REDIS_PORT", "6379")

  setup %{case: case_name, test: test_name} do
    [key: String.downcase("#{case_name}:#{test_name}") |> String.replace(" ", ".")]
  end

  test "runs commands", %{key: key} do
    start_supervised!({RedixClustered, name: @name, host: @host, port: @port})

    assert {:ok, _} = command(@name, ["del", key])
    assert {:ok, nil} = command(@name, ["GET", key])
    assert {:ok, _} = command(@name, ["setex", key, 100, "value"])
    assert {:ok, "value"} = command(@name, ["get", key])
    assert {:ok, num} = command(@name, ["TTL", key])
    assert num == 99 || num == 100
    assert {:ok, _} = command(@name, ["del", key])
  end

  test "runs pipelines", %{key: key} do
    start_supervised!({RedixClustered, name: @name, host: @host, port: @port})

    assert {:ok, _} = pipeline(@name, [["del", key, key], ["DEL", key]])
    assert {:ok, [[]]} = pipeline(@name, [["hgetall", key]])
    assert {:ok, [_, _]} = pipeline(@name, [["hset", key, "f1", "v1"], ["hset", key, "f2", "v2"]])
    assert {:ok, [["f1", "v1", "f2", "v2"]]} = pipeline(@name, [["hgetall", key]])
    assert {:ok, _} = pipeline(@name, [["del", key, key], ["DEL", key]])
  end

  test "namespaces commands", %{key: key} do
    start_supervised!({RedixClustered, name: @name, host: @host, port: @port, namespace: "ns"})

    assert {:ok, _} = command(@name, ["del", key])
    assert {:ok, _} = command(@name, ["del", key], namespace: false)

    assert {:ok, nil} = command(@name, ["get", key])
    assert {:ok, nil} = command(@name, ["get", key], namespace: false)

    assert {:ok, _} = command(@name, ["set", key, "value"])
    assert {:ok, "value"} = command(@name, ["get", key])
    assert {:ok, nil} = command(@name, ["get", key], namespace: false)

    assert {:ok, _} = command(@name, ["del", key])
    assert {:ok, _} = command(@name, ["del", key], namespace: false)
  end

  test "namespaces pipelines", %{key: key} do
    start_supervised!({RedixClustered, name: @name, host: @host, port: @port, namespace: "ns"})

    del_cmds = [["del", key, key], ["DEL", key]]
    assert {:ok, _} = pipeline(@name, del_cmds)
    assert {:ok, _} = pipeline(@name, del_cmds, namespace: false)

    get_cmds = [["mget", key, key], ["get", key]]
    assert {:ok, [[nil, nil], nil]} = pipeline(@name, get_cmds)
    assert {:ok, [[nil, nil], nil]} = pipeline(@name, get_cmds, namespace: false)

    assert {:ok, [_, _]} = pipeline(@name, [["set", key, "v1"], ["mset", key, "v2", key, "v3"]])
    assert {:ok, [["v3", "v3"], "v3"]} = pipeline(@name, get_cmds)
    assert {:ok, [[nil, nil], nil]} = pipeline(@name, get_cmds, namespace: false)

    assert {:ok, _} = pipeline(@name, del_cmds)
    assert {:ok, _} = pipeline(@name, del_cmds, namespace: false)
  end

  test "works with unnamed clusters", %{key: key} do
    start_supervised!({RedixClustered, host: @host, port: @port, namespace: "ns"})

    assert {:ok, _} = command(["del", key])
    assert {:ok, _} = command(["del", key], namespace: false)

    assert {:ok, nil} = command(["get", key])
    assert {:ok, nil} = command(["get", key], namespace: false)

    assert {:ok, _} = pipeline([["set", key, "v1"], ["mset", key, "v2", key, "v3"]])
    assert {:ok, "v3"} = command(["get", key])
    assert {:ok, "v3"} = command(["get", "ns:#{key}"], namespace: false)
    assert {:ok, nil} = command(["get", key], namespace: false)
  end
end
