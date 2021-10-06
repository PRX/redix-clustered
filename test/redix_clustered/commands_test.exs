defmodule RedixClustered.CommandsTest do
  use ExUnit.Case, async: true

  import RedixClustered.Commands

  @name :commands_test
  @host System.get_env("REDIS_HOST", "127.0.0.1")
  @port System.get_env("REDIS_PORT", "6379")

  setup_all do
    opts = [name: @name, host: @host, port: @port]
    start_supervised!({RedixClustered, opts})
    :ok
  end

  setup %{case: case_name, test: test_name} do
    key = String.downcase("#{case_name}:#{test_name}") |> String.replace(" ", ".")
    nuke(@name, "#{key}*")
    [key: key]
  end

  test "round trips keys", %{key: key} do
    assert get(@name, key) == {:ok, nil}
    assert setex(@name, key, 100, "val") == {:ok, "OK"}
    assert get(@name, key) == {:ok, "val"}
    assert ttl(@name, key) == {:ok, 100}
    assert del(@name, key) == {:ok, 1}
    assert get(@name, key) == {:ok, nil}
    assert ttl(@name, key) == {:ok, -2}
  end

  test "expires keys", %{key: key} do
    assert expire(@name, key, 200) == {:ok, 0}
    assert setex(@name, key, 100, "val") == {:ok, "OK"}
    assert expire(@name, key, 200) == {:ok, 1}
    assert ttl(@name, key) == {:ok, 200}
  end

  test "multi-gets both the value and ttl", %{key: key} do
    assert get_ttl(@name, key) == {:ok, [nil, -2]}
    setex(@name, key, 100, "val1")
    assert get_ttl(@name, key) == {:ok, ["val1", 100]}
  end

  test "increments hashes", %{key: key} do
    assert hgetall(@name, key) == {:ok, []}

    hincrby(@name, key, "val1", 1)
    hincrby(@name, key, "val1", 2)
    assert hgetall(@name, key) == {:ok, ["val1", "3"]}

    hincrby(@name, key, %{"val1" => 4, "val2" => 5})
    assert hgetall(@name, key) == {:ok, ["val1", "7", "val2", "5"]}
  end

  test "sets new items expires and gets", %{key: key} do
    assert setnx_expire_get(@name, key, 100, "val1") == {:ok, [1, 1, "val1"]}
    assert get(@name, key) == {:ok, "val1"}
    assert ttl(@name, key) == {:ok, 100}

    assert setnx_expire_get(@name, key, 200, "val2") == {:ok, [0, 1, "val1"]}
    assert get(@name, key) == {:ok, "val1"}
    assert ttl(@name, key) == {:ok, 200}
  end

  test "scans keys", %{key: key} do
    assert scan(@name, "#{key}:*") == {:ok, []}
    setex(@name, "#{key}:1", 100, "val1")
    setex(@name, "#{key}:2", 100, "val2")
    {:ok, keys} = scan(@name, "#{key}:*")
    assert length(keys) == 2
    assert Enum.member?(keys, "#{key}:1")
    assert Enum.member?(keys, "#{key}:2")
  end

  test "can nuke all keys with a prefix", %{key: key} do
    assert scan(@name, "#{key}:*") == {:ok, []}
    setex(@name, "#{key}:1", 100, "val1")
    setex(@name, "#{key}:2", 100, "val2")
    {:ok, keys} = scan(@name, "#{key}:*")
    assert length(keys) == 2
  end
end
