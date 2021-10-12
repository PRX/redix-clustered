defmodule RedixClustered.ScannerTest do
  use ExUnit.Case, async: true

  import RedixClustered

  @name :redis_clustered_scanner_test
  @host System.get_env("REDIS_HOST", "127.0.0.1")
  @port System.get_env("REDIS_PORT", "6379")

  setup %{case: case_name, test: test_name} do
    [key: String.downcase("#{case_name}:#{test_name}") |> String.replace(" ", ".")]
  end

  test "scans for keys", %{key: key} do
    start_supervised!({RedixClustered, name: @name, host: @host, port: @port})

    command(@name, ["set", "#{key}:one", "val1"])
    command(@name, ["set", "#{key}:two", "val2"])
    command(@name, ["set", "#{key}:three", "val3"])

    {:ok, keys} = scan(@name, "#{key}*")
    assert length(keys) == 3
    assert "#{key}:one" in keys
    assert "#{key}:two" in keys
    assert "#{key}:three" in keys

    assert nuke(@name, "#{key}:*") == 3
  end

  test "scans for namespaced keys", %{key: key} do
    start_supervised!({RedixClustered, name: @name, host: @host, port: @port, namespace: "ns"})

    command(@name, ["set", "#{key}:one", "val1"])
    command(@name, ["set", "#{key}:two", "val2"])
    command(@name, ["set", "#{key}:three", "val3"])

    assert {:ok, keys} = scan(@name, "#{key}*")
    assert length(keys) == 3
    assert "#{key}:one" in keys
    assert "#{key}:two" in keys
    assert "#{key}:three" in keys

    assert {:ok, []} = scan(@name, "#{key}*", namespace: false)

    assert {:ok, keys} = scan(@name, "ns:#{key}*", namespace: false)
    assert length(keys) == 3
    assert "ns:#{key}:one" in keys
    assert "ns:#{key}:two" in keys
    assert "ns:#{key}:three" in keys

    assert nuke(@name, "#{key}:*") == 3
  end

  test "limits keys scanned", %{key: key} do
    start_supervised!({RedixClustered, name: @name, host: @host, port: @port})

    for i <- 1..10 do
      command(@name, ["set", "#{key}:#{i}", "val#{i}"])
    end

    {:ok, keys} = scan(@name, "#{key}*", limit: 3)
    assert length(keys) == 3
    assert Enum.at(keys, 0) |> String.starts_with?(key)
    assert Enum.at(keys, 1) |> String.starts_with?(key)
    assert Enum.at(keys, 2) |> String.starts_with?(key)

    assert nuke(@name, "#{key}:*") == 10
  end
end
