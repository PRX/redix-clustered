defmodule RedixClustered.ConnTest do
  use ExUnit.Case, async: true

  import RedixClustered.Conn

  @name :conn_test
  @host System.get_env("REDIS_HOST", "127.0.0.1")
  @port System.get_env("REDIS_PORT", "6379")

  setup_all do
    opts = [name: @name, host: @host, port: @port]
    start_supervised!({RedixClustered, opts})
    :ok
  end

  setup %{case: case_name, test: test_name} do
    [key: String.downcase("#{case_name}:#{test_name}") |> String.replace(" ", ".")]
  end

  test "gets redis command keys" do
    assert get_key(["GET", "key"]) == "key"
    assert get_key(["SETEX", "key", 123, "val"]) == "key"
    assert get_key(["ANYTHING", "key", "blah", "blah"]) == "key"
    assert get_key(["INFO"]) == nil
    assert get_key(["CLUSTER", "INFO"]) == nil
  end

  test "gets redis pipeline keys" do
    assert get_pipeline_key([["GET", "key1"]]) == "key1"
    assert get_pipeline_key([["GET", "key1"], ["GET", "key2"]]) == "key1"
    assert get_pipeline_key([["MULTI"], ["GET", "key1"]]) == "key1"
    assert get_pipeline_key([["INFO"]]) == nil
    assert get_pipeline_key([["CLUSTER", "INFO"]]) == nil
  end

  test "runs commands", %{key: key} do
    assert {:ok, _} = command(@name, ["DEL", key])
    assert {:ok, nil} = command(@name, ["GET", key])
    assert {:ok, _} = command(@name, ["SET", key, "value"])
    assert {:ok, "value"} = command(@name, ["GET", key])
    assert {:ok, _} = command(@name, ["DEL", key])
  end

  test "runs pipelines", %{key: key} do
    assert {:ok, [_, _]} = pipeline(@name, [["DEL", key], ["DEL", key]])

    cmds = [["GET", key], ["SET", key, "value"], ["GET", key]]
    assert {:ok, [nil, _, "value"]} = pipeline(@name, cmds)

    assert {:ok, [_]} = pipeline(@name, [["DEL", key]])
  end

  describe "redis cluster" do
    @describetag :cluster

    test "returns the cluster nodes" do
      command(@name, ["PING"])
      cluster_nodes = nodes(@name)
      assert length(cluster_nodes) >= 1
      assert Enum.member?(cluster_nodes, "#{@host}:#{@port}")
    end

    test "follows command redirects", %{key: key} do
      node = find_nonmatching_node(@name, key)

      {:ok, nil} = command(@name, ["GET", key], node)
      {:ok, "OK"} = command(@name, ["SET", key, "val"], node)
      {:ok, "val"} = command(@name, ["GET", key], node)
      {:ok, 1} = command(@name, ["DEL", key], node)
    end

    test "follows pipeline redirects", %{key: key} do
      node = find_nonmatching_node(@name, key)

      cmds = [
        ["GET", key],
        ["SET", key, "val"],
        ["GET", key],
        ["DEL", key]
      ]

      assert {:ok, [nil, "OK", "val", 1]} = pipeline(@name, cmds, node, 1)
    end

    test "limits the number of redirects to follow", %{key: key} do
      node = find_nonmatching_node(@name, key)

      assert {:error, msg} = command(@name, ["GET", key], node, 0)
      assert msg =~ "Max redis redirects reached after 1"
    end
  end

  # find a cluster node that DOES NOT contain this key
  defp find_nonmatching_node(name, key) do
    {:ok, num} = command(name, ["CLUSTER", "KEYSLOT", key])
    {:ok, slots} = command(name, ["CLUSTER", "SLOTS"])

    nonmatch = Enum.find(slots, fn [first, last | _rest] -> num < first || num > last end)
    [_first, _last, [ip, port, _id] | _rest] = nonmatch
    "#{ip}:#{port}"
  end
end
