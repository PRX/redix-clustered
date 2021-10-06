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
end
