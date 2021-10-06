defmodule RedixClusteredTest do
  use ExUnit.Case, async: true

  import RedixClustered

  @name :redis_clustered_test
  @host System.get_env("REDIS_HOST", "127.0.0.1")
  @port System.get_env("REDIS_PORT", "6379")

  setup %{case: case_name, test: test_name} do
    [key: String.downcase("#{case_name}:#{test_name}") |> String.replace(" ", ".")]
  end

  test "gets cluster names" do
    assert cluster_name([]) == :redix_clustered
    assert cluster_name(name: :my_redis) == :redix_clustered_my_redis
    assert cluster_name(nil) == :redix_clustered
    assert cluster_name(:my_redis) == :redix_clustered_my_redis
  end

  test "gets registry names" do
    assert registry_name([]) == :redix_clustered_registry
    assert registry_name(name: :my_redis) == :redix_clustered_my_redis_registry
    assert registry_name(nil) == :redix_clustered_registry
    assert registry_name(:my_redis) == :redix_clustered_my_redis_registry
  end

  test "gets pool names" do
    assert pool_name([]) == :redix_clustered_pool
    assert pool_name(name: :my_redis) == :redix_clustered_my_redis_pool
    assert pool_name(nil) == :redix_clustered_pool
    assert pool_name(:my_redis) == :redix_clustered_my_redis_pool
  end

  test "handles prefixes" do
    start_supervised!({RedixClustered, name: :with_prefix, prefix: "some-thing"})

    assert prefix(:with_prefix) == "some-thing:"
    assert prefix(:with_prefix, "mykey") == "some-thing:mykey"
    assert unprefix(:with_prefix, "some-thing:mykey") == "mykey"
    assert unprefix(:with_prefix, "mykey") == "mykey"
  end

  test "handles non-prefixes" do
    start_supervised!({RedixClustered, name: :no_prefix})

    assert prefix(:no_prefix) == ""
    assert prefix(:no_prefix, "mykey") == "mykey"
    assert unprefix(:no_prefix, "mykey") == "mykey"
  end

  test "delegates commands", %{key: key} do
    start_supervised!({RedixClustered, name: @name, host: @host, port: @port})

    assert {:ok, _} = del(@name, key)
    assert {:ok, nil} = get(@name, key)
    assert {:ok, _} = setex(@name, key, 100, "value")
    assert {:ok, ["value", num]} = get_ttl(@name, key)
    assert num == 99 || num == 100
    assert {:ok, _} = del(@name, key)
  end
end
