defmodule RedixClusteredTest do
  use ExUnit.Case, async: true

  import RedixClustered

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
end
