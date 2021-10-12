defmodule RedixClustered.OptionsTest do
  use ExUnit.Case, async: true

  import RedixClustered.Options

  @name :redis_clustered_options_test
  @host System.get_env("REDIS_HOST", "127.0.0.1")
  @port System.get_env("REDIS_PORT", "6379")

  test "gets cluster names" do
    assert cluster_name([]) == :redix_clustered
    assert cluster_name(name: :my_redis) == :redix_clustered_my_redis
    assert cluster_name(nil) == :redix_clustered
    assert cluster_name(:my_redis) == :redix_clustered_my_redis
  end

  test "gets clone cluster names" do
    assert clone_cluster_name([]) == :clone
    assert clone_cluster_name(name: :my_redis) == :my_redis_clone
    assert clone_cluster_name(:my_redis) == :my_redis_clone
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

  test "retrieves options" do
    opts = [name: @name, host: @host, port: @port, namespace: "some-ns", pool_size: 4]
    start_supervised!({RedixClustered, opts})

    assert redix_opts(@name) == [host: @host, port: @port]
    assert pool_size(@name) == 4
    assert namespace(@name) == "some-ns"
  end

  test "has default options" do
    start_supervised!({RedixClustered, []})

    assert redix_opts(nil) == []
    assert pool_size(nil) == 1
    assert namespace(nil) == nil
  end

  test "handles empty strings" do
    start_supervised!({RedixClustered, [host: "", port: "", namespace: "", pool_size: ""]})

    assert redix_opts(nil) == []
    assert pool_size(nil) == 1
    assert namespace(nil) == nil
  end

  test "handles string port numbers" do
    start_supervised!({RedixClustered, [namespace: "", pool_size: "4"]})

    assert pool_size(nil) == 4
  end

  test "handles multiple cluster options" do
    opts1 = [name: @name, host: @host, port: @port, namespace: "ns-1"]
    opts2 = [name: "#{@name}2", host: "12.34.56.78", port: 9999, pool_size: 5]

    start_supervised!(%{id: :one, start: {RedixClustered, :start_link, [opts1]}})
    start_supervised!(%{id: :two, start: {RedixClustered, :start_link, [opts2]}})

    assert redix_opts(@name) == [host: @host, port: @port]
    assert pool_size(@name) == 1
    assert namespace(@name) == "ns-1"

    assert redix_opts("#{@name}2") == [host: "12.34.56.78", port: 9999]
    assert pool_size("#{@name}2") == 5
    assert namespace("#{@name}2") == nil
  end
end
