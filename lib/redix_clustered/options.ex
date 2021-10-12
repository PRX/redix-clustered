defmodule RedixClustered.Options do
  @namespace "namespace"
  @pool_size "pool_size"
  @default_pool_size 1
  @redix_opts "redix_opts"
  @redix_keys [:host, :port, :username, :password, :timeout]

  def init(opts) do
    cluster_name = cluster_name(opts)
    :ets.new(cluster_name, [:set, :protected, :named_table])
    :ets.insert(cluster_name, {@namespace, Keyword.get(opts, :namespace)})
    :ets.insert(cluster_name, {@pool_size, Keyword.get(opts, :pool_size, @default_pool_size)})
    :ets.insert(cluster_name, {@redix_opts, Keyword.take(opts, @redix_keys)})
  end

  def cluster_name(opts) when is_list(opts), do: cluster_name(Keyword.get(opts, :name))
  def cluster_name(nil), do: :redix_clustered
  def cluster_name(name), do: :"redix_clustered_#{name}"

  def registry_name(opts), do: :"#{cluster_name(opts)}_registry"
  def slots_name(opts), do: :"#{cluster_name(opts)}_slots"
  def pool_name(opts), do: :"#{cluster_name(opts)}_pool"

  def get(name, key) do
    case :ets.lookup(cluster_name(name), key) do
      [{_key, val}] -> val
      _ -> nil
    end
  end

  def namespace(name), do: get(name, @namespace)
  def pool_size(name), do: get(name, @pool_size)
  def redix_opts(name), do: get(name, @redix_opts)
end
