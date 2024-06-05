defmodule RedixClustered.Options do
  @namespace "namespace"
  @pool_size "pool_size"
  @default_pool_size 1
  @redix_opts "redix_opts"
  @redix_keys [:host, :port, :username, :password, :timeout, :ssl, :socket_opts]
  @redix_request_opts "redix_request_opts"
  @redix_request_key :request_opts

  def init(opts) do
    cluster_name = cluster_name(opts)
    :ets.new(cluster_name, [:set, :protected, :named_table])
    :ets.insert(cluster_name, {@namespace, get_non_blank(opts, :namespace)})
    :ets.insert(cluster_name, {@pool_size, get_number(opts, :pool_size, @default_pool_size)})
    :ets.insert(cluster_name, {@redix_opts, take_non_blank(opts, @redix_keys)})
    :ets.insert(cluster_name, {@redix_request_opts, Keyword.get(opts, @redix_request_key, [])})
  end

  def cluster_name(opts) when is_list(opts), do: cluster_name(Keyword.get(opts, :name))
  def cluster_name(nil), do: :redix_clustered
  def cluster_name(name), do: :"redix_clustered_#{name}"

  def clone_cluster_name(o) when is_list(o), do: clone_cluster_name(Keyword.get(o, :name))
  def clone_cluster_name(nil), do: :clone
  def clone_cluster_name(name), do: :"#{name}_clone"

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
  def redix_request_opts(name), do: get(name, @redix_request_opts)

  defp get_non_blank(opts, key) do
    case Keyword.get(opts, key) do
      "" -> nil
      val -> val
    end
  end

  defp get_number(opts, key, default) do
    case Keyword.get(opts, key, default) do
      "" -> default
      "" <> str -> String.to_integer(str)
      num -> num
    end
  end

  defp take_non_blank(opts, keys) do
    opts
    |> Keyword.take(keys)
    |> Enum.reject(fn {_key, val} ->
      is_nil(val) || val == ""
    end)
  end
end
