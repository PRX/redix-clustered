defmodule RedixClustered do
  @moduledoc """
  Redis connection (via Redix) with cluster support and more
  """

  use Supervisor

  @prefix_key "redis_prefix"

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: cluster_name(opts))
  end

  def init(opts) do
    :ets.new(cluster_name(opts), [:set, :protected, :named_table])
    :ets.insert(cluster_name(opts), {@prefix_key, Keyword.get(opts, :prefix)})

    registry_opts =
      opts
      |> Keyword.put(:name, registry_name(opts))
      |> Keyword.put(:pool_name, pool_name(opts))

    children = [
      {RedixClustered.Registry, registry_opts},
      {DynamicSupervisor, name: pool_name(opts), strategy: :one_for_one}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  def cluster_name(opts) when is_list(opts), do: cluster_name(Keyword.get(opts, :name))
  def cluster_name(nil), do: :redix_clustered
  def cluster_name(name), do: :"redix_clustered_#{name}"

  def registry_name(opts), do: :"#{cluster_name(opts)}_registry"
  def pool_name(opts), do: :"#{cluster_name(opts)}_pool"

  def prefix(name) do
    case :ets.lookup(cluster_name(name), @prefix_key) do
      [{@prefix_key, "" <> pre}] -> "#{pre}:"
      _ -> ""
    end
  end

  def prefix(name, key), do: "#{prefix(name)}#{key}"
  def unprefix(name, keys) when is_list(keys), do: Enum.map(keys, &unprefix(name, &1))

  def unprefix(name, key) do
    case prefix(name) do
      "" -> key
      pre -> String.trim_leading(key, pre)
    end
  end
end
