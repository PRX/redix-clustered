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
      {RedixClustered.Slots, name: slots_name(opts)},
      {DynamicSupervisor, name: pool_name(opts), strategy: :one_for_one}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  def cluster_name(opts) when is_list(opts), do: cluster_name(Keyword.get(opts, :name))
  def cluster_name(nil), do: :redix_clustered
  def cluster_name(name), do: :"redix_clustered_#{name}"

  def registry_name(opts), do: :"#{cluster_name(opts)}_registry"
  def slots_name(opts), do: :"#{cluster_name(opts)}_slots"
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

  # singleton/unnamed cluster
  def command(cmd), do: command(nil, cmd)
  def pipeline(cmds), do: pipeline(nil, cmds)
  def get(key), do: get(nil, key)
  def get_ttl(key), do: get_ttl(nil, key)
  def ttl(key), do: ttl(nil, key)
  def pttl(key), do: pttl(nil, key)
  def expire(key, ttl), do: expire(nil, key, ttl)
  def pexpire(key, ttl), do: pexpire(nil, key, ttl)
  def set(key, val), do: set(nil, key, val)
  def setex(key, ttl, val), do: setex(nil, key, ttl, val)
  def psetex(key, ttl, val), do: psetex(nil, key, ttl, val)
  def setnx_expire_get(key, ttl, val), do: setnx_expire_get(nil, key, ttl, val)
  def del(key), do: del(nil, key)
  def hgetall(key), do: hgetall(nil, key)
  def hincrby(key, flds) when is_map(flds), do: hincrby(nil, key, flds)
  def hincrby(key, flds, ttl) when is_map(flds), do: hincrby(nil, key, flds, ttl)
  def hincrby(key, "" <> fld, num), do: hincrby(nil, key, fld, num)
  def hincrby(key, fld, num) when is_number(fld), do: hincrby(nil, key, fld, num)
  def scan("" <> pattern), do: scan(nil, pattern)
  def scan("" <> pattern, limit) when is_number(limit), do: scan(nil, pattern, limit)
  def nuke(pattern), do: nuke(nil, pattern)

  # public interface
  defdelegate command(name, cmd), to: RedixClustered.Conn
  defdelegate pipeline(name, cmds), to: RedixClustered.Conn
  defdelegate get(name, key), to: RedixClustered.Commands
  defdelegate get_ttl(name, key), to: RedixClustered.Commands
  defdelegate ttl(name, key), to: RedixClustered.Commands
  defdelegate pttl(name, key), to: RedixClustered.Commands
  defdelegate expire(name, key, ttl), to: RedixClustered.Commands
  defdelegate pexpire(name, key, ttl), to: RedixClustered.Commands
  defdelegate set(name, key, val), to: RedixClustered.Commands
  defdelegate setex(name, key, ttl, val), to: RedixClustered.Commands
  defdelegate psetex(name, key, ttl, val), to: RedixClustered.Commands
  defdelegate setnx_expire_get(name, key, ttl, val), to: RedixClustered.Commands
  defdelegate del(name, key), to: RedixClustered.Commands
  defdelegate hgetall(name, key), to: RedixClustered.Commands
  defdelegate hincrby(name, key, flds), to: RedixClustered.Commands
  defdelegate hincrby(name, key, flds, ttl), to: RedixClustered.Commands
  defdelegate scan(name, pattern), to: RedixClustered.Commands
  defdelegate scan(name, pattern, limit), to: RedixClustered.Commands
  defdelegate nuke(name, pattern), to: RedixClustered.Commands
end
