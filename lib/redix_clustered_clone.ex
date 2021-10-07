defmodule RedixClusteredClone do
  @moduledoc """
  Wrapper on top of RedixClustered to clone writes to a separate cluster
  """

  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: supervisor_name(opts))
  end

  def init(opts) do
    {:ok, clone} = Keyword.fetch(opts, :clone)

    clone_opts =
      clone
      |> Keyword.put(:name, cloned_cluster_name(opts))
      |> Keyword.put_new(:prefix, Keyword.get(opts, :prefix))

    children = [
      %{id: :primary, start: {RedixClustered, :start_link, [opts]}},
      %{id: :clone, start: {RedixClustered, :start_link, [clone_opts]}}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def supervisor_name(opts), do: :"#{RedixClustered.cluster_name(opts)}_super"

  def cloned_cluster_name(o) when is_list(o), do: cloned_cluster_name(Keyword.get(o, :name))
  def cloned_cluster_name(nil), do: :clone
  def cloned_cluster_name(name), do: :"#{name}_clone"

  # pass through direct calls
  defdelegate command(cmd), to: RedixClustered
  defdelegate command(name, cmd), to: RedixClustered
  defdelegate pipeline(cmds), to: RedixClustered
  defdelegate pipeline(name, cmds), to: RedixClustered

  # pass through read commands
  defdelegate get(key), to: RedixClustered
  defdelegate get(name, key), to: RedixClustered
  defdelegate get_ttl(key), to: RedixClustered
  defdelegate get_ttl(name, key), to: RedixClustered
  defdelegate ttl(key), to: RedixClustered
  defdelegate ttl(name, key), to: RedixClustered
  defdelegate pttl(key), to: RedixClustered
  defdelegate pttl(name, key), to: RedixClustered
  defdelegate hgetall(key), to: RedixClustered
  defdelegate hgetall(name, key), to: RedixClustered
  defdelegate scan(pattern), to: RedixClustered
  defdelegate scan(name, pattern), to: RedixClustered

  # duplicate write commands to clone cluster
  def expire(key, ttl), do: expire(nil, key, ttl)

  def expire(name, key, ttl) do
    RedixClustered.expire(cloned_cluster_name(name), key, ttl)
    RedixClustered.expire(name, key, ttl)
  end

  def pexpire(key, ttl), do: pexpire(nil, key, ttl)

  def pexpire(name, key, ttl) do
    RedixClustered.pexpire(cloned_cluster_name(name), key, ttl)
    RedixClustered.pexpire(name, key, ttl)
  end

  def set(key, val), do: set(nil, key, val)

  def set(name, key, val) do
    RedixClustered.set(cloned_cluster_name(name), key, val)
    RedixClustered.set(name, key, val)
  end

  def setex(key, ttl, val), do: setex(nil, key, ttl, val)

  def setex(name, key, ttl, val) do
    RedixClustered.setex(cloned_cluster_name(name), key, ttl, val)
    RedixClustered.setex(name, key, ttl, val)
  end

  def psetex(key, ttl, val), do: psetex(nil, key, ttl, val)

  def psetex(name, key, ttl, val) do
    RedixClustered.psetex(cloned_cluster_name(name), key, ttl, val)
    RedixClustered.psetex(name, key, ttl, val)
  end

  def setnx_expire_get(key, ttl, val), do: setnx_expire_get(nil, key, ttl, val)

  def setnx_expire_get(name, key, ttl, val) do
    RedixClustered.setnx_expire_get(cloned_cluster_name(name), key, ttl, val)
    RedixClustered.setnx_expire_get(name, key, ttl, val)
  end

  def del(key), do: del(nil, key)

  def del(name, key) do
    RedixClustered.del(cloned_cluster_name(name), key)
    RedixClustered.del(name, key)
  end

  def hincrby(key, flds) when is_map(flds), do: hincrby(nil, key, flds)
  def hincrby(key, flds, ttl) when is_map(flds), do: hincrby(nil, key, flds, ttl)
  def hincrby(key, "" <> fld, num), do: hincrby(nil, key, fld, num)

  def hincrby(name, key, flds) do
    RedixClustered.hincrby(cloned_cluster_name(name), key, flds)
    RedixClustered.hincrby(name, key, flds)
  end

  def hincrby(name, key, flds, ttl) do
    RedixClustered.hincrby(cloned_cluster_name(name), key, flds, ttl)
    RedixClustered.hincrby(name, key, flds, ttl)
  end

  def nuke(pattern), do: nuke(nil, pattern)

  def nuke(name, pattern) do
    RedixClustered.nuke(cloned_cluster_name(name), pattern)
    RedixClustered.nuke(name, pattern)
  end
end
