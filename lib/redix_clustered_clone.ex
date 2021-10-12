defmodule RedixClusteredClone do
  @moduledoc """
  Wrapper on top of RedixClustered to clone writes to a separate cluster
  """

  use Supervisor

  alias RedixClustered.Options

  @clone_commands [
    "APPEND",
    "DECR",
    "DECRBY",
    "DEL",
    "EXPIRE",
    "EXPIREAT",
    "GETSET",
    "HDEL",
    "HINCRBY",
    "HINCRBYFLOAT",
    "HMSET",
    "HSET",
    "HSETNX",
    "INCR",
    "INCRBY",
    "INCRBYFLOAT",
    "MSET",
    "MSETNX",
    "PEXPIRE",
    "PEXPIREAT",
    "PSETEX",
    "PTTL",
    "SET",
    "SETEX",
    "SETNX",
    "SETRANGE",
    "UNLINK"
  ]

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: supervisor_name(opts))
  end

  def init(opts) do
    {:ok, clone} = Keyword.fetch(opts, :clone)

    clone_opts = Keyword.put(clone, :name, cloned_cluster_name(opts))

    children = [
      %{id: :primary, start: {RedixClustered, :start_link, [opts]}},
      %{id: :clone, start: {RedixClustered, :start_link, [clone_opts]}}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def supervisor_name(opts), do: :"#{Options.cluster_name(opts)}_super"

  def cloned_cluster_name(o) when is_list(o), do: cloned_cluster_name(Keyword.get(o, :name))
  def cloned_cluster_name(nil), do: :clone
  def cloned_cluster_name(name), do: :"#{name}_clone"

  def command(cmd), do: command(nil, cmd, [])
  def command(cmd, opts) when is_list(cmd) and is_list(opts), do: command(nil, cmd, opts)

  def command(cluster_name, cmd, opts \\ []) do
    if needs_cloning?(cmd) do
      RedixClustered.command(cloned_cluster_name(cluster_name), cmd, opts)
    end

    RedixClustered.command(cluster_name, cmd, opts)
  end

  def pipeline(cmd), do: pipeline(nil, cmd, [])
  def pipeline(cmd, opts) when is_list(cmd) and is_list(opts), do: pipeline(nil, cmd, opts)

  def pipeline(cluster_name, cmds, opts \\ []) do
    if needs_cloning?(cmds) do
      RedixClustered.pipeline(cloned_cluster_name(cluster_name), cmds, opts)
    end

    RedixClustered.pipeline(cluster_name, cmds, opts)
  end

  def needs_cloning?([command | rest]) when is_list(command) do
    needs_cloning?(command) || needs_cloning?(rest)
  end

  def needs_cloning?(["" <> cmd | _args]) do
    String.upcase(cmd) in @clone_commands
  end

  def needs_cloning?(_), do: false
end
