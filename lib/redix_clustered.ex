defmodule RedixClustered do
  @moduledoc """
  Redis connection (via Redix) with cluster support and more
  """

  use Supervisor

  alias RedixClustered.Options
  alias RedixClustered.Namespace
  alias RedixClustered.Conn

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: Options.cluster_name(opts))
  end

  @impl true
  def init(opts) do
    Options.init(opts)

    children = [
      {RedixClustered.Registry, name: Options.registry_name(opts)},
      {RedixClustered.Slots, name: Options.slots_name(opts)},
      {DynamicSupervisor, name: Options.pool_name(opts), strategy: :one_for_one}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  def command(cmd), do: command(nil, cmd, [])
  def command(cmd, opts) when is_list(cmd) and is_list(opts), do: command(nil, cmd, opts)

  def command(cluster_name, cmd, opts \\ []) do
    Conn.command(cluster_name, add_namespace(cluster_name, cmd, opts))
  end

  def pipeline(cmd), do: pipeline(nil, cmd, [])
  def pipeline(cmd, opts) when is_list(cmd) and is_list(opts), do: pipeline(nil, cmd, opts)

  def pipeline(cluster_name, cmds, opts \\ []) do
    Conn.pipeline(cluster_name, add_namespaces(cluster_name, cmds, opts))
  end

  defdelegate scan(pattern), to: RedixClustered.Scanner
  defdelegate scan(name, pattern), to: RedixClustered.Scanner
  defdelegate scan(name, pattern, opts), to: RedixClustered.Scanner
  defdelegate nuke(pattern), to: RedixClustered.Scanner
  defdelegate nuke(name, pattern), to: RedixClustered.Scanner
  defdelegate nuke(name, pattern, opts), to: RedixClustered.Scanner

  defp add_namespaces(cluster_name, cmds, opts) do
    Enum.map(cmds, &add_namespace(cluster_name, &1, opts))
  end

  defp add_namespace(cluster_name, cmd, opts) do
    if Keyword.get(opts, :namespace, true) do
      Namespace.add(cluster_name, cmd)
    else
      cmd
    end
  end
end
