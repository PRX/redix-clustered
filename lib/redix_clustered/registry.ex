defmodule RedixClustered.Registry do
  use GenServer

  import DynamicSupervisor, only: [start_child: 2]

  alias RedixClustered.Options
  alias RedixClustered.Slots

  @conn_pool "conn_pool"

  defmodule Conn do
    @enforce_keys [:node, :pid, :ref]
    defstruct [:node, :pid, :ref]
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, opts)
  end

  @impl true
  def init(opts) do
    {:ok, registry_name} = Keyword.fetch(opts, :name)
    :ets.new(registry_name, [:set, :protected, :named_table])
    :ets.insert(registry_name, {@conn_pool, []})
    {:ok, nil}
  end

  @impl true
  def handle_call({:connect, cluster_name, node_str}, _, _) do
    {node, host, port} = parse(node_str)
    pool_size = Options.pool_size(cluster_name)
    conn_pool = pool(cluster_name, node)

    if length(conn_pool) >= pool_size do
      {:reply, Enum.random(conn_pool).pid, nil}
    else
      pool_name = Options.pool_name(cluster_name)

      {:ok, pid} = start_child(pool_name, redix_child_spec(cluster_name, host, port))
      ref = Process.monitor(pid)
      conn = %Conn{node: node, pid: pid, ref: ref}

      registry_name = Options.registry_name(cluster_name)
      :ets.insert(registry_name, {@conn_pool, pool(cluster_name) ++ [conn]})

      {:reply, pid, cluster_name}
    end
  end

  @impl true
  def handle_call({:tick}, _, cluster_name) do
    {:reply, :ok, cluster_name}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _, _}, cluster_name) do
    conn_pool = pool(cluster_name) |> Enum.reject(&(&1.ref == ref))

    registry_name = Options.registry_name(cluster_name)
    :ets.insert(registry_name, {@conn_pool, conn_pool})

    {:noreply, cluster_name}
  end

  @impl true
  def handle_info(_msg, cluster_name), do: {:noreply, cluster_name}

  def pool(cluster_name) do
    case :ets.lookup(Options.registry_name(cluster_name), @conn_pool) do
      [{_key, val}] -> val
      _ -> []
    end
  end

  def pool(cluster_name, node), do: pool(cluster_name) |> Enum.filter(&(&1.node == node))
  def pool_empty?(cluster_name), do: pool(cluster_name) |> Enum.empty?()
  def pool_empty?(cluster_name, node), do: pool(cluster_name, node) |> Enum.empty?()
  def nodes(cluster_name), do: pool(cluster_name) |> Enum.map(& &1.node)
  def nodes(cluster_name, key), do: pool(cluster_name, key) |> Enum.map(& &1.node)

  def lookup(cluster_name, redis_key) do
    if pool_empty?(cluster_name) do
      connect(cluster_name)
    else
      node_for_key = Slots.lookup(cluster_name, redis_key)
      conn_pool = pool(cluster_name, node_for_key)
      pool_size = Options.pool_size(cluster_name)

      case {node_for_key, conn_pool} do
        {nil, _} -> connect(cluster_name)
        {_, conns} when length(conns) >= pool_size -> Enum.random(conns).pid
        _ -> connect(cluster_name, node_for_key)
      end
    end
  end

  def connect(cluster_name) do
    opts = Options.redix_opts(cluster_name)
    host = Keyword.get(opts, :host, "127.0.0.1")
    port = Keyword.get(opts, :port, "6379")
    connect(cluster_name, "#{host}:#{port}")
  end

  def connect(cluster_name, node) do
    registry_name = Options.registry_name(cluster_name)
    GenServer.call(registry_name, {:connect, cluster_name, node})
  end

  # for testing purposes, wait for the registry to finish
  def tick(cluster_name) do
    registry_name = Options.registry_name(cluster_name)
    GenServer.call(registry_name, {:tick})
  end

  defp redix_child_spec(cluster_name, host, port) do
    opts =
      Options.redix_opts(cluster_name)
      |> Keyword.put(:host, host)
      |> Keyword.put(:port, port)

    Redix.child_spec(opts) |> Map.put(:restart, :temporary)
  end

  defp parse("" <> node_str) do
    {ip, port} =
      case String.split(node_str, [" ", ":"]) do
        [_slot, ip, port] -> {ip, port_to_num(port)}
        [ip, port] -> {ip, port_to_num(port)}
      end

    {"#{ip}:#{port}", ip, port}
  end

  defp port_to_num(portnum) do
    case portnum do
      nil -> 6379
      "" -> 6379
      "" <> str -> String.to_integer(str)
      num -> num
    end
  end
end
