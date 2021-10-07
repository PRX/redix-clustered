defmodule RedixClustered.Registry do
  use GenServer

  import DynamicSupervisor, only: [start_child: 2]
  import RedixClustered, only: [registry_name: 1]

  defmodule State do
    @enforce_keys [:opts, :conns, :refs]
    defstruct [:opts, :conns, :refs]
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, opts)
  end

  @impl true
  def init(opts), do: {:ok, %State{opts: opts, conns: %{}, refs: %{}}}

  @impl true
  def handle_call({:nodes}, _from, %State{conns: conns} = state) do
    {:reply, Map.keys(conns), state}
  end

  @impl true
  def handle_call({:lookup, name, key}, from, %State{conns: conns} = state) do
    if Enum.empty?(conns) do
      handle_call({:connect}, from, state)
    else
      case conns[RedixClustered.Slots.lookup(name, key)] do
        nil -> {:reply, Enum.random(Map.values(conns)), state}
        conn -> {:reply, conn, state}
      end
    end
  end

  @impl true
  def handle_call({:connect}, from, %State{opts: opts} = state) do
    host = Keyword.get(opts, :host, "127.0.0.1")
    port = Keyword.get(opts, :port, "6379")
    handle_call({:connect, "#{host}:#{port}"}, from, state)
  end

  @impl true
  def handle_call({:connect, node}, _from, %State{opts: opts, conns: conns, refs: refs} = state) do
    {key, host, port} = parse(node)

    if conns[key] do
      {:reply, conns[key], state}
    else
      {:ok, pool_name} = Keyword.fetch(opts, :pool_name)
      {:ok, pid} = start_child(pool_name, redix_child_spec(host, port))
      ref = Process.monitor(pid)
      refs = Map.put(refs, ref, key)
      conns = Map.put(conns, key, pid)
      {:reply, pid, %State{opts: opts, conns: conns, refs: refs}}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _, _}, %State{opts: opts, conns: conns, refs: refs}) do
    {key, refs} = Map.pop(refs, ref)
    conns = Map.delete(conns, key)
    {:noreply, %State{opts: opts, conns: conns, refs: refs}}
  end

  @impl true
  def handle_info(_msg, state), do: {:noreply, state}

  def nodes(name), do: GenServer.call(registry_name(name), {:nodes})
  def lookup(key, name), do: GenServer.call(registry_name(name), {:lookup, name, key})
  def connect(name), do: GenServer.call(registry_name(name), {:connect})
  def connect(node, name), do: GenServer.call(registry_name(name), {:connect, node})

  def redix_child_spec(host, port) do
    Redix.child_spec(host: host, port: port) |> Map.put(:restart, :temporary)
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
