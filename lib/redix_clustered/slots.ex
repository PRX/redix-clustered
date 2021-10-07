defmodule RedixClustered.Slots do
  use GenServer

  import RedixClustered, only: [slots_name: 1]

  @num_slots 16384
  @slots_key "cluster_slots"
  @refresh_debounce :timer.seconds(10)

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, opts)
  end

  @impl true
  def init(opts) do
    {:ok, slots_name} = Keyword.fetch(opts, :name)
    :ets.new(slots_name, [:set, :protected, :named_table])
    {:ok, 0}
  end

  @impl true
  def handle_call({:last_refreshed}, _, time), do: {:reply, time, time}

  @impl true
  def handle_call({:set_last_refreshed, time}, _, _), do: {:reply, :ok, time}

  @impl true
  def handle_call({:set_slots, name, slots}, _, _) do
    :ets.insert(slots_name(name), {@slots_key, slots})
    {:reply, :ok, :os.system_time(:millisecond)}
  end

  @impl true
  def handle_cast({:refresh, name, conn}, last_refreshed) do
    if :os.system_time(:millisecond) - last_refreshed > @refresh_debounce do
      :ets.insert(slots_name(name), {@slots_key, get_cluster_slots(conn)})
      {:noreply, :os.system_time(:millisecond)}
    else
      {:noreply, last_refreshed}
    end
  end

  # calls into genserver, mostly for testing
  def last_refreshed(name), do: GenServer.call(slots_name(name), {:last_refreshed})
  def set_last_refreshed(n, t), do: GenServer.call(slots_name(n), {:set_last_refreshed, t})
  def set_slots(name, slots), do: GenServer.call(slots_name(name), {:set_slots, name, slots})

  # background-refresh cluster slots
  def refresh(name, conn), do: GenServer.cast(slots_name(name), {:refresh, name, conn})

  # lookup slots
  def slots(name) do
    case :ets.lookup(slots_name(name), @slots_key) do
      [{@slots_key, val}] -> val
      _ -> []
    end
  end

  # lookup the slot for a key
  def lookup(name, key), do: slots(name) |> find_node(key)

  def find_node(_nodes, nil), do: nil
  def find_node([], _key), do: nil

  def find_node(nodes, key) do
    slot = hash(key)

    Enum.find_value(nodes, fn {node, range} ->
      if slot in range do
        node
      end
    end)
  end

  def hash(key) do
    hash_key =
      case String.split(key, "{", parts: 2) do
        [_, rest] ->
          case String.split(rest, "}", parts: 2) do
            ["", _rest] -> key
            [str, _rest] -> str
            _ -> key
          end

        _ ->
          key
      end

    CRC.ccitt_16_xmodem(hash_key) |> rem(@num_slots)
  end

  def get_cluster_slots(conn) do
    case Redix.command(conn, ["CLUSTER", "SLOTS"]) do
      {:ok, slots} -> parse_cluster_slots(slots)
      _err -> []
    end
  end

  def parse_cluster_slots(slots) do
    slots
    |> Enum.map(&parse_slot/1)
    |> Enum.filter(& &1)
  end

  defp parse_slot([range_start, range_end, [ip, port, _] | _replicas]) do
    {"#{ip}:#{port}", range_start..range_end}
  end

  defp parse_slot(_), do: nil
end
