defmodule RedixClustered.Scanner do
  alias RedixClustered.Conn
  alias RedixClustered.Namespace

  @max_scan_limit 1000

  # handle scanning a redis cluster, including prefixes and multiple-nodes
  def scan(pattern), do: scan(nil, pattern, [])
  def scan(pattern, opts) when is_list(opts), do: scan(nil, pattern, opts)
  def scan(cluster_name, pattern), do: scan(cluster_name, pattern, [])

  def scan(cluster_name, pattern, opts) do
    scan_nodes(cluster_name, pattern, opts, nodes(cluster_name), 0, [])
  end

  def nuke(pattern), do: nuke(nil, pattern, [])
  def nuke(pattern, opts) when is_list(opts), do: nuke(nil, pattern, opts)
  def nuke(cluster_name, pattern), do: nuke(cluster_name, pattern, [])

  def nuke(cluster_name, pattern, opts) do
    case Mix.env() do
      :test ->
        {:ok, keys} = scan(cluster_name, pattern, opts)
        Enum.each(keys, &Conn.command(cluster_name, ["DEL", &1]))
        length(keys)
    end
  end

  defp scan_nodes(_name, _pattern, _opts, [], _cursor, acc), do: {:ok, acc}

  # COMPLICATED: have to iterate over ALL nodes in cluster-mode
  defp scan_nodes(name, pattern, opts, [node | rest], cursor, acc) do
    if length(acc) >= Keyword.get(opts, :limit, Infinity) do
      {:ok, Enum.take(acc, Keyword.get(opts, :limit))}
    else
      key = prefix(name, pattern, opts)
      scan_limit = Enum.min([Keyword.get(opts, :limit, @max_scan_limit), @max_scan_limit])

      case Conn.command(name, ["SCAN", cursor, "MATCH", key, "COUNT", scan_limit], node) do
        {:ok, ["0", keys]} ->
          scan_nodes(name, pattern, opts, rest, 0, acc ++ unprefix(name, keys, opts))

        {:ok, [next, keys]} ->
          scan_nodes(name, pattern, opts, [node] ++ rest, next, acc ++ unprefix(name, keys, opts))

        err ->
          err
      end
    end
  end

  defp prefix(cluster_name, key, opts) do
    if Keyword.get(opts, :namespace, true) do
      Namespace.prefix(cluster_name, key)
    else
      key
    end
  end

  defp unprefix(cluster_name, key, opts) do
    if Keyword.get(opts, :namespace, true) do
      Namespace.unprefix(cluster_name, key)
    else
      key
    end
  end

  defp nodes(cluster_name) do
    case Conn.command(cluster_name, ["CLUSTER", "SLOTS"]) do
      {:ok, slots} -> Enum.map(slots, &slot_to_node/1)
      _err -> [nil]
    end
  end

  defp slot_to_node([_from, _to, [ip, port, _id] | _rest]), do: "#{ip}:#{port}"
end
