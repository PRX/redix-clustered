defmodule RedixClustered.Commands do
  import RedixClustered.Conn, only: [command: 2, command: 3, pipeline: 2]
  import RedixClustered, only: [prefix: 2, unprefix: 2]

  def get(name, key), do: command(name, ["GET", prefix(name, key)])

  # get both value and ttl
  def get_ttl(name, key) do
    pipeline(name, [["GET", prefix(name, key)], ["TTL", prefix(name, key)]])
  end

  def ttl(name, key), do: command(name, ["TTL", prefix(name, key)])
  def pttl(name, key), do: command(name, ["PTTL", prefix(name, key)])
  def expire(name, key, ttl), do: command(name, ["EXPIRE", prefix(name, key), ttl])
  def pexpire(name, key, ttl), do: command(name, ["PEXPIRE", prefix(name, key), ttl])
  def set(name, key, val), do: command(name, ["SET", prefix(name, key), val])
  def setex(name, key, ttl, val), do: command(name, ["SETEX", prefix(name, key), ttl, val])
  def psetex(name, key, ttl, val), do: command(name, ["PSETEX", prefix(name, key), ttl, val])

  # set if doesn't exist, but always set the ttl and return value
  def setnx_expire_get(name, key, ttl, val) do
    pipeline(name, [
      ["SETNX", prefix(name, key), val],
      ["EXPIRE", prefix(name, key), ttl],
      ["GET", prefix(name, key)]
    ])
  end

  def del(name, key), do: command(name, ["DEL", prefix(name, key)])
  def hgetall(name, key), do: command(name, ["HGETALL", prefix(name, key)])
  def hincrby(name, key, flds), do: hincrby(name, key, flds, nil)

  # increment multiple hash keys
  def hincrby(name, key, flds, ttl) when is_map(flds) do
    incrs = Enum.map(flds, fn {fld, num} -> ["HINCRBY", prefix(name, key), fld, num] end)

    if ttl == nil do
      pipeline(name, incrs)
    else
      pipeline(name, incrs ++ [["EXPIRE", prefix(name, key), ttl]])
    end
  end

  def hincrby(name, key, fld, num), do: command(name, ["HINCRBY", prefix(name, key), fld, num])

  def scan(name, pattern), do: scan(name, pattern, nil)
  def scan(name, pattern, limit), do: scan(name, pattern, limit, nodes(name), 0, [])
  def scan(_name, _pattern, limit, _nodes, _cursor, acc) when length(acc) >= limit, do: {:ok, acc}
  def scan(_name, _pattern, _limit, [], _cursor, acc), do: {:ok, acc}

  # COMPLICATED: have to iterate over ALL nodes in cluster-mode
  def scan(name, pattern, limit, [node | rest], cursor, acc) do
    case command(name, ["SCAN", cursor, "MATCH", prefix(name, pattern), "COUNT", 1000], node) do
      {:ok, ["0", keys]} ->
        scan(name, pattern, limit, rest, 0, acc ++ unprefix(name, keys))

      {:ok, [next, keys]} ->
        scan(name, pattern, limit, [node] ++ rest, next, acc ++ unprefix(name, keys))

      err ->
        err
    end
  end

  def nuke(name, pattern) do
    case Mix.env() do
      :test ->
        {:ok, keys} = scan(name, pattern)
        Enum.each(keys, &del(name, &1))
        length(keys)
    end
  end

  defp nodes(name) do
    case command(name, ["CLUSTER", "SLOTS"]) do
      {:ok, slots} -> Enum.map(slots, &slot_to_node/1)
      _err -> [nil]
    end
  end

  defp slot_to_node([_from, _to, [ip, port, _id] | _rest]), do: "#{ip}:#{port}"
end
