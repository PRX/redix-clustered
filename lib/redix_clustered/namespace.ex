defmodule RedixClustered.Namespace do
  alias RedixClustered.Options

  @prefix_first_arg [
    "APPEND",
    "DECR",
    "DECRBY",
    "EXPIRE",
    "EXPIREAT",
    "GET",
    "GETSET",
    "HDEL",
    "HEXISTS",
    "HGET",
    "HGETALL",
    "HINCRBY",
    "HINCRBYFLOAT",
    "HKEYS",
    "HLEN",
    "HMGET",
    "HMSET",
    "HSET",
    "HSETNX",
    "HVALS",
    "INCR",
    "INCRBY",
    "INCRBYFLOAT",
    "PEXPIRE",
    "PEXPIREAT",
    "PSETEX",
    "PTTL",
    "SET",
    "SETEX",
    "SETNX",
    "SETRANGE",
    "STRLEN",
    "TTL",
    "TYPE"
  ]

  @prefix_all_args [
    "DEL",
    "EXISTS",
    "MGET",
    "UNLINK"
  ]

  @prefix_every_other_arg [
    "MSET",
    "MSETNX"
  ]

  def add(cluster_name, command) do
    with [cmd | args] <- command do
      case String.upcase(cmd) do
        up when up in @prefix_first_arg ->
          [first_arg | rest] = args
          [cmd, prefix(cluster_name, first_arg)] ++ rest

        up when up in @prefix_all_args ->
          [cmd] ++ prefix(cluster_name, args)

        up when up in @prefix_every_other_arg ->
          [cmd] ++ prefix_chunks(cluster_name, args)

        _ ->
          command
      end
    else
      _ -> command
    end
  end

  def prefix(_name, []), do: []
  def prefix(name, [key | rest]), do: [prefix(name, key)] ++ prefix(name, rest)

  def prefix(name, key) do
    case Options.namespace(name) do
      "" <> namespace -> "#{namespace}:#{key}"
      _ -> key
    end
  end

  defp prefix_chunks(_name, []), do: []

  defp prefix_chunks(name, [key, value | rest]) do
    [prefix(name, key), value] ++ prefix_chunks(name, rest)
  end
end
