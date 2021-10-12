defmodule RedixClustered.Clone do
  @moduledoc """
  Conditionally clone write requests to a separate RedixClustered
  """

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

  def needs_cloning?([command | rest]) when is_list(command) do
    needs_cloning?(command) || needs_cloning?(rest)
  end

  def needs_cloning?(["" <> cmd | _args]) do
    String.upcase(cmd) in @clone_commands
  end

  def needs_cloning?(_), do: false

  def clone_alive?(cluster_name) do
    Options.clone_cluster_name(cluster_name) |> RedixClustered.alive?()
  end

  def clone_command(cluster_name, cmd, opts \\ []) do
    case {clone_alive?(cluster_name), needs_cloning?(cmd)} do
      {false, _} ->
        {:disabled}

      {_, false} ->
        {:readonly}

      _ ->
        clone_cluster_name = Options.clone_cluster_name(cluster_name)
        RedixClustered.command(clone_cluster_name, cmd, opts)
    end
  end

  def clone_pipeline(cluster_name, cmds, opts \\ []) do
    case {clone_alive?(cluster_name), needs_cloning?(cmds)} do
      {false, _} ->
        {:disabled}

      {_, false} ->
        {:readonly}

      _ ->
        clone_cluster_name = Options.clone_cluster_name(cluster_name)
        RedixClustered.pipeline(clone_cluster_name, cmds, opts)
    end
  end
end
