defmodule RedixClustered.Conn do
  @max_redirects 5

  alias RedixClustered.Options
  alias RedixClustered.Registry
  alias RedixClustered.Slots

  def nodes(name), do: Registry.nodes(name)

  def command(name, cmd), do: command(name, cmd, @max_redirects)
  def command(name, cmd, "" <> node), do: command(name, cmd, node, @max_redirects)

  def command(name, cmd, max_redirects) do
    get_key(cmd)
    |> registry_lookup(name)
    |> follow_redirects(name, &Redix.command/3, cmd, max_redirects)
  end

  def command(name, cmd, node, max_redirects) do
    node
    |> registry_connect(name)
    |> follow_redirects(name, &Redix.command/3, cmd, max_redirects)
  end

  def pipeline(name, cmds), do: pipeline(name, cmds, @max_redirects)
  def pipeline(name, cmds, "" <> node), do: pipeline(name, cmds, node, @max_redirects)

  def pipeline(name, cmds, max_redirects) do
    get_pipeline_key(cmds)
    |> registry_lookup(name)
    |> follow_redirects(name, &Redix.pipeline/3, cmds, max_redirects)
  end

  def pipeline(name, cmds, node, max_redirects) do
    node
    |> registry_connect(name)
    |> follow_redirects(name, &Redix.pipeline/3, cmds, max_redirects)
  end

  def get_key(["CLUSTER" | _]), do: nil
  def get_key([_, key | _]), do: key
  def get_key(_), do: nil

  def get_pipeline_key([["MULTI"] | rest_cmds]), do: get_pipeline_key(rest_cmds)
  def get_pipeline_key([first_cmd | _rest_cmds]), do: get_key(first_cmd)
  def get_pipeline_key(_), do: nil

  defp registry_lookup(key, cluster_name), do: Registry.lookup(cluster_name, key)
  defp registry_connect(node, cluster_name), do: Registry.connect(cluster_name, node)

  defp follow_redirects(pid, name, redix_fn, args, max) do
    follow_redirects(pid, name, redix_fn, args, max, 0)
  end

  defp follow_redirects(_pid, _name, _redix_fn, _args, max, attempt) when attempt > max do
    {:error, "Max redis redirects reached after #{attempt}"}
  end

  defp follow_redirects(pid, name, redix_fn, args, max, attempt) do
    # TODO: ASK support
    # TODO: explicit request options passed with each call
    case redix_fn.(pid, args, Options.redix_request_opts(name)) do
      {:error, %Redix.Error{message: "MOVED " <> moved}} ->
        refresh_and_follow(pid, name, redix_fn, args, max, attempt, moved)

      # ASSUME pipeline errors are attempting to set the same key. which is a
      # terrible assumption, but i'm a terrible person.
      {:ok, [%Redix.Error{message: "MOVED " <> moved} | _rest]} ->
        refresh_and_follow(pid, name, redix_fn, args, max, attempt, moved)

      result ->
        result
    end
  end

  defp refresh_and_follow(pid, name, redix_fn, args, max, attempt, moved) do
    Slots.refresh(name, pid)

    moved
    |> registry_connect(name)
    |> follow_redirects(name, redix_fn, args, max, attempt + 1)
  end
end
