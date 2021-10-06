defmodule RedixClustered.Conn do
  @max_redirects 5

  alias RedixClustered.Registry

  def nodes(name), do: Registry.nodes(name)

  def command(name, cmd), do: command(name, cmd, @max_redirects)
  def command(name, cmd, "" <> node), do: command(name, cmd, node, @max_redirects)

  def command(name, cmd, max_redirects) do
    get_key(cmd)
    |> Registry.lookup(name)
    |> follow_redirects(name, &Redix.command/2, cmd, max_redirects)
  end

  def command(name, cmd, node, max_redirects) do
    node
    |> Registry.connect(name)
    |> follow_redirects(name, &Redix.command/2, cmd, max_redirects)
  end

  def pipeline(name, cmds), do: pipeline(name, cmds, @max_redirects)
  def pipeline(name, cmds, "" <> node), do: pipeline(name, cmds, node, @max_redirects)

  def pipeline(name, cmds, max_redirects) do
    get_pipeline_key(cmds)
    |> Registry.lookup(name)
    |> follow_redirects(name, &Redix.pipeline/2, cmds, max_redirects)
  end

  def pipeline(name, cmds, node, max_redirects) do
    node
    |> Registry.connect(name)
    |> follow_redirects(name, &Redix.pipeline/2, cmds, max_redirects)
  end

  def get_key(["CLUSTER" | _]), do: nil
  def get_key([_, key | _]), do: key
  def get_key(_), do: nil

  def get_pipeline_key([first_cmd | _rest_cmds]), do: get_key(first_cmd)
  def get_pipeline_key(_), do: nil

  defp follow_redirects(pid, name, redix_fn, args, max) do
    follow_redirects(pid, name, redix_fn, args, max, 0)
  end

  defp follow_redirects(_pid, _name, _redix_fn, _args, max, attempt) when attempt > max do
    {:error, "Max redis redirects reached after #{attempt}"}
  end

  defp follow_redirects(pid, name, redix_fn, args, max, attempt) do
    # TODO: ASK support
    case redix_fn.(pid, args) do
      {:error, %Redix.Error{message: "MOVED " <> moved}} ->
        moved
        |> Registry.connect(name)
        |> follow_redirects(name, redix_fn, args, max, attempt + 1)

      # ASSUME pipeline errors are attempting to set the same key. which is a
      # terrible assumption, but i'm a terrible person.
      {:ok, [%Redix.Error{message: "MOVED " <> moved} | _rest]} ->
        moved
        |> Registry.connect(name)
        |> follow_redirects(name, redix_fn, args, max, attempt + 1)

      result ->
        result
    end
  end
end
