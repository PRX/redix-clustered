# RedixClustered

Cluster support for Redix, and other stuff! Currently:

1. Very WIP
2. Needs documentation
3. The pipelines aren't smart enough to deal with keys on different nodes

Ported from some other PRX applications, and working to improve.

## Installation

Add `redix_clustered` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:redix_clustered, "~> 1.0.0"}
  ]
end
```

Then just add your cluster as a child of your application:

```elixir
children = [
  {RedixClustered, host: "127.0.0.1", port: 6379, namespace: "my-ns"}
]
```

Options you can pass to the RedixClustered spec:

- `host` the hostname or IP of your redis cluster (default `"127.0.0.1"`)
- `port` the port of your redis cluster (default `6379`)
- `username` passed to Redix
- `password` passed to Redix
- `timeout` passed to Redix
- `name` optional name used to access your cluster, and also the supervision [:name](https://hexdocs.pm/elixir/1.12/Supervisor.html#start_link/2)
- `namespace` optional prefix to add to your redis keys
- `pool_size` the number of Redix connections to establish per node (default `1`)

And then you can run commands/pipelines:

```elixir
{:ok, _pid} = RedixClustered.start_link()
{:ok, _pid} = RedixClustered.start_link(name: :red2, namespace: "ns2")

RedixClustered.command(["set", "foo", "val1"])
# {:ok, "OK"}

RedixClustered.command(:red2, ["set", "foo", "val2"])
# {:ok, "OK"}

RedixClustered.command(["get", "foo"])
# {:ok, "val1"}

RedixClustered.command(:red2, ["get", "foo"])
# {:ok, "val2"}

RedixClustered.command(:red2, ["get", "ns2:foo"], namespace: false)
# {:ok, "val2"}
```

Or if you want to clone set commands to a 2nd redis cluster:

```elixir
clone = [host: "127.0.0.2", port: 6380]

children = [
  {RedixClusteredClone, host: "127.0.0.1", namespace: "ns1", clone: clone}
]
```

## License

[MIT License](LICENSE)

## Contributing

1. Fork it
2. Create your feature branch (git checkout -b my-new-feature)
3. Commit your changes (git commit -am 'Add some feature')
4. Push to the branch (git push origin my-new-feature)
5. Create new Pull Request
