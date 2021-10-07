# RedixClustered

Cluster support for Redix, and other stuff! Currently:

1. Very WIP
2. Needs documentation
3. Overall probably too opinionated

Ported from some other PRX applications, and working to improve.

## Installation

Add `redix_clustered` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:redix_clustered, "~> 0.1.0"}
  ]
end
```

Then just add your cluster as a child of your application:

```elixir
children = [
  {RedixClustered, name: :my_redis, host: "127.0.0.1", port: 6379, prefix: "anything"}
]
```

And then you can run commands by referencing your app name:

```elixir
RedixClustered.get(:my_redis, "some-key")
RedixClustered.set(:my_redis, "some-key", "some-value")

# or if you left your cluster with name: nil
RedixCluster.get("some-key")
RedixCluster.set("some-key")
```

Or if you want to clone set commands to a 2nd redis cluster:

```elixir
clone = [host: "127.0.0.2", port: 6380]

children = [
  {RedixClustered, host: "127.0.0.1", port: 6379, prefix: "anything", clone: clone}
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
