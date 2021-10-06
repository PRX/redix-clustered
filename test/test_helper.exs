Dotenv.load()

# check if we have clustered redis running
host = System.get_env("REDIS_HOST", "127.0.0.1")
port = System.get_env("REDIS_PORT", "6379")
{:ok, conn} = Redix.start_link(host: host, port: String.to_integer(port))

with {:error, _err} <- Redix.command(conn, ["CLUSTER", "INFO"]) do
  ExUnit.configure(exclude: [:cluster])
end

Process.exit(conn, :normal)

ExUnit.start()
