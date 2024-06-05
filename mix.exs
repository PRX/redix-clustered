defmodule RedixClustered.MixProject do
  use Mix.Project

  def project do
    [
      app: :redix_clustered,
      version: "1.1.1",
      elixir: "~> 1.12",
      name: "RedixClustered",
      source_url: "https://github.com/PRX/redix-clustered",
      homepage_url: "https://github.com/PRX/redix-clustered",
      description: description(),
      package: package(),
      docs: docs(),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:redix, "~> 1.1"},
      {:castore, ">= 0.0.0"},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false},
      {:mix_test_watch, "~> 1.0", only: :dev, runtime: false},
      {:dotenv, "~> 3.0.0", only: [:dev, :test]}
    ]
  end

  defp description do
    "Hex package to run redix with cluster support and more"
  end

  defp package do
    [
      contributors: ["Ryan Cavis"],
      maintainers: ["Ryan Cavis"],
      licenses: ["MIT"],
      links: %{github: "https://github.com/PRX/redix-clustered"},
      files: ~w(lib LICENSE mix.exs README.md)
    ]
  end

  defp docs do
    [main: "readme", extras: ["README.md"]]
  end
end
