defmodule Smolquery.MixProject do
  use Mix.Project

  def project do
    [
      app: :smolquery,
      version: "0.1.0",
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      dialyzer: [plt_local_path: "priv/plts", plt_core_path: "priv/plts"],
      preferred_cli_env: [precommit: :test, ci: :test]
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Smolquery.Application, []}
    ]
  end

  defp deps do
    [
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:ex_slop, "~> 0.4", only: [:dev, :test], runtime: false},
      {:mix_audit, "~> 2.1", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_dna, "~> 1.5", only: [:dev, :test], runtime: false},
      {:reach, "~> 2.7", only: [:dev, :test], runtime: false}
    ]
  end

  defp aliases do
    [
      precommit: [
        "compile --warnings-as-errors",
        "deps.unlock --unused",
        "format",
        "credo --strict",
        "ex_dna",
        "test"
      ],
      ci: [
        "hex.audit",
        "compile --warnings-as-errors",
        "deps.unlock --check-unused",
        "format --check-formatted",
        "credo --strict",
        "deps.audit",
        "ex_dna",
        "reach.check --arch --smells --strict --baseline .reach.baseline.json"
      ]
    ]
  end
end
