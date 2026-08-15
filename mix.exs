defmodule Smolquery.MixProject do
  use Mix.Project

  def project do
    [
      app: :smolquery,
      version: "0.8.0",
      elixir: "~> 1.20",
      package: [licenses: ["Apache-2.0"]],
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      compilers: [:phoenix_live_view] ++ Mix.compilers(),
      listeners: [Phoenix.CodeReloader],
      deps: deps(),
      aliases: aliases(),
      dialyzer: [
        plt_local_path: "priv/plts",
        plt_core_path: "priv/plts",
        plt_add_apps: [:ex_unit, :aws_credentials]
      ],
      releases: releases()
    ]
  end

  defp releases do
    [
      smolquery: [
        include_executables_for: [:unix],
        applications: [runtime_tools: :permanent, aws_credentials: :load]
      ]
    ]
  end

  def cli do
    [preferred_envs: [precommit: :test, ci: :test]]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Smolquery.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  defp deps do
    [
      {:adbc, "~> 0.12"},
      {:explorer, "~> 0.12"},
      {:bandit, "~> 1.12"},
      {:plug, "~> 1.20"},
      {:req, "~> 0.7"},
      {:req_s3, "~> 0.2"},
      {:aws_credentials, "~> 1.1", runtime: false},
      {:gen_rpc, github: "emqx/gen_rpc", tag: "3.6.1", manager: :rebar3},
      {:libcluster_postgres, "~> 0.2"},
      {:postgrex, "~> 0.22"},
      {:phoenix, "~> 1.8"},
      {:phoenix_html, "~> 4.1"},
      {:phoenix_live_view, "~> 1.2"},
      {:phoenix_live_reload, "~> 1.2", only: :dev},
      {:lazy_html, ">= 0.1.0", only: :test},
      {:esbuild, "~> 0.10", runtime: Mix.env() == :dev},
      {:tailwind, "~> 0.3", runtime: Mix.env() == :dev},
      {:heroicons,
       github: "tailwindlabs/heroicons",
       tag: "v2.2.0",
       sparse: "optimized",
       app: false,
       compile: false,
       depth: 1},
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
      "assets.setup": ["tailwind.install --if-missing", "esbuild.install --if-missing"],
      "assets.build": ["compile", "tailwind smolquery", "esbuild smolquery"],
      "assets.deploy": [
        "tailwind smolquery --minify",
        "esbuild smolquery --minify",
        "phx.digest"
      ],
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
