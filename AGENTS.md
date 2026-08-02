smolquery is an open source BigQuery alternative on DuckDB + Elixir: four
services in one OTP app (`Smolquery.IngestService`, `Smolquery.BufferService`,
`Smolquery.StorageService`, `Smolquery.QueryService`) around immutable Parquet
segments and a DuckLake catalog. [`docs/architecture.md`](docs/architecture.md)
is the walk-through of how it works; the architecture *plan* lives in the
project tracker (see below), not in local markdown.

## Project guidelines

- Use `mix precommit` when you are done with a change and fix any pending
  issues (compiles with warnings-as-errors, formats, runs `credo --strict`
  with ex_slop, checks duplication with `ex_dna`, and tests). `mix ci` is the
  non-mutating superset CI runs (adds format/unused-deps checks, `deps.audit`,
  `hex.audit`, and structural checks via `reach.check --arch --smells`); it
  must pass before a PR is green. `mix dialyzer` runs as its own CI job.
- Plans and tasks live in the **smolquery-pm tracker** (a smolsqls database) —
  use the `smolquery-pm` skill. Don't create `.plans/*.md` files here.
- Services talk only through client modules; never reach into another
  service's GenServers, Registry, or ETS. `.reach.exs` encodes the forbidden
  cross-service dependencies — a violation fails `mix ci`.
- Use `:req` (`Req`) for HTTP requests; avoid `:httpoison`, `:tesla`, `:httpc`.

## Branches, stacked PRs, and merging

`main` is protected by the **"protect main"** ruleset, so you cannot push to it
— every change lands through a PR:

- Required checks: `Checks (mix ci)`, `Tests`, `Dialyzer`. **Strict** is on, so
  once `main` moves an open branch must be brought up to date before it can
  merge.
- A PR is required, with **0 approving reviews** — a solo maintainer can merge
  their own work. Force-pushes and branch deletion are blocked.
- There are **no bypass actors**. Nothing overrides the checks, so if a job is
  renamed in `ci.yml`, update the ruleset's context names to match or every PR
  blocks.

Split anything non-trivial into a **stack** of dependent PRs rather than one
large branch, so each layer is reviewable on its own. This repo uses GitHub's
native stacked PRs via the `gh stack` extension (needs `gh` ≥ 2.90, Git ≥ 2.20):

```sh
gh extension install github/gh-stack

# build each layer as its own branch + commit, verifying each is green, then:
gh stack init layer-1 layer-2 layer-3   # adopts existing branches, bottom-up
gh stack view                           # branches, PR links, statuses
gh stack submit --open                  # push all, create linked PRs
gh stack rebase                         # after main moves (strict checks)
gh stack merge --yes --rebase           # atomic: all merge or none do
```

Rules that make a stack work:

- **Every layer must pass `mix ci`, `mix test`, and `mix dialyzer` on its own.**
  CI runs against each PR independently, and a layer that only compiles once a
  later layer lands will fail. In particular, never reference a module a higher
  layer introduces — `compile --warnings-as-errors` fails on the undefined
  module.
- **A config or tooling change belongs in the layer that forces it.** Pinning
  `reach` to `roots: ["lib"]` and regenerating `.reach.baseline.json` had to
  ride with the layer that put `test/support` on `elixirc_paths`, because that
  is what changed reach's derived scope.
- **Write real commit messages.** `gh stack submit` is interactive in a TTY; in
  a non-interactive shell it skips the editor and derives each PR's title from
  the commit subject and its body from the commit body. Good commits produce
  good PRs for free. Note `--auto` creates *drafts* unless you pass `--open`.
- **Stacks cannot bypass merge requirements.** Don't add protection rules that
  a stack can't satisfy (e.g. required approvals with no second reviewer).
- Reference tracker items by display key (`T-12`, `PL-1`) in commits and PR
  bodies — never a bare `#<id>`, which GitHub autolinks to its own issues.

Gotcha when moving between layers locally: `config :adbc, :drivers` is a
compile-time application env key, so checking out a layer that adds or drops it
needs `mix deps.clean adbc --build`.

## Understanding the codebase & anti-slop tooling

Before reading files one by one, use these static-analysis tools — they answer
structural questions faster and more accurately than grepping, and they catch
AI-generated slop the compiler won't. All support `--format json`.

- **Orient in an unfamiliar area** — `mix reach.map`: modules, coupling,
  `--hotspots` (highest-risk functions by branches × callers), `--boundaries`
  (layer violations), `--effects`, `--depth`.
- **Before editing a function/module** — `mix reach.inspect
  <Mod.fun/arity | file:line> --impact --deps --why <target>` shows the blast
  radius (callers, dependents, slices).
- **Trace data flow / taint** — `mix reach.trace --from params --to write!`
  (or `--backward file:line` / `--forward`).
- **OTP topology** — `mix reach.otp` (add `--concurrency`): GenServer state
  machines, missing message handlers, supervision trees.
- **Find dead code** — `mix reach.check --dead-code` (advisory; not a CI gate).
- **Find duplication before extracting a helper** — `mix ex_dna` lists clones;
  `mix ex_dna.explain <n>` shows the anti-unification and suggested extraction.

Slop gates enforced by CI (don't disable them to get green — fix the code):

- `credo --strict` runs the **ex_slop** plugin: 31 of its 41 checks for LLM
  patterns (blanket rescues, narrator/obvious comments, anti-idiomatic `Enum`,
  N+1). Matches the "no explanatory comments" rule. `.credo.exs` appends
  `ExSlop.recommended_checks()` to `checks.enabled` — that append is load-
  bearing. Credo treats an explicit `enabled` list as authoritative and
  discards a plugin's default `checks.extra`, so dropping it silently takes
  the run from 100 checks back to 69 with zero ex_slop coverage.
- `ex_dna` fails CI on new duplication. When you legitimately remove clones,
  keep the config honest; never loosen it to paper over a paste.
- `reach.check --arch` enforces the service-boundary policy in `.reach.exs`;
  `--smells --strict --baseline .reach.baseline.json` fails on *new*
  structural smells only. If you intentionally accept a new smell, regenerate
  the baseline with `mix reach.check --arch --smells --write-baseline
  .reach.baseline.json`, pretty-print it (`jq . .reach.baseline.json > tmp &&
  mv tmp .reach.baseline.json`), and commit it.

## Elixir guidelines

- No inline/explanatory comments; documentation lives in `@moduledoc`/`@doc`/
  `@spec`. Code explains itself through naming and structure.
- Never `String.to_atom/1` on unbounded input (atom-table exhaustion);
  `.reach.exs` forbids it project-wide. Keep dynamic identifiers as strings;
  if an atom is genuinely required use `String.to_existing_atom/1`.
- Tagged tuples for returns (`{:ok, _}` / `{:error, _}`); `with`/`case`
  threading.
- Every new module gets a mirrored test file (`lib/foo/bar.ex` →
  `test/foo/bar_test.exs`); every public function gets at least a happy-path
  test. Slow/external tests are `@moduletag :integration` and excluded by
  default in `test/test_helper.exs`.
- Migrations (when they exist) are immutable once applied; new migration, not
  edits.

## Tests

```sh
mix test                            # fast suite (integration excluded)
mix test --include integration      # everything
mix test --only integration         # integration suite alone (what CI's Integration tests job runs)
```
