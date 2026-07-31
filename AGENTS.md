smolquery is an open source BigQuery alternative on DuckDB + Elixir: four
services in one OTP app (`Smolquery.IngestService`, `Smolquery.BufferService`,
`Smolquery.StorageService`, `Smolquery.QueryService`) around immutable Parquet
segments and a DuckLake catalog. The architecture plan lives in the project
tracker (see below), not in local markdown.

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

- `credo --strict` runs the **ex_slop** plugin: 40 checks for LLM patterns
  (blanket rescues, narrator/obvious comments, anti-idiomatic `Enum`, N+1).
  Matches the "no explanatory comments" rule.
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
```
