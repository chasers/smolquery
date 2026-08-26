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
- **Model-check a distributed algorithm** — `./tla/run check` (or `mise run
  tla`) runs the TLA+ specs in `tla/` against `tla/expected.tsv`; CI runs the
  same gate on every PR. See `tla/README.md` for the config matrix and
  `tla/FINDINGS.md` for what each spec proved or found.

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

## ETS: keep every read bounded

`Smolquery.BufferService.HotManifest` is the node's hot-tier index and it sits on
the write path — the maintenance tick reads it on **every commit**. A read whose
cost grows with the table is therefore a read whose cost grows with the backlog,
and a deeper backlog is exactly when a buffer can least afford it. That bug has
been found twice (T-317, T-318). The rules below are what stops a third.

**Bound every read, and say what bounds it.** A read is acceptable if its cost is
constant, bounded by a configured valve, or proportional to what it returns.
"Proportional to the table" is only acceptable on a path that already costs that
much — serving the whole manifest over HTTP, or recovery.

| pattern | use |
|---|---|
| `:ets.lookup/2` on a full key | always preferred; O(1) in `set`, O(log N) in `ordered_set` |
| `:ets.select/2` with a match spec | when you must filter; the guard runs in C and non-matching rows are never copied out |
| `:ets.select/3` with a limit | when you only need the first N — pair it with a continuation loop, a chunk can return fewer than the limit |
| `:ets.next/2` walking a prefix | when the rows are contiguous in key order **because the key says so** — not because of an invariant held elsewhere; also slower per step under `write_concurrency` on an `ordered_set`, and two ops per row with no isolation between them |
| `:ets.match_object/2` | **avoid** — the Erlang docs say to prefer `ets:select/2` |
| `:ets.tab2list/1` | never on a table that grows with traffic |

**Never re-sort what ETS already ordered.** An `ordered_set` is traversed in term
order of the key, so `select`, `match_object` and `foldl` return rows already
sorted by key. If the key is `{table_ref, ulid}`, key order *is* age order.
`|> Enum.sort_by(& &1.id)` after a scan is pure waste and it hid an O(n log n)
term in `entries/2` until it was measured.

**Project in the match spec; do not copy rows you will discard.** ETS copies every
term it returns into the calling process. Returning whole rows to read three
fields off them is garbage the collector then has to chase. Measured on 1,024
32-column entries: 7,151,616 bytes returned whole against 98,304 projected, 73×.

**Bind as much of the key as you can.** A fully bound key turns a select into a
single lookup with no traversal. On an `ordered_set` a *partially* bound key
limits the traversal to a subset by term order — which is why every spec here
binds `table_ref` and leaves only the id open.

**Set `write_concurrency: true` on anything written on the request path.** It is
not a micro-optimisation. Measured on an `ordered_set` with 8 processes inserting
under distinct keys: 73,705 inserts/s without it, 909,556 with it — 12.3×. A
single-process microbenchmark shows nothing, so do not use one as evidence.

**Index on what you filter by.** If a read selects rows by a field, put that
field in the key of a second table rather than walking the primary one and
stopping at the first miss. A walk that relies on "the rows I want happen to be
contiguous" is relying on an invariant maintained somewhere else, and it fails
silently and permanently when that invariant does not hold — `retired_before/3`
stopped forever on any node that missed a best-effort replication, leaking
everything behind the hole.

**Cache a derivation rather than an incremental update.** When a read is hot and
the underlying state changes rarely, recompute the answer in full on mutation and
store it, instead of patching it. `live_claim/2` does this: it went from a scan on
every tick to an O(1) lookup, and because every mutation re-derives it there is no
incremental-update rule to get wrong. The test that keeps it honest asserts the
cached value equals a fresh derivation at every step of the lifecycle.

**Know how a table *shrinks*, not just how it grows.** Trace the delete path
before you trust a bound. The hot manifest index has exactly one steady-state
shrink path — a reaper that runs `retire_grace_ms` after a *successful* seal — so
it grows without limit whenever sealing stalls, and it holds a resident floor of
`flush_rate x retire_grace_ms / 1000` entries — the flush rate times the
  grace window in seconds even when sealing is healthy. Emit enough
counters to tell those two states apart
(`smolquery_hot_manifest_index_entries_total{change}`).

**Know how big the table can get, and write it down.** A row holding a per-column
stats map costs about 13.7 KB at 63 columns, so 16,384 entries is 214 MiB for one
table's index. `:compressed` cuts that to 77.8 MiB but costs ~1.8× on reads —
a lever, not a default. Anything unbounded in a table needs a documented ceiling
or a documented reaper.

**Measure at depth, not at zero.** Every one of these was invisible on an empty
table. `bench/hot_manifest.exs` and `bench/buffer.exs`'s `backlog_drag` section
exist to sweep depth; `[:smolquery, :hot_manifest, :read]` reports the same in
production, labelled by op.

References: the Erlang [Tables and Databases](https://www.erlang.org/doc/system/tablesdatabases.html)
efficiency guide (select/match "usually need to scan the complete table";
`ets:select/2` "to be preferred over" `ets:match_object/2`; bound-key traversal
limits) and [The New Scalable ETS ordered_set](https://www.erlang.org/blog/the-new-scalable-ets-ordered_set/).
[Cachex](https://hexdocs.pm/cachex) is the reference for ETS-backed caching in
Elixir — it creates its table with `read_concurrency: true, write_concurrency: true`,
and exposes `:compressed` and `:ordered` as explicit trade-offs rather than
defaults, which is the posture taken here. For the storage-engine thinking behind
the manifest log — an append-only log with a compaction step, and indexes that must
not turn a write into a scan — see Kleppmann, *Designing Data-Intensive
Applications*, ch. 3 ("Storage and Retrieval").

## Tests

```sh
mix test                            # fast suite (integration excluded)
mix test --include integration      # everything
mix test --only integration         # integration suite alone (what CI's Integration tests job runs)
```
