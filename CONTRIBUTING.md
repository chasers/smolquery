# Contributing to smolquery

Thanks for your interest. This is an early, opinionated project; see
[`README.md`](README.md) for what smolquery is.

## Dev setup

Toolchain versions are pinned in [`.tool-versions`](.tool-versions) and match
[`.github/workflows/ci.yml`](.github/workflows/ci.yml) — **OTP 29.0.2 / Elixir
1.20.2**. With [mise](https://mise.jdx.dev) (or asdf) installed:

```sh
mise install    # installs the pinned Erlang and Elixir
mix deps.get
mix test        # fast suite; add --include integration for everything
```

Pin the toolchain rather than relying on a system Erlang: a Homebrew `erl` ahead
of mise's on `PATH` builds against a different OTP than CI, and the failures that
produces point somewhere else entirely (a rebar3 plugin failing to parse
Erlang's own `leexinc.hrl`, for one).

## Quality gate — run before opening a PR

CI runs these; they must be green:

```sh
mix ci                        # compile -Werror, format, credo --strict (ex_slop), audits, ex_dna, reach arch policy
mix dialyzer                  # type analysis (own CI job)
mix test --only integration   # slow/external suite (own CI job; the two-node tests need epmd running)
```

`mix precommit` is the mutating convenience variant (formats instead of
checking format) for local use before committing.

## Pull requests

1. Branch off `main` — it's protected, so you can't push to it directly.
2. Keep changes focused; update `README.md` (and other docs) in the same
   change when behavior/commands/structure move.
3. Get `mix ci` + `mix dialyzer` + tests green locally.
4. Open a PR against `main` with a clear what/why. Reference tracker items by
   their display key (`T-12`, `PL-1`) — never a bare `#<id>`, which GitHub
   autolinks to its own issues.

`Checks (mix ci)`, `Tests`, and `Dialyzer` are required and enforced on every
PR, with no bypass. No approving review is required, so you can merge your own
work once those pass. Branches must be up to date with `main` before merging.

For anything non-trivial, split the work into a **stack** of dependent PRs so
each layer is reviewable on its own — every layer must be independently green.
See [`AGENTS.md`](AGENTS.md) for the `gh stack` workflow and the constraints
that come with it.

Releases are automatic: a merged bump of `version:` in `mix.exs` creates the
matching `v<version>` GitHub release (see
[`.github/workflows/release.yml`](.github/workflows/release.yml)).

## Coordination — the project tracker

Plans and tasks for this repo live in a live smolsqls database (we coordinate
over the sibling product we dogfood), **not** in local markdown or GitHub
issues. The architecture plan, milestones, and per-task status are all rows in
that database.

This repo ships [Claude Code](https://code.claude.com) skills in
[`skills/`](skills/) for working with it — **`smolquery-pm`** (the tracker:
projects, plans, tasks) built on **`query-db`** (the SQL-over-HTTP CLI).
Enable them locally:

```sh
./skills/install.sh            # link into ./.claude/skills (this repo)
./skills/install.sh --global   # link into ~/.claude/skills (any directory)
```

The tracker data is real and single-instance, so there's nothing to
self-provision — **if you genuinely need to read or update the tracker, get
the database credentials from the project owner.** Store them in the
git-ignored `.claude/smolquery-pm.env`; never commit a token.

## Anti-slop measures

CI gates against low-quality (especially AI-generated) code; see
[`AGENTS.md`](AGENTS.md) for the full tooling guide. In short:

- `credo --strict` with the **ex_slop** plugin (LLM-pattern checks: blanket
  rescues, narrator comments, anti-idiomatic `Enum`, N+1)
- `ex_dna` duplication ratchet — new clones fail CI
- `reach.check --arch` — the service-boundary policy in `.reach.exs` (services
  communicate only through client modules); `--smells --strict` fails on new
  structural smells vs the committed baseline
- No explanatory comments — docs live in `@moduledoc`/`@doc`/`@spec`

Don't disable a gate to get green; fix the code.
