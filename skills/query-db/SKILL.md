---
name: query-db
description: >-
  The shared Elixir tool for querying ANY smolsqls database over the HTTP query
  API (POST /v1/databases/:id/query). This is the low-level primitive the
  smolquery-pm skill builds on. Use it directly to run SQL against an arbitrary
  smolsqls database given its URL / id / auth_token, or when you need the query
  CLI's mechanics (positional args, --args-file, applying a .sql file, JSON
  output). Triggers on: "query a smolsqls db", "run SQL against a smolsqls
  database", "use the query tool".
---

# query-db — the smolsqls query tool

`smolsqls_query.exs` is a self-contained Elixir CLI that POSTs to a smolsqls
database's HTTP query endpoint. It's an Elixir project, so we query it in
Elixir, not curl. `Mix.install` pulls Req on first run — no project compile,
works from the repo or a globally-symlinked skill. Needs `elixir` on `PATH`.

```sh
elixir skills/query-db/smolsqls_query.exs [opts] "SQL [...]"
elixir skills/query-db/smolsqls_query.exs [opts] --file path/to/schema.sql
```

## Options

| Option | Meaning |
|---|---|
| `--db NAME` | credential set (default `pm`); any name works |
| `--url URL` | base URL override (non-secret) |
| `--id ID` | database id override (non-secret) |
| `--env FILE` | dotenv file to load first (default: per `--db`) |
| `--args JSON` | positional args bound to `?`, e.g. `--args '["x",1]'` |
| `--args-file FILE` | read the JSON args array from a file (large values, e.g. a markdown body) |
| `--file FILE` | apply each statement in FILE (schema/migration), split on `;` but BEGIN/CASE/END-aware so a `CREATE TRIGGER ... BEGIN ... END;` body isn't split mid-way |
| `--json` | print the raw JSON response instead of an aligned table |

## Credentials — from the environment, never on argv

For `--db NAME`, the tool reads `SMOLSQLS_<NAME>_URL` / `_DB_ID` / `_DB_TOKEN`
(NAME upper-cased, non-alphanumerics → `_`) and auto-loads a git-ignored dotenv
file if present:

| `--db` | env prefix | default env file |
|---|---|---|
| `pm` | `SMOLSQLS_PM_*` | `.claude/smolquery-pm.env` |
| `alpha` | `SMOLSQLS_ALPHA_*` | `.claude/alpha-db.env` |
| `<x>` | `SMOLSQLS_<X>_*` | `.claude/<x>.env` |

`--url` and `--id` may be passed explicitly (they aren't secret). The
**`auth_token` is only ever read from the environment** — never accept it on
the command line. URL defaults to `https://alpha.smolsqls.com`.

## Output & errors

- Default: an aligned text table; writes print `ok (num_changes: N)`.
- `--json`: the raw success body `{"data": {"columns", "rows", "num_changes"}}`.
- On an API error the tool prints `error: <code>: <message>` to stderr and
  exits non-zero.

## Server-side rules (apply to every caller)

- **One statement per query.** The endpoint runs a single statement and rejects
  transaction control (`BEGIN`/`COMMIT`/`ROLLBACK`) → `transactions_not_supported`.
- **`ATTACH` / `DETACH` / `VACUUM`** are denied for tenant SQL → `authorization
  denied`.
- Bind values with `?` placeholders + `--args`/`--args-file` — never
  string-interpolate.

## Built on this

- **`smolquery-pm`** — this repo's project tracker (`--db pm`), with a schema
  and project/plan/task operations.
