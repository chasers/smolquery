---
name: smolquery-pm
description: >-
  Track this repo's own work — projects, plans, and tasks — in a live smolsqls
  database (coordinating over the sibling product we dogfood). Use whenever you
  plan or manage smolquery work: creating/updating a project, writing or
  reading a plan (markdown design doc), adding/moving tasks, or answering
  "what's in progress / what's next / what's the plan for X". This is where
  plans live for the smolquery repo (superseding local .plans/ markdown).
  Triggers on: "add a task", "what's in progress", "track this", "store the
  plan", "project tracker", "what's next", "mark done".
---

# smolquery project tracker (in a smolsqls DB)

A simple Linear-style tracker for the smolquery repo, stored in a smolsqls
database on the alpha deployment. Queries go through the shared Elixir tool
`skills/query-db/smolsqls_query.exs` (see the `query-db` skill for the CLI
contract).

## Data model

- **project** — a named initiative/area that groups work over time
  (e.g. "BufferService"). Coarse, long-lived.
- **plan** — a markdown *design document* attached to a project (Problem /
  Design / Decisions — what a `.plans/*.md` file used to be). A project can
  have several over time. This is the *thinking*.
- **task** — a discrete, trackable action item with a status, in a project and
  optionally linked to the plan that spawned it. This is the *doing*. Three
  GitHub-username columns track who's touched it: `created_by` (filed it,
  set once at INSERT), `updated_by` (last person to touch it, set on every
  UPDATE), and `in_progress_by` (who's actively working it right now — set
  when status moves to `in_progress`, cleared when it moves away). All three
  are NULL until someone sets them.
- **changes** — an append-only activity log: one row per create/update across
  projects/plans/tasks (`entity_type`, `entity_key`, `action`, `actor`,
  `summary`, `created_at`). Populated automatically by triggers, not written
  to directly. This is how you notice parallel work — see "Coordinating with
  parallel work" below.

`project groups → plan describes → tasks execute`. Schema in
[`schema.sql`](./schema.sql).

| | statuses |
|---|---|
| project | `active` · `paused` · `done` · `archived` |
| plan | `draft` · `active` · `done` · `superseded` |
| task | `todo` · `in_progress` · `blocked` · `done` · `cancelled` (priority `low`/`med`/`high`/`urgent`) |

## Display keys — how to reference rows outside the tracker

Every row has a generated `key` column: tasks are `T-<id>`, projects `P-<id>`,
plans `PL-<id>`. **Always use the key — never a bare `#<id>` — when referencing
tracker items in GitHub PR descriptions, commit messages, or issues**: GitHub
autolinks `#55` to its own PR/issue 55. Keys work in queries too
(`WHERE key = 'T-137'`), and SQL can still use integer `id`s internally.

## Credentials — from the environment, never committed

The query tool reads a **dedicated** database's creds from the environment,
auto-loading the git-ignored file `.claude/smolquery-pm.env` (`.claude/` is
ignored) if present:

```
SMOLSQLS_PM_URL          (default https://alpha.smolsqls.com)
SMOLSQLS_PM_DB_ID
SMOLSQLS_PM_DB_TOKEN     (per-database auth_token, Bearer)
SMOLSQLS_PM_MGMT_TOKEN   (tenant api_key, Bearer — management calls only, e.g. PATCH /v1/databases/:id)
```

If the id/token are missing, stop — get the database credentials from the
project owner (or provision per Setup); never guess an id or token.

## The query tool

`skills/query-db/smolsqls_query.exs` — a self-contained Elixir CLI
(`Mix.install` pulls Req; no project compile needed). Run from the repo root:

```sh
# convenience alias for this shell (or expand it inline in each call)
pmq() { elixir skills/query-db/smolsqls_query.exs --db pm "$@"; }

pmq "SELECT ..."                      # prints an aligned table
pmq "INSERT ... RETURNING id"         # writes; prints rows / num_changes
pmq "SELECT ..." --args '["x", 1]'    # bind ? placeholders (positional)
pmq "INSERT ..." --args-file a.json   # args array from a file (big values)
pmq --file skills/smolquery-pm/schema.sql   # apply a .sql migration
pmq --json "SELECT ..."               # raw JSON instead of a table
```

Bind values with `?` placeholders + `--args` — never string-interpolate. One
statement per query (the endpoint runs a single statement and rejects
transaction control).

## Common operations

```sh
# What's on my plate — open tasks across active projects, highest priority first
pmq "SELECT t.key, p.slug, t.status, t.priority, t.title
       FROM tasks t JOIN projects p ON p.id = t.project_id
      WHERE t.status IN (?, ?) AND p.status = ?
      ORDER BY CASE t.priority WHEN ?4 THEN 0 WHEN ?5 THEN 1 WHEN ?6 THEN 2 ELSE 3 END,
               t.position" \
    --args '["in_progress","todo","active","urgent","high","med"]'

# List projects
pmq "SELECT slug, name, status FROM projects ORDER BY status, name"

# Create a project
pmq "INSERT INTO projects (slug, name, description) VALUES (?, ?, ?) RETURNING id, slug" \
    --args '["buffer-service","BufferService","Durable, queryable hot tier."]'

# Attach a plan. Small body: inline via --args. Large body: build the args array
# with the markdown read from a file, then --args-file.
jq -nc --arg p buffer-service --arg s 2026-07-31-hot-tier \
       --arg t "Hot tier design" --rawfile b /tmp/plan.md --arg st active \
       '[$p,$s,$t,$b,$st]' > /tmp/plan_args.json
pmq "INSERT INTO plans (project_id, slug, title, body_md, status)
     VALUES ((SELECT id FROM projects WHERE slug = ?), ?, ?, ?, ?)" --args-file /tmp/plan_args.json

# Add a task (optionally linked to a plan) — the returned key is what you cite in PRs
pmq "INSERT INTO tasks (project_id, plan_id, title, priority, created_by, updated_by)
     VALUES ((SELECT id FROM projects WHERE slug = ?),
             (SELECT id FROM plans   WHERE slug = ?), ?, ?, ?, ?) RETURNING id, key" \
    --args '["buffer-service","2026-07-31-hot-tier","Group-commit TableBuffer","high","chasegranberry","chasegranberry"]'

# Start working a task — claims it and stamps who touched it
pmq "UPDATE tasks
        SET status = 'in_progress', in_progress_by = ?, updated_by = ?,
            updated_at = datetime('now')
      WHERE key = ?" --args '["chasegranberry","chasegranberry","T-1"]'

# What <username> is actively working on right now
pmq "SELECT t.key, p.slug, t.title
       FROM tasks t JOIN projects p ON p.id = t.project_id
      WHERE t.in_progress_by = ?" --args '["chasegranberry"]'

# Move a task (by key or integer id); marking done stamps completed_at and
# clears in_progress_by (it's no longer "in progress" once it leaves that status)
pmq "UPDATE tasks
        SET status = ?,
            in_progress_by = CASE WHEN ? = 'in_progress' THEN in_progress_by ELSE NULL END,
            completed_at = CASE WHEN ? = 'done' THEN datetime('now') ELSE completed_at END,
            updated_by = ?,
            updated_at = datetime('now')
      WHERE key = ?" --args '["done","done","done","chasegranberry","T-1"]'

# Read a plan's markdown
pmq "SELECT body_md FROM plans WHERE slug = ?" --args '["2026-07-31-hot-tier"]'

# Project overview — task counts by status
pmq "SELECT t.status, COUNT(*) AS n FROM tasks t JOIN projects p ON p.id = t.project_id
      WHERE p.slug = ? GROUP BY t.status" --args '["buffer-service"]'
```

`RETURNING` rows come back in the output. Always bump `updated_at` on an UPDATE
— that column has no trigger; only `changes` logging is trigger-driven (see
below).

## Coordinating with parallel work

Other agents or humans may be working the tracker at the same time. `changes`
is populated automatically (triggers on `projects`/`plans`/`tasks`, both
INSERT and UPDATE) — check it before starting non-trivial work, and again
partway through anything long-running, to catch work that landed in parallel:

```sh
# Recent activity across the whole tracker
pmq "SELECT id, entity_type, entity_key, action, actor, summary, created_at
       FROM changes ORDER BY id DESC LIMIT 20"

# Poll loop: only what's new since the last change id you saw
pmq "SELECT id, entity_type, entity_key, action, actor, summary, created_at
       FROM changes WHERE id > ? ORDER BY id" --args '[123]'

# Activity on one task/plan/project specifically
pmq "SELECT action, actor, summary, created_at FROM changes
      WHERE entity_type = ? AND entity_key = ? ORDER BY id" --args '["task","T-9"]'
```

`changes` only gets a summary of *which field* changed first (status, then
priority, then a few others — see the trigger `CASE` in `schema.sql`), not a
full diff; for the actual current values, query the entity's own row.

## Setup / provisioning

The tracker database is provisioned once. Creating it is a **management** call
(tenant `api_key`); the database `auth_token` is returned only at create:

```sh
# 1. create the database (returns data.id and data.auth_token — capture both)
curl -sS -X POST "${SMOLSQLS_PM_URL:-https://alpha.smolsqls.com}/v1/databases" \
  -H "authorization: Bearer $SMOLSQLS_ALPHA_API_KEY" \
  -H 'content-type: application/json' \
  -d '{"name": "smolquery-pm"}'

# 2. store creds in the git-ignored env file (auto-loaded by the tool; never commit)
#    printf 'export SMOLSQLS_PM_DB_ID=%s\nexport SMOLSQLS_PM_DB_TOKEN=%s\n' "$ID" "$TOKEN" > .claude/smolquery-pm.env

# 3. apply the schema (splits on ';' and posts each statement — the loader is
#    BEGIN/CASE/END-aware, so the changes-log triggers apply as one statement each)
elixir skills/query-db/smolsqls_query.exs --db pm --file skills/smolquery-pm/schema.sql

# 4. verify
pmq "SELECT type, name FROM sqlite_master WHERE type IN ('table','trigger') ORDER BY type, name"
```

Re-applying is safe (`CREATE TABLE/INDEX/TRIGGER IF NOT EXISTS`).

## Notes

- **This supersedes `.plans/` for the smolquery repo.** When planning work
  here, create/attach a plan row and tasks instead of a local markdown file.
- Backups: this is real data on alpha, with litestream replication enabled
  (`litestream_enabled: true` on the database). For a point-in-time copy, use
  the management backups endpoint, not tenant SQL (`VACUUM` is denied for
  tenants).
