# Claude Code skills

Canonical home for this repo's [Claude Code skills](https://code.claude.com/docs/en/skills).
Each `<name>/SKILL.md` here is the single source of truth and is committed.

`.claude/` is **git-ignored** (a per-user directory), so the symlink that makes
a skill load is a one-time local setup step — the same way you'd enable any
skill for yourself:

```sh
./skills/install.sh            # link into ./.claude/skills  (loads in this repo)
./skills/install.sh --global   # link into ~/.claude/skills  (loads from any directory)
```

The script is re-runnable (correct links are left alone, stale ones replaced)
and adds every `skills/<name>/` it finds. Claude Code follows the symlinks and
de-duplicates if the same target is reachable from more than one location, so
running both is fine.

Prefer to do it by hand? It's just:

```sh
ln -s "$PWD/skills/smolquery-pm" .claude/skills/smolquery-pm      # in-repo
ln -s "$PWD/skills/smolquery-pm" ~/.claude/skills/smolquery-pm    # global
```

## Skills

- **`query-db`** — the shared Elixir query tool (`query-db/smolsqls_query.exs`)
  for running SQL against any smolsqls database over the HTTP API. The
  primitive `smolquery-pm` builds on.
- **`smolquery-pm`** — project tracker (projects · plans · tasks) for this
  repo, stored in a dedicated smolsqls DB on the alpha deployment. Where plans
  live now — coordination happens through this database, not local markdown.

`query-db` is an Elixir tool, so we query in Elixir. It's self-contained via
`Mix.install` (needs `elixir` on `PATH`; the first run fetches Req).
Credentials come from the environment (git-ignored `.claude/*.env`,
auto-loaded); nothing secret is committed.

## Adding a skill

1. Create `skills/<name>/SKILL.md` (frontmatter `name` + `description`; the
   **directory name** is what identifies the skill).
2. Commit the skill directory.
3. Run `./skills/install.sh` to link it in (don't commit anything under
   `.claude/` — it's ignored).
