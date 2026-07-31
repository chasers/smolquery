-- smolquery-pm — project tracker for the smolquery repo, stored in a smolsqls DB.
--
-- Apply ONE statement at a time via POST /v1/databases/:id/query: the HTTP
-- query endpoint runs a single statement per call and rejects transaction
-- control. Every statement below is terminated by a single ';' and contains
-- no internal ';', so splitting the file on ';' is safe.
--
-- foreign_keys are ON per connection (server default). updated_at is a
-- convention: set `updated_at = datetime('now')` in UPDATEs (no triggers, to
-- keep statements single-';' for the one-statement-per-request model).

CREATE TABLE IF NOT EXISTS projects (
  id          INTEGER PRIMARY KEY,
  key         TEXT GENERATED ALWAYS AS ('P-' || id) VIRTUAL,
  slug        TEXT NOT NULL UNIQUE,
  name        TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  status      TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','paused','done','archived')),
  created_at  TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS plans (
  id          INTEGER PRIMARY KEY,
  key         TEXT GENERATED ALWAYS AS ('PL-' || id) VIRTUAL,
  project_id  INTEGER NOT NULL REFERENCES projects(id),
  slug        TEXT NOT NULL UNIQUE,
  title       TEXT NOT NULL,
  body_md     TEXT NOT NULL DEFAULT '',
  status      TEXT NOT NULL DEFAULT 'draft' CHECK (status IN ('draft','active','done','superseded')),
  created_at  TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS tasks (
  id             INTEGER PRIMARY KEY,
  key            TEXT GENERATED ALWAYS AS ('T-' || id) VIRTUAL,
  project_id     INTEGER NOT NULL REFERENCES projects(id),
  plan_id        INTEGER REFERENCES plans(id),
  title          TEXT NOT NULL,
  body           TEXT NOT NULL DEFAULT '',
  status         TEXT NOT NULL DEFAULT 'todo' CHECK (status IN ('todo','in_progress','blocked','done','cancelled')),
  priority       TEXT NOT NULL DEFAULT 'med' CHECK (priority IN ('low','med','high','urgent')),
  position       REAL NOT NULL DEFAULT 0,
  created_by     TEXT,
  updated_by     TEXT,
  in_progress_by TEXT,
  created_at     TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at     TEXT NOT NULL DEFAULT (datetime('now')),
  completed_at   TEXT
);

CREATE INDEX IF NOT EXISTS tasks_project_status ON tasks (project_id, status);

CREATE INDEX IF NOT EXISTS tasks_in_progress_by ON tasks (in_progress_by);

CREATE INDEX IF NOT EXISTS tasks_plan ON tasks (plan_id);

CREATE INDEX IF NOT EXISTS plans_project ON plans (project_id);
