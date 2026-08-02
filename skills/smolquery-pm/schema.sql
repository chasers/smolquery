-- smolquery-pm — project tracker for the smolquery repo, stored in a smolsqls DB.
--
-- Apply ONE statement at a time via POST /v1/databases/:id/query: the HTTP
-- query endpoint runs a single statement per call and rejects transaction
-- control. The --file loader splits this file into statements on ';', but is
-- BEGIN/CASE/END-aware, so a CREATE TRIGGER body's internal ';'s (including
-- inside a CASE expression) don't split early.
--
-- foreign_keys are ON per connection (server default). updated_at is a
-- convention: set `updated_at = datetime('now')` in UPDATEs — the triggers
-- below only maintain the `changes` log, not updated_at.

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

-- Activity log: one row per create/update across projects/plans/tasks, kept
-- so other agents/humans working the tracker can notice parallel work
-- without diffing every row of every table. Populated by triggers below.
CREATE TABLE IF NOT EXISTS changes (
  id          INTEGER PRIMARY KEY,
  entity_type TEXT NOT NULL CHECK (entity_type IN ('project','plan','task')),
  entity_id   INTEGER NOT NULL,
  entity_key  TEXT NOT NULL,
  action      TEXT NOT NULL CHECK (action IN ('created','updated')),
  actor       TEXT,
  summary     TEXT NOT NULL DEFAULT '',
  created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS changes_entity ON changes (entity_type, entity_id);

CREATE INDEX IF NOT EXISTS changes_created_at ON changes (created_at);

CREATE TRIGGER IF NOT EXISTS projects_change_insert AFTER INSERT ON projects
BEGIN
  INSERT INTO changes (entity_type, entity_id, entity_key, action, summary)
  VALUES ('project', NEW.id, NEW.key, 'created', 'created: ' || NEW.name);
END;

CREATE TRIGGER IF NOT EXISTS projects_change_update AFTER UPDATE ON projects
BEGIN
  INSERT INTO changes (entity_type, entity_id, entity_key, action, summary)
  VALUES (
    'project', NEW.id, NEW.key, 'updated',
    CASE
      WHEN OLD.status <> NEW.status THEN 'status: ' || OLD.status || ' -> ' || NEW.status
      WHEN OLD.name <> NEW.name THEN 'name changed'
      WHEN OLD.description <> NEW.description THEN 'description changed'
      ELSE 'updated'
    END
  );
END;

CREATE TRIGGER IF NOT EXISTS plans_change_insert AFTER INSERT ON plans
BEGIN
  INSERT INTO changes (entity_type, entity_id, entity_key, action, summary)
  VALUES ('plan', NEW.id, NEW.key, 'created', 'created: ' || NEW.title);
END;

CREATE TRIGGER IF NOT EXISTS plans_change_update AFTER UPDATE ON plans
BEGIN
  INSERT INTO changes (entity_type, entity_id, entity_key, action, summary)
  VALUES (
    'plan', NEW.id, NEW.key, 'updated',
    CASE
      WHEN OLD.status <> NEW.status THEN 'status: ' || OLD.status || ' -> ' || NEW.status
      WHEN OLD.title <> NEW.title THEN 'title changed'
      WHEN OLD.body_md <> NEW.body_md THEN 'body changed'
      ELSE 'updated'
    END
  );
END;

CREATE TRIGGER IF NOT EXISTS tasks_change_insert AFTER INSERT ON tasks
BEGIN
  INSERT INTO changes (entity_type, entity_id, entity_key, action, actor, summary)
  VALUES ('task', NEW.id, NEW.key, 'created', NEW.created_by, 'created: ' || NEW.title);
END;

CREATE TRIGGER IF NOT EXISTS tasks_change_update AFTER UPDATE ON tasks
BEGIN
  INSERT INTO changes (entity_type, entity_id, entity_key, action, actor, summary)
  VALUES (
    'task', NEW.id, NEW.key, 'updated', NEW.updated_by,
    CASE
      WHEN OLD.status <> NEW.status THEN 'status: ' || OLD.status || ' -> ' || NEW.status
      WHEN OLD.priority <> NEW.priority THEN 'priority: ' || OLD.priority || ' -> ' || NEW.priority
      WHEN OLD.in_progress_by IS NOT NEW.in_progress_by
        THEN 'in_progress_by: ' || coalesce(OLD.in_progress_by, '-') || ' -> ' || coalesce(NEW.in_progress_by, '-')
      WHEN OLD.title <> NEW.title THEN 'title changed'
      WHEN OLD.body <> NEW.body THEN 'body changed'
      ELSE 'updated'
    END
  );
END;
