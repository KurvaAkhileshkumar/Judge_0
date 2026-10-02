-- Register "JavaScript (Node.js 20.17.0)" as a new Judge0 language.
-- ---------------------------------------------------------------------------
-- Adds a NEW language row (id 1001) that runs on the node20 binary baked into
-- Dockerfile.judge0-node20.  The stock Node 12 entry (id 63) is left untouched,
-- so nothing existing changes — this is purely additive.
--
-- id 1001: a high, fixed id well clear of Judge0 CE's built-in range (≤100) and
-- the "extra" range, so it won't collide with any official language.
--
-- run_cmd mirrors the stock JS entry's format exactly — Judge0 references the
-- source filename directly (not a placeholder):
--     id 63 : "/usr/local/node-12.14.0/bin/node script.js"
--     id 1001: "/usr/local/bin/node20 script.js"   <-- node20 from the image
--
-- Apply (from the EC2 host, against the running db container):
--   docker compose -f docker-compose.ec2.yml exec -T db \
--     psql -U judge0 -d judge0 < sql/register_node20.sql
--
-- Idempotent: safe to run more than once (ON CONFLICT upserts the row).

-- NOTE: the Judge0 languages table has no created_at/updated_at columns —
-- only (id, name, compile_cmd, run_cmd, source_file, is_archived).
INSERT INTO languages (id, name, compile_cmd, run_cmd, source_file, is_archived)
VALUES (
    1001,
    'JavaScript (Node.js 20.17.0)',
    NULL,                                  -- interpreted: no compile step
    '/usr/local/bin/node20 script.js',
    'script.js',
    false
)
ON CONFLICT (id) DO UPDATE SET
    name        = EXCLUDED.name,
    compile_cmd = EXCLUDED.compile_cmd,
    run_cmd     = EXCLUDED.run_cmd,
    source_file = EXCLUDED.source_file,
    is_archived = EXCLUDED.is_archived;

-- Verify
SELECT id, name, run_cmd, source_file, is_archived FROM languages WHERE id = 1001;
