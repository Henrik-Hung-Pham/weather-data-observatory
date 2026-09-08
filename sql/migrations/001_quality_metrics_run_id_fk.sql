-- ============================================================================
-- 001: Make data_quality_metrics.run_id joinable to pipeline_runs.run_id
-- ============================================================================
-- sql/schema.sql is CREATE TABLE IF NOT EXISTS, so it will not retrofit the
-- foreign key onto a database that already exists. Run this once against any
-- deployment created before the fix.
--
--   psql -h <host> -U observatory -d observatory -f sql/migrations/001_quality_metrics_run_id_fk.sql
--
-- Before the fix each QualityGate minted its own uuid4(), so every existing
-- data_quality_metrics row carries a random id matching no pipeline run.
-- Those rows cannot be reattributed -- the association was never recorded --
-- so they are copied aside rather than dropped, then removed so the FK can be
-- added.

BEGIN;

-- 1. Preserve the unjoinable rows instead of destroying them.
CREATE TABLE IF NOT EXISTS data_quality_metrics_orphaned_backup AS
SELECT *
FROM data_quality_metrics dqm
WHERE NOT EXISTS (
    SELECT 1 FROM pipeline_runs pr WHERE pr.run_id = dqm.run_id
);

-- 2. Remove them from the live table.
DELETE FROM data_quality_metrics dqm
WHERE NOT EXISTS (
    SELECT 1 FROM pipeline_runs pr WHERE pr.run_id = dqm.run_id
);

-- 3. Enforce the relationship from here on.
ALTER TABLE data_quality_metrics
    DROP CONSTRAINT IF EXISTS data_quality_metrics_run_id_fkey;

ALTER TABLE data_quality_metrics
    ADD CONSTRAINT data_quality_metrics_run_id_fkey
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id) ON DELETE CASCADE;

COMMIT;
