-- ==========================================
-- METADATA SCHEMA OBJECTS
-- ==========================================

-- PIPELINE RUN LOG (ONE ROW PER ORCHESTRATED RUN)
CREATE TABLE IF NOT EXISTS metadata.pipeline_run_log (
    run_id BIGSERIAL PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    orchestrator_run_id TEXT,
    trigger_type TEXT DEFAULT 'scheduled',
    run_status TEXT CHECK (run_status IN ('started', 'completed', 'failed', 'partial')),
    error_message TEXT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP,
    duration_seconds INTEGER
);


-- PIPELINE TASK LOG (ONE ROW PER TASK/STAGE)
CREATE TABLE IF NOT EXISTS metadata.pipeline_task_log (
    task_id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES metadata.pipeline_run_log(run_id) ON DELETE CASCADE,
    stage_name TEXT NOT NULL CHECK (stage_name IN ('ingestion', 'staging', 'marts', 'analytics')),
    task_name TEXT NOT NULL,
    source_file TEXT,
    source_relation TEXT,
    target_relation TEXT,
    status TEXT NOT NULL CHECK (status IN ('started', 'completed', 'failed', 'skipped')),
    rows_in BIGINT,
    rows_out BIGINT,
    rows_rejected BIGINT DEFAULT 0,
    retry_count INTEGER DEFAULT 0,
    error_message TEXT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP,
    duration_seconds INTEGER
);


-- INDEXES FOR MONITORING/ALERTS
CREATE INDEX IF NOT EXISTS idx_pipeline_run_status
    ON metadata.pipeline_run_log(run_status, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_task_status
    ON metadata.pipeline_task_log(stage_name, status, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_task_run
    ON metadata.pipeline_task_log(run_id, task_name);
