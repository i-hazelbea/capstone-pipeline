-- ==========================================
-- PIPELINE RUN LOG
-- ==========================================

CREATE TABLE IF NOT EXISTS metadata.pipeline_run_log (
    run_id SERIAL PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    run_status TEXT CHECK (run_status IN ('started', 'completed', 'failed')),
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP,
    duration_seconds AS (EXTRACT(EPOCH FROM (finished_at - started_at)))
);

-- ==========================================
-- INGESTION LOG TABLE
-- ==========================================

CREATE TABLE IF NOT EXISTS metadata.ingestion_log (
    ingestion_id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES metadata.pipeline_run_log(run_id),
    table_name TEXT NOT NULL,
    source_file TEXT,
    rows_inserted INTEGER DEFAULT 0,
    rows_failed INTEGER DEFAULT 0,
    status TEXT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP
);

-- ==========================================
-- INDEXES FOR MONITORING
-- ==========================================
CREATE INDEX idx_run_status ON metadata.pipeline_run_log(run_status, started_at DESC);
CREATE INDEX idx_ingestion_table ON metadata.ingestion_log(table_name, status);