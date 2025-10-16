-- Create observability schema
CREATE SCHEMA IF NOT EXISTS observability;

-- Set search path
SET search_path TO observability, public;

-- Executions table
CREATE TABLE IF NOT EXISTS observability.executions (
    execution_id UUID PRIMARY KEY,
    dataflow_name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    started_at TIMESTAMPTZ DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    duration_seconds NUMERIC(10, 3),
    records_processed INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Transformation metrics table
CREATE TABLE IF NOT EXISTS observability.transformation_metrics (
    metric_id SERIAL PRIMARY KEY,
    execution_id UUID NOT NULL,
    transformation_name VARCHAR(255) NOT NULL,
    transformation_type VARCHAR(100) NOT NULL,
    records_in INTEGER NOT NULL,
    records_out INTEGER NOT NULL,
    duration_ms NUMERIC(10, 3) NOT NULL,
    executed_at TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (execution_id) REFERENCES observability.executions(execution_id) ON DELETE CASCADE
);

-- Record lineage table
CREATE TABLE IF NOT EXISTS observability.record_lineage (
    lineage_id UUID PRIMARY KEY,
    execution_id UUID NOT NULL,
    record_id VARCHAR(255) NOT NULL,
    source_file VARCHAR(500) NOT NULL,
    transformation_path TEXT NOT NULL,
    output_path VARCHAR(500) NOT NULL,
    validation_passed BOOLEAN NOT NULL,
    processed_at TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (execution_id) REFERENCES observability.executions(execution_id) ON DELETE CASCADE
);

-- Validation errors table
CREATE TABLE IF NOT EXISTS observability.validation_errors (
    error_id SERIAL PRIMARY KEY,
    lineage_id UUID NOT NULL,
    field_name VARCHAR(255) NOT NULL,
    validation_rule VARCHAR(100) NOT NULL,
    error_details JSONB,
    detected_at TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (lineage_id) REFERENCES observability.record_lineage(lineage_id) ON DELETE CASCADE
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_executions_dataflow ON observability.executions(dataflow_name);
CREATE INDEX IF NOT EXISTS idx_executions_status ON observability.executions(status);
CREATE INDEX IF NOT EXISTS idx_executions_started ON observability.executions(started_at);
CREATE INDEX IF NOT EXISTS idx_lineage_execution ON observability.record_lineage(execution_id);
CREATE INDEX IF NOT EXISTS idx_lineage_record ON observability.record_lineage(record_id);
CREATE INDEX IF NOT EXISTS idx_lineage_validation ON observability.record_lineage(validation_passed);
CREATE INDEX IF NOT EXISTS idx_errors_lineage ON observability.validation_errors(lineage_id);