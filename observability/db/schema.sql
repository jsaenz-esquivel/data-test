-- Create observability schema
CREATE SCHEMA IF NOT EXISTS observability;

-- Main execution tracking
CREATE TABLE IF NOT EXISTS observability.executions (
    execution_id UUID PRIMARY KEY,
    dataflow_name VARCHAR(100) NOT NULL,
    start_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    end_time TIMESTAMPTZ,
    status VARCHAR(20) NOT NULL DEFAULT 'running',
    total_records INTEGER DEFAULT 0,
    valid_records INTEGER DEFAULT 0,
    invalid_records INTEGER DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_executions_start_time ON observability.executions(start_time DESC);
CREATE INDEX IF NOT EXISTS idx_executions_dataflow ON observability.executions(dataflow_name);
CREATE INDEX IF NOT EXISTS idx_executions_status ON observability.executions(status);

-- Transformation metrics
CREATE TABLE IF NOT EXISTS observability.transformation_metrics (
    id SERIAL PRIMARY KEY,
    execution_id UUID NOT NULL REFERENCES observability.executions(execution_id) ON DELETE CASCADE,
    transformation_name VARCHAR(100) NOT NULL,
    transformation_type VARCHAR(50) NOT NULL,
    duration_seconds NUMERIC(10, 3),
    input_records INTEGER NOT NULL DEFAULT 0,
    output_ok INTEGER DEFAULT 0,
    output_ko INTEGER DEFAULT 0,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_trans_execution ON observability.transformation_metrics(execution_id);
CREATE INDEX IF NOT EXISTS idx_trans_name ON observability.transformation_metrics(transformation_name);

-- Record lineage
CREATE TABLE IF NOT EXISTS observability.record_lineage (
    id SERIAL PRIMARY KEY,
    execution_id UUID NOT NULL REFERENCES observability.executions(execution_id) ON DELETE CASCADE,
    record_id UUID NOT NULL,
    stage VARCHAR(50) NOT NULL,
    transformation_name VARCHAR(100),
    status VARCHAR(10) NOT NULL,
    error_details JSONB,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_lineage_execution ON observability.record_lineage(execution_id);
CREATE INDEX IF NOT EXISTS idx_lineage_record ON observability.record_lineage(record_id);
CREATE INDEX IF NOT EXISTS idx_lineage_status ON observability.record_lineage(status);
CREATE INDEX IF NOT EXISTS idx_lineage_timestamp ON observability.record_lineage(timestamp DESC);

-- Validation errors
CREATE TABLE IF NOT EXISTS observability.validation_errors (
    id SERIAL PRIMARY KEY,
    execution_id UUID NOT NULL REFERENCES observability.executions(execution_id) ON DELETE CASCADE,
    field_name VARCHAR(100) NOT NULL,
    validation_rule VARCHAR(50) NOT NULL,
    error_count INTEGER NOT NULL DEFAULT 1,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_errors_execution ON observability.validation_errors(execution_id);
CREATE INDEX IF NOT EXISTS idx_errors_field ON observability.validation_errors(field_name);
CREATE INDEX IF NOT EXISTS idx_errors_rule ON observability.validation_errors(validation_rule);

-- Grant permissions to airflow user
GRANT USAGE ON SCHEMA observability TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA observability TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA observability TO airflow;