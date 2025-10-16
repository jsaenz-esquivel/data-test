"""Prometheus metrics collectors for ETL observability."""

from prometheus_client import Counter, Gauge, Histogram

records_processed_total = Counter(
    'etl_records_processed_total',
    'Total number of records processed',
    ['dataflow']
)

records_valid_total = Counter(
    'etl_records_valid_total',
    'Total number of valid records',
    ['dataflow']
)

records_invalid_total = Counter(
    'etl_records_invalid_total',
    'Total number of invalid records',
    ['dataflow']
)

executions_total = Counter(
    'etl_executions_total',
    'Total number of ETL executions',
    ['dataflow', 'status']
)

execution_status = Gauge(
    'etl_execution_status',
    'Current execution status (0=stopped, 1=running)',
    ['dataflow']
)

records_in_current_batch = Gauge(
    'etl_records_in_current_batch',
    'Number of records in current processing batch',
    ['dataflow']
)

transformation_duration_seconds = Histogram(
    'etl_transformation_duration_seconds',
    'Time spent in transformation steps',
    ['dataflow', 'transformation_type'],
    buckets=[0.001, 0.01, 0.1, 0.5, 1.0, 5.0, 10.0]
)

execution_duration_seconds = Histogram(
    'etl_execution_duration_seconds',
    'Total execution time',
    ['dataflow'],
    buckets=[0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0]
)