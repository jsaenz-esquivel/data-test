"""Database query functions for observability."""

import json
import logging

logger = logging.getLogger(__name__)


def start_execution(conn, execution_id: str, dataflow_name: str) -> None:
    """Insert new execution record at ETL start."""
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO observability.executions 
                (execution_id, dataflow_name, status, started_at)
                VALUES (%s, %s, %s, NOW())
                """,
                (execution_id, dataflow_name, 'running')
            )
        conn.commit()
        logger.info(f"Execution {execution_id} started")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to start execution: {e}")
        raise


def finish_execution(conn, execution_id: str, status: str, records_processed: int) -> None:
    """Update execution record at ETL finish."""
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE observability.executions
                SET status = %s,
                    finished_at = NOW(),
                    records_processed = %s,
                    duration_seconds = EXTRACT(EPOCH FROM (NOW() - started_at))
                WHERE execution_id = %s
                """,
                (status, records_processed, execution_id)
            )
        conn.commit()
        logger.info(f"Execution {execution_id} finished with status: {status}")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to finish execution: {e}")
        raise


def save_transformation_metrics(
    conn,
    execution_id: str,
    transformation_name: str,
    transformation_type: str,
    records_in: int,
    records_out: int,
    duration_ms: float
) -> None:
    """Save metrics for a transformation step."""
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO observability.transformation_metrics
                (execution_id, transformation_name, transformation_type, 
                 records_in, records_out, duration_ms, executed_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                """,
                (execution_id, transformation_name, transformation_type, 
                 records_in, records_out, duration_ms)
            )
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to save transformation metrics: {e}")
        raise


def save_record_lineage(
    conn,
    lineage_id: str,
    execution_id: str,
    record_id: str,
    source_file: str,
    transformation_path: str,
    output_path: str,
    validation_passed: bool
) -> None:
    """Save lineage tracking for individual record."""
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO observability.record_lineage
                (lineage_id, execution_id, record_id, source_file, 
                 transformation_path, output_path, validation_passed, processed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                """,
                (lineage_id, execution_id, record_id, source_file,
                 transformation_path, output_path, validation_passed)
            )
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to save record lineage: {e}")
        raise


def save_validation_error(
    conn,
    lineage_id: str,
    field_name: str,
    validation_rule: str,
    error_details: dict = None
) -> None:
    """Save validation error for a specific field."""
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO observability.validation_errors
                (lineage_id, field_name, validation_rule, error_details, detected_at)
                VALUES (%s, %s, %s, %s, NOW())
                """,
                (lineage_id, field_name, validation_rule, 
                 json.dumps(error_details) if error_details else None)
            )
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to save validation error: {e}")
        raise