"""
Database query functions for Streamlit observability dashboard.
Reuses existing connection pool from observability module.
"""

import logging
from typing import Dict, List, Any

import pandas as pd
import streamlit as st

from observability.db.connection import init_pool, get_connection

logger = logging.getLogger(__name__)


def execute_query(query: str, params: tuple = None) -> pd.DataFrame:
    """Execute SQL query and return DataFrame."""
    with get_connection() as conn:
        return pd.read_sql_query(query, conn, params=params)


# TAB 1: Executive Dashboard

@st.cache_data(ttl=60)
def get_kpi_metrics() -> Dict[str, Any]:
    """Get main KPI metrics for executive dashboard."""
    query = """
    SELECT 
        COUNT(DISTINCT execution_id) as total_executions,
        SUM(records_processed) as total_records,
        ROUND(AVG(duration_seconds), 2) as avg_execution_time,
        SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
    FROM observability.executions;
    """
    
    df = execute_query(query)
    
    if df.empty:
        return {
            "total_executions": 0,
            "total_records": 0,
            "success_rate": 0.0,
            "avg_execution_time": 0.0
        }
    
    row = df.iloc[0]
    total = int(row["total_executions"])
    completed = int(row["completed"])
    
    return {
        "total_executions": total,
        "total_records": int(row["total_records"] or 0),
        "success_rate": round((completed / total * 100) if total > 0 else 0, 2),
        "avg_execution_time": float(row["avg_execution_time"] or 0)
    }


@st.cache_data(ttl=60)
def get_executions_timeline() -> pd.DataFrame:
    """Get execution metrics over time for trend charts."""
    query = """
    SELECT 
        DATE_TRUNC('hour', started_at) as time_bucket,
        COUNT(*) as execution_count,
        SUM(records_processed) as records_processed,
        ROUND(AVG(duration_seconds), 2) as avg_duration
    FROM observability.executions
    WHERE started_at >= NOW() - INTERVAL '24 hours'
    GROUP BY time_bucket
    ORDER BY time_bucket;
    """
    
    return execute_query(query)


@st.cache_data(ttl=60)
def get_validation_stats() -> pd.DataFrame:
    """Get validation error statistics by field."""
    query = """
    SELECT 
        field_name,
        validation_rule,
        COUNT(*) as error_count
    FROM observability.validation_errors
    GROUP BY field_name, validation_rule
    ORDER BY error_count DESC
    LIMIT 10;
    """
    
    return execute_query(query)


# TAB 2: Data Lineage

@st.cache_data(ttl=300)
def search_record_lineage(search_term: str) -> pd.DataFrame:
    """Search records by ID."""
    query = """
    SELECT 
        rl.lineage_id,
        rl.record_id,
        rl.execution_id,
        rl.source_file,
        rl.output_path,
        rl.validation_passed,
        rl.processed_at,
        e.dataflow_name,
        e.status
    FROM observability.record_lineage rl
    JOIN observability.executions e ON rl.execution_id = e.execution_id
    WHERE rl.record_id ILIKE %s
    ORDER BY rl.processed_at DESC
    LIMIT 50;
    """
    
    search_pattern = f"%{search_term}%"
    return execute_query(query, params=(search_pattern,))


@st.cache_data(ttl=300)
def get_record_journey(record_id: str) -> Dict[str, Any]:
    """Get complete journey of a specific record."""
    query = """
    SELECT 
        rl.lineage_id,
        rl.record_id,
        rl.execution_id,
        rl.source_file,
        rl.transformation_path,
        rl.output_path,
        rl.validation_passed,
        rl.processed_at,
        e.dataflow_name,
        e.started_at,
        e.finished_at,
        e.status
    FROM observability.record_lineage rl
    JOIN observability.executions e ON rl.execution_id = e.execution_id
    WHERE rl.record_id = %s
    ORDER BY rl.processed_at DESC
    LIMIT 1;
    """
    
    df = execute_query(query, params=(record_id,))
    
    if df.empty:
        return {}
    
    row = df.iloc[0]
    return {
        "lineage_id": row["lineage_id"],
        "record_id": row["record_id"],
        "execution_id": row["execution_id"],
        "source_file": row["source_file"],
        "transformation_path": row["transformation_path"],
        "output_path": row["output_path"],
        "validation_passed": row["validation_passed"],
        "processed_at": row["processed_at"],
        "dataflow_name": row["dataflow_name"],
        "execution_status": row["status"]
    }


@st.cache_data(ttl=300)
def get_record_errors(lineage_id: str) -> pd.DataFrame:
    """Get validation errors for a specific record."""
    query = """
    SELECT 
        field_name,
        validation_rule,
        error_details,
        detected_at
    FROM observability.validation_errors
    WHERE lineage_id = %s
    ORDER BY detected_at;
    """
    
    return execute_query(query, params=(lineage_id,))


# TAB 3: Live Monitoring

@st.cache_data(ttl=30)
def get_recent_executions(limit: int = 20) -> pd.DataFrame:
    """Get most recent executions for live monitoring."""
    query = """
    SELECT 
        execution_id,
        dataflow_name,
        status,
        started_at,
        finished_at,
        records_processed,
        duration_seconds
    FROM observability.executions
    ORDER BY started_at DESC
    LIMIT %s;
    """
    
    return execute_query(query, params=(limit,))


@st.cache_data(ttl=30)
def get_error_alerts() -> Dict[str, Any]:
    """Check for error alerts in recent executions."""
    query = """
    SELECT 
        COUNT(*) as recent_executions,
        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_count
    FROM observability.executions
    WHERE started_at >= NOW() - INTERVAL '1 hour';
    """
    
    df = execute_query(query)
    
    if df.empty:
        return {"error_rate": 0.0, "alert": False}
    
    row = df.iloc[0]
    total = int(row["recent_executions"])
    failed = int(row["failed_count"])
    
    error_rate = (failed / total * 100) if total > 0 else 0
    
    return {
        "error_rate": round(error_rate, 2),
        "failed_count": failed,
        "total_executions": total,
        "alert": error_rate > 10.0
    }