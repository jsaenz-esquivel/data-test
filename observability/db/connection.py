"""Database connection pool management."""

import os
from contextlib import contextmanager
import psycopg2
from psycopg2 import pool
import logging

logger = logging.getLogger(__name__)

# DB config from environment
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "airflow")
DB_USER = os.getenv("DB_USER", "airflow")
DB_PASSWORD = os.getenv("DB_PASSWORD", "airflow")

_connection_pool = None


def init_pool():
    """Initialize connection pool (singleton)."""
    global _connection_pool
    
    if _connection_pool is not None:
        logger.info("Connection pool already initialized")
        return _connection_pool
    
    try:
        logger.info("Initializing database connection pool")
        
        _connection_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=2,
            maxconn=10,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        
        logger.info("Connection pool ready (min=2, max=10)")
        return _connection_pool
        
    except Exception as e:
        logger.error(f"Failed to initialize connection pool: {e}")
        raise


@contextmanager
def get_connection():
    """Get a connection from the pool."""
    if _connection_pool is None:
        raise RuntimeError("Connection pool not initialized. Call init_pool() first.")
    
    conn = _connection_pool.getconn()
    
    try:
        yield conn
    finally:
        _connection_pool.putconn(conn)


def close_pool():
    """Close all connections in the pool."""
    global _connection_pool
    
    if _connection_pool is not None:
        logger.info("Closing connection pool")
        _connection_pool.closeall()
        _connection_pool = None
        logger.info("Connection pool closed")