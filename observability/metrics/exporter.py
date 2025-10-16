"""Prometheus metrics exporter using Pushgateway."""

import logging
from prometheus_client import push_to_gateway, REGISTRY

logger = logging.getLogger(__name__)

PUSHGATEWAY_URL = 'pushgateway:9091'


def push_metrics(job_name: str = 'etl-job'):
    """Push current metrics to Pushgateway."""
    try:
        push_to_gateway(PUSHGATEWAY_URL, job=job_name, registry=REGISTRY)
        logger.info(f"Pushed metrics to Pushgateway at {PUSHGATEWAY_URL}")
        
    except Exception as e:
        logger.warning(f"Failed to push metrics to Pushgateway: {e}")