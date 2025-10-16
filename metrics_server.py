"""Standalone Prometheus metrics server."""

import logging
import time
from observability.metrics import exporter

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


if __name__ == "__main__":
    logger.info("Starting Prometheus metrics exporter on port 8000")
    exporter.start_exporter(port=8000)
    
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Shutting down metrics server")