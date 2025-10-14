"""Run ETL locally for testing."""

import logging
from pathlib import Path
from etl_engine.processor import run_etl


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


if __name__ == "__main__":
    metadata_path = Path("config/metadata.json")
    run_etl(str(metadata_path))