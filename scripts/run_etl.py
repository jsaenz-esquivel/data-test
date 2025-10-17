"""Run ETL locally for testing."""

import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables from .env
load_dotenv(project_root / ".env")

from etl_engine.processor import run_etl


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


if __name__ == "__main__":
    metadata_path = project_root / "config" / "metadata.json"
    run_etl(str(metadata_path))