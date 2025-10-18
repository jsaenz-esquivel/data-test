"""
Streamlit Observability Dashboard
Interactive platform for ETL monitoring and data lineage exploration.
"""

import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

import streamlit as st

# Load environment variables (prefer .env.local for development)
load_dotenv(".env.local")  # Streamlit local config
load_dotenv()  # Fallback to .env

# Add project root to path
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))

# Import observability and tab modules
from observability.db.connection import init_pool
from streamlit_tabs.tab1_lineage import render_lineage_explorer
from streamlit_tabs.tab2_monitoring import render_technical_monitoring

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize connection pool GLOBALLY (before any Streamlit code)
try:
    init_pool()
    logger.info("Database connection pool initialized at startup")
except Exception as e:
    logger.error(f"Failed to initialize connection pool: {e}")


# Page configuration
st.set_page_config(
    page_title="ETL Observability Platform",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)


def main():
    """Main application entry point."""
    
    # Remove top padding and maximize width
    st.markdown("""
        <style>
            .block-container {
                padding-top: 1rem;
                padding-bottom: 0rem;
                padding-left: 2rem;
                padding-right: 2rem;
                max-width: 100%;
            }
        </style>
    """, unsafe_allow_html=True)
    
    # Compact header
    st.markdown("## ETL Observability Platform")
    
    # Main navigation tabs (no space between)
    tab1, tab2 = st.tabs([
        "🔎 Data Lineage",
        "📊 Monitoring"
    ])
    
    with tab1:
        render_lineage_explorer()
    
    with tab2:
        render_technical_monitoring()
    
    # Footer
    st.markdown("---")
    st.caption("Apache Airflow · PostgreSQL · Prometheus · Grafana")


if __name__ == "__main__":
    main()