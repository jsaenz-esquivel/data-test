"""
TAB 2: Technical Monitoring
Embedded Grafana dashboards for comprehensive observability.
"""

import streamlit as st
import streamlit.components.v1 as components


# Grafana dashboard URLs
GRAFANA_DASHBOARDS = {
    "ETL Executions": {
        "url": "http://localhost:3000/d/b009acdd-de2e-4eb2-aaf6-9955b874d173/etl-executions-overview",
        "description": "Overview of ETL execution metrics, success rates, and performance trends"
    },
    "Validation Errors": {
        "url": "http://localhost:3000/d/e4845f91-5ca8-4199-917b-c9adf20f7fda/validation-errors-analysis",
        "description": "Analysis of validation errors by field, type, and frequency"
    },
    "Prometheus Metrics": {
        "url": "http://localhost:3000/d/b488ef31-3a88-454d-9be3-32fc3a20c6ba/etl-health-monitoring",
        "description": "Real-time metrics from Prometheus: execution duration, records processed, error rates"
    },
    "Container Metrics": {
        "url": "http://localhost:3000/d/pMEd7m0Mz/cadvisor-exporter",
        "description": "Container resource usage: CPU, memory, network, disk per container"
    },
    "Host Metrics": {
        "url": "http://localhost:3000/d/rYdddlPWk/node-exporter-full",
        "description": "Server-level metrics: CPU, memory, disk, network of the host machine"
    }
}


def render_technical_monitoring():
    """Render embedded Grafana dashboards with sub-tabs."""
    
    # Create sub-tabs for each dashboard
    tabs = st.tabs(["ETL Executions", "Validation Errors", "Prometheus", "Containers", "Host"])
    
    # ETL Executions Dashboard
    with tabs[0]:
        components.iframe(
            src=GRAFANA_DASHBOARDS["ETL Executions"]['url'] + "?kiosk=tv&theme=dark",
            height=750,
            scrolling=True,
            width=None
        )
    
    # Validation Errors Dashboard
    with tabs[1]:
        components.iframe(
            src=GRAFANA_DASHBOARDS["Validation Errors"]['url'] + "?kiosk=tv&theme=dark",
            height=750,
            scrolling=True,
            width=None
        )
    
    # Prometheus Metrics Dashboard
    with tabs[2]:
        components.iframe(
            src=GRAFANA_DASHBOARDS["Prometheus Metrics"]['url'] + "?kiosk=tv&theme=dark",
            height=750,
            scrolling=True,
            width=None
        )
    
    # Container Metrics (cAdvisor)
    with tabs[3]:
        components.iframe(
            src=GRAFANA_DASHBOARDS["Container Metrics"]['url'] + "?kiosk=tv&theme=dark",
            height=750,
            scrolling=True,
            width=None
        )
    
    # Host Metrics (Node Exporter)
    with tabs[4]:
        components.iframe(
            src=GRAFANA_DASHBOARDS["Host Metrics"]['url'] + "?kiosk=tv&theme=dark",
            height=750,
            scrolling=True,
            width=None
        )