"""
TAB 1: Data Lineage Explorer
Interactive search and drill-down into record-level lineage.
"""

import streamlit as st
import pandas as pd
from datetime import datetime
from streamlit_db import (
    search_record_lineage, 
    get_record_journey, 
    get_record_errors
)


def render_lineage_explorer():
    """Render data lineage search and exploration interface."""
    
    # Search interface (no header, direct to search)
    col1, col2 = st.columns([3, 1])
    
    with col1:
        search_term = st.text_input(
            "Search by Record ID",
            placeholder="Enter record ID to track lineage...",
            label_visibility="collapsed"
        )
    
    with col2:
        search_button = st.button("Search", type="primary", use_container_width=True)
    
    if search_button and search_term:
        st.markdown("---")
        
        # Execute search
        with st.spinner("Searching records..."):
            results_df = search_record_lineage(search_term)
        
        if results_df.empty:
            st.warning(f"No records found matching: '{search_term}'")
            return
        
        # Display search results
        st.markdown(f"**Search Results** ({len(results_df)} found)")
        st.markdown("")
        
        # Format dataframe for display
        display_df = results_df.copy()
        display_df['processed_at'] = pd.to_datetime(display_df['processed_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Add status indicator
        display_df['status_icon'] = display_df['validation_passed'].apply(
            lambda x: "✅ Valid" if x else "❌ Invalid"
        )
        
        # Show results table
        st.dataframe(
            display_df[[
                'record_id', 
                'status_icon', 
                'execution_id', 
                'dataflow_name',
                'processed_at'
            ]].rename(columns={
                'record_id': 'Record ID',
                'status_icon': 'Status',
                'execution_id': 'Execution ID',
                'dataflow_name': 'Dataflow',
                'processed_at': 'Processed At'
            }),
            use_container_width=True,
            hide_index=True
        )
        
        st.markdown("---")
        
        # Record selection for detailed view
        st.markdown("**Detailed View**")
        st.markdown("")
        
        selected_lineage_id = st.selectbox(
            "Select a record to view its complete journey:",
            options=results_df['lineage_id'].tolist(),
            format_func=lambda x: results_df[results_df['lineage_id'] == x]['record_id'].iloc[0]
        )
        
        if selected_lineage_id:
            # Get selected record details
            selected_record = results_df[results_df['lineage_id'] == selected_lineage_id].iloc[0]
            record_id = selected_record['record_id']
            
            # Fetch complete journey
            with st.spinner("Loading record journey..."):
                journey = get_record_journey(record_id)
                errors_df = get_record_errors(selected_lineage_id)
            
            if journey:
                # Display journey info (all fields visible)
                st.markdown("**Record Details**")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.text(f"Record ID: {journey['record_id']}")
                    st.text(f"Execution: {journey['execution_id']}")
                
                with col2:
                    st.text(f"Dataflow: {journey['dataflow_name']}")
                    status = "✅ SUCCESS" if selected_record['validation_passed'] else "❌ FAILED"
                    st.text(f"Status: {status}")
                
                st.markdown("---")
                
                # Journey flow visualization
                st.markdown("**Record Journey**")
                st.markdown("")
                
                # Create visual flow
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.markdown("**Source**")
                    st.code(journey['source_file'], language=None)
                
                with col2:
                    st.markdown("**Transformation**")
                    st.code(journey['transformation_path'], language=None)
                
                with col3:
                    st.markdown("**Output**")
                    output = journey['output_path'] if selected_record['validation_passed'] else "discarded"
                    st.code(output, language=None)
                
                # Transformation details
                if journey.get('transformations'):
                    st.markdown("---")
                    st.markdown("**Transformations Applied**")
                    
                    trans_df = pd.DataFrame(journey['transformations'])
                    if not trans_df.empty:
                        st.dataframe(
                            trans_df[[
                                'transformation_type',
                                'transformed_field',
                                'original_value',
                                'transformed_value'
                            ]].rename(columns={
                                'transformation_type': 'Type',
                                'transformed_field': 'Field',
                                'original_value': 'Original',
                                'transformed_value': 'Transformed'
                            }),
                            use_container_width=True,
                            hide_index=True
                        )
                
                # Validation errors
                if not errors_df.empty:
                    st.markdown("---")
                    st.markdown("**Validation Errors**")
                    
                    for _, error in errors_df.iterrows():
                        with st.expander(f"⚠️ {error['field_name']} - {error['validation_rule']}", expanded=True):
                            st.error(error['error_message'])
                            st.caption(f"Detected at: {error['detected_at']}")
                else:
                    if not selected_record['validation_passed']:
                        st.info("No detailed error information available")
    
    else:
        # Show instructions when no search
        st.info("Enter a record ID above to start exploring lineage")
        
        st.markdown("#### Capabilities:")
        st.markdown("""
        - Search for any record by its unique identifier
        - Track the complete journey from source to output
        - View all transformations applied to the record
        - Inspect validation errors if the record was rejected
        - Debug data quality issues at record level
        """)