"""Main ETL processor."""

import logging
from typing import Dict, List, Any

from .metadata_reader import MetadataReader
from .source_loader import SourceLoader
from .transformations import validate_records, add_fields
from .sink_writer import write_output
from .exceptions import ETLError


logger = logging.getLogger(__name__)


def run_etl(metadata_path: str) -> None:
    """Run ETL process from metadata config."""
    
    logger.info(f"Starting ETL with metadata: {metadata_path}")
    metadata = MetadataReader.read(metadata_path)
    
    for dataflow in metadata.dataflows:
        logger.info(f"Processing dataflow: {dataflow.name}")
        
        # Track intermediate datasets by name
        datasets = {}
        
        # Load sources
        for source in dataflow.sources:
            logger.info(f"Loading {source.name} from {source.path}")
            records = SourceLoader.load(source.path, source.format)
            datasets[source.name] = records
            logger.info(f"Loaded {len(records)} records from {source.name}")
        
        # Apply transformations
        for trans in dataflow.transformations:
            logger.info(f"Running transformation: {trans.name} ({trans.type})")
            
            if trans.type == "validate_fields":
                input_data = datasets[trans.params["input"]]
                
                valid, invalid = validate_records(
                    input_data,
                    trans.params["validations"]
                )
                
                datasets[f"{trans.name}_ok"] = valid
                datasets[f"{trans.name}_ko"] = invalid
                logger.info(f"Validation: {len(valid)} valid, {len(invalid)} invalid")
            
            elif trans.type == "add_fields":
                input_data = datasets[trans.params["input"]]
                
                result = add_fields(
                    input_data,
                    trans.params["addFields"]
                )
                
                datasets[trans.name] = result
                logger.info(f"Added fields to {len(result)} records")
            
            else:
                raise ETLError(f"Unknown transformation: {trans.type}")
        
        # Write outputs
        for sink in dataflow.sinks:
            if sink.input not in datasets:
                raise ETLError(f"Dataset not found: {sink.input}")
            
            output_data = datasets[sink.input]
            
            logger.info(f"Writing {len(output_data)} records to {sink.paths[0]} ({sink.saveMode})")
            write_output(
                records=output_data,
                output_path=sink.paths[0],
                format=sink.format,
                save_mode=sink.saveMode
            )
        
        logger.info(f"Dataflow {dataflow.name} completed!")