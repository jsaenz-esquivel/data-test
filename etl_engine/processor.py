"""Main ETL processor with observability."""

import logging
import time
import uuid
from typing import Dict, List, Any

from .metadata_reader import MetadataReader
from .source_loader import SourceLoader
from .transformations import validate_records, add_fields
from .sink_writer import write_output
from .exceptions import ETLError

from observability.db.connection import init_pool, get_connection, close_pool
from observability.db import queries
from observability.lineage.tracker import track_record
from observability.metrics import collectors

logger = logging.getLogger(__name__)


def run_etl(metadata_path: str) -> None:
    """Run ETL process from metadata config with full observability."""
    
    execution_id = str(uuid.uuid4())
    logger.info(f"Starting ETL execution: {execution_id}")
    
    try:
        init_pool()
    except Exception as e:
        logger.error(f"Failed to initialize observability: {e}")
        raise
    
    total_records_processed = 0
    dataflow_name = None
    
    try:
        logger.info(f"Starting ETL with metadata: {metadata_path}")
        metadata = MetadataReader.read(metadata_path)
        
        for dataflow in metadata.dataflows:
            dataflow_name = dataflow.name
            logger.info(f"Processing dataflow: {dataflow_name}")
            
            with get_connection() as conn:
                queries.start_execution(conn, execution_id, dataflow_name)
            
            execution_start_time = time.time()
            collectors.execution_status.labels(dataflow=dataflow_name).set(1)
            
            datasets = {}
            
            # Load sources
            for source in dataflow.sources:
                logger.info(f"Loading {source.name} from {source.path}")
                records_with_source = SourceLoader.load(source.path, source.format)
                datasets[source.name] = records_with_source
                logger.info(f"Loaded {len(records_with_source)} records from {source.name}")
            
            # Apply transformations
            for trans in dataflow.transformations:
                logger.info(f"Running transformation: {trans.name} ({trans.type})")
                
                trans_start_time = time.time()
                
                if trans.type == "validate_fields":
                    input_data = datasets[trans.params["input"]]
                    records_only = [item['record'] for item in input_data]
                    
                    valid, invalid = validate_records(
                        records_only,
                        trans.params["validations"]
                    )
                    
                    record_to_source = {}
                    for item in input_data:
                        rec = item['record']
                        key = f"{rec.get('name')}-{rec.get('age')}-{rec.get('office')}"
                        record_to_source[key] = item['source_file']
                    
                    valid_with_source = []
                    for rec in valid:
                        key = f"{rec.get('name')}-{rec.get('age')}-{rec.get('office')}"
                        valid_with_source.append({
                            'record': rec,
                            'source_file': record_to_source.get(key, 'unknown'),
                            'transformation_path': f"{trans.params['input']}->validation->validation_ok"
                        })
                    
                    invalid_with_source = []
                    for rec in invalid:
                        key = f"{rec.get('name')}-{rec.get('age')}-{rec.get('office')}"
                        invalid_with_source.append({
                            'record': rec,
                            'source_file': record_to_source.get(key, 'unknown'),
                            'transformation_path': f"{trans.params['input']}->validation->validation_ko"
                        })
                    
                    datasets[f"{trans.name}_ok"] = valid_with_source
                    datasets[f"{trans.name}_ko"] = invalid_with_source
                    
                    logger.info(f"Validation: {len(valid_with_source)} valid, {len(invalid_with_source)} invalid")
                    
                    collectors.records_valid_total.labels(dataflow=dataflow_name).inc(len(valid_with_source))
                    collectors.records_invalid_total.labels(dataflow=dataflow_name).inc(len(invalid_with_source))
                    
                    trans_duration_ms = (time.time() - trans_start_time) * 1000
                    with get_connection() as conn:
                        queries.save_transformation_metrics(
                            conn=conn,
                            execution_id=execution_id,
                            transformation_name=trans.name,
                            transformation_type=trans.type,
                            records_in=len(input_data),
                            records_out=len(valid_with_source),
                            duration_ms=trans_duration_ms
                        )
                    
                    collectors.transformation_duration_seconds.labels(
                        dataflow=dataflow_name,
                        transformation_type=trans.type
                    ).observe(trans_duration_ms / 1000)
                
                elif trans.type == "add_fields":
                    input_data = datasets[trans.params["input"]]
                    records_only = [item['record'] for item in input_data]
                    
                    result_records = add_fields(
                        records_only,
                        trans.params["addFields"]
                    )
                    
                    result_with_source = []
                    for i, rec in enumerate(result_records):
                        result_with_source.append({
                            'record': rec,
                            'source_file': input_data[i]['source_file'],
                            'transformation_path': input_data[i]['transformation_path'] + f"->add_fields->{trans.name}"
                        })
                    
                    datasets[trans.name] = result_with_source
                    logger.info(f"Added fields to {len(result_records)} records")
                    
                    trans_duration_ms = (time.time() - trans_start_time) * 1000
                    with get_connection() as conn:
                        queries.save_transformation_metrics(
                            conn=conn,
                            execution_id=execution_id,
                            transformation_name=trans.name,
                            transformation_type=trans.type,
                            records_in=len(input_data),
                            records_out=len(result_records),
                            duration_ms=trans_duration_ms
                        )
                    
                    collectors.transformation_duration_seconds.labels(
                        dataflow=dataflow_name,
                        transformation_type=trans.type
                    ).observe(trans_duration_ms / 1000)
                
                else:
                    raise ETLError(f"Unknown transformation: {trans.type}")
            
            # Write outputs
            for sink in dataflow.sinks:
                if sink.input not in datasets:
                    raise ETLError(f"Dataset not found: {sink.input}")
                
                output_data = datasets[sink.input]
                
                logger.info(f"Writing {len(output_data)} records to {sink.paths[0]} ({sink.saveMode})")
                
                with get_connection() as conn:
                    for item in output_data:
                        record = item['record']
                        source_file = item['source_file']
                        transformation_path = item['transformation_path']
                        
                        validation_passed = 'arraycoderrorbyfield' not in record
                        
                        track_record(
                            conn=conn,
                            execution_id=execution_id,
                            record=record,
                            source_file=source_file,
                            transformation_path=transformation_path,
                            output_path=sink.paths[0],
                            validation_passed=validation_passed
                        )
                        
                        total_records_processed += 1
                        collectors.records_processed_total.labels(dataflow=dataflow_name).inc()
                
                records_only = [item['record'] for item in output_data]
                write_output(
                    records=records_only,
                    output_path=sink.paths[0],
                    format=sink.format,
                    save_mode=sink.saveMode
                )
            
            execution_duration = time.time() - execution_start_time
            
            with get_connection() as conn:
                queries.finish_execution(
                    conn=conn,
                    execution_id=execution_id,
                    status='success',
                    records_processed=total_records_processed
                )
            
            collectors.execution_status.labels(dataflow=dataflow_name).set(0)
            collectors.execution_duration_seconds.labels(dataflow=dataflow_name).observe(execution_duration)
            collectors.executions_total.labels(dataflow=dataflow_name, status='success').inc()
            
            logger.info(f"Dataflow {dataflow_name} completed!")
    
    except Exception as e:
        logger.error(f"ETL failed: {e}")
        if dataflow_name:
            try:
                with get_connection() as conn:
                    queries.finish_execution(
                        conn=conn,
                        execution_id=execution_id,
                        status='failed',
                        records_processed=total_records_processed
                    )
                collectors.executions_total.labels(dataflow=dataflow_name, status='failed').inc()
            except:
                pass
        raise
    
    finally:
        from observability.metrics import exporter
        exporter.push_metrics(job_name='etl-observability')
        close_pool()