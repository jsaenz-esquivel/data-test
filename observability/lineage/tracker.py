"""Record lineage tracking utilities."""

import uuid
import logging

logger = logging.getLogger(__name__)


def generate_record_id(record: dict) -> str:
    """Generate readable ID from key business fields."""
    name = record.get('name', 'unknown')
    age = record.get('age', 'none')
    office = record.get('office', 'empty')
    
    age_str = str(age) if age is not None else 'none'
    office_str = office if office else 'empty'
    
    return f"{name}-{age_str}-{office_str}"


def generate_lineage_id() -> str:
    """Generate unique UUID for lineage table."""
    return str(uuid.uuid4())


def track_record(
    conn,
    execution_id: str,
    record: dict,
    source_file: str,
    transformation_path: str,
    output_path: str,
    validation_passed: bool
) -> str:
    """Track individual record lineage and errors if any."""
    from observability.db import queries
    
    record_id = generate_record_id(record)
    lineage_id = generate_lineage_id()
    
    queries.save_record_lineage(
        conn=conn,
        lineage_id=lineage_id,
        execution_id=execution_id,
        record_id=record_id,
        source_file=source_file,
        transformation_path=transformation_path,
        output_path=output_path,
        validation_passed=validation_passed
    )
    
    if not validation_passed and 'arraycoderrorbyfield' in record:
        errors = record['arraycoderrorbyfield']
        
        for field_name, validation_rules in errors.items():
            for rule in validation_rules:
                queries.save_validation_error(
                    conn=conn,
                    lineage_id=lineage_id,
                    field_name=field_name,
                    validation_rule=rule,
                    error_details={'actual_value': record.get(field_name)}
                )
    
    return lineage_id