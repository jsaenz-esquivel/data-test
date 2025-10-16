"""Data transformation functions."""

from typing import List, Dict, Any, Tuple
from datetime import datetime

from .exceptions import ValidationError


VALIDATION_RULES = {
    "notNull": lambda value: value is not None,
    "notEmpty": lambda value: value not in [None, "", []],
}


FIELD_FUNCTIONS = {
    "current_timestamp": lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
}


def validate_records(
    records: List[Dict[str, Any]],
    validations: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Validate records and split into OK and KO."""
    valid = []
    invalid = []
    
    for record in records:
        errors = {}
        
        for validation_rule in validations:
            field_name = validation_rule["field"]
            rules = validation_rule["validations"]
            
            field_value = record.get(field_name)
            field_errors = []
            
            for rule_name in rules:
                if rule_name not in VALIDATION_RULES:
                    raise ValidationError(f"Unknown validation rule: {rule_name}")
                
                validator = VALIDATION_RULES[rule_name]
                
                if not validator(field_value):
                    field_errors.append(rule_name)
            
            if field_errors:
                errors[field_name] = field_errors
        
        if errors:
            record_copy = record.copy()
            record_copy["arraycoderrorbyfield"] = errors
            invalid.append(record_copy)
        else:
            valid.append(record)
    
    return valid, invalid


def add_fields(
    records: List[Dict[str, Any]],
    fields_to_add: List[Dict[str, str]]
) -> List[Dict[str, Any]]:
    """Add new fields to records."""
    result = []
    
    for record in records:
        new_record = record.copy()
        
        for field_config in fields_to_add:
            field_name = field_config["name"]
            function_name = field_config["function"]
            
            if function_name not in FIELD_FUNCTIONS:
                raise ValidationError(f"Unknown field function: {function_name}")
            
            func = FIELD_FUNCTIONS[function_name]
            new_record[field_name] = func()
        
        result.append(new_record)
    
    return result