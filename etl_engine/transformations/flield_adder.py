"""
Field adder transformation - adds new fields to records.
"""

from datetime import datetime
from typing import Any, Callable
import uuid

from etl_engine.transformations.base import BaseTransformation
from etl_engine.models import ProcessedRecord, TransformationParams
from etl_engine.exceptions import TransformationError


class FieldAdderTransformation(BaseTransformation):
    """
    Adds new fields to records based on functions.
    """
    
    # Field generation functions registry
    FIELD_FUNCTIONS: dict[str, Callable[[], Any]] = {
        'current_timestamp': lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'current_date': lambda: datetime.now().strftime("%Y-%m-%d"),
        'uuid': lambda: str(uuid.uuid4()),
        'unix_timestamp': lambda: int(datetime.now().timestamp()),
    }
    
    def __init__(self, name: str, params: TransformationParams):
        super().__init__(name, params)
        
        # Validate that all functions exist
        if params.addFields:
            for field_rule in params.addFields:
                if field_rule.function not in self.FIELD_FUNCTIONS:
                    raise TransformationError(
                        f"Field function '{field_rule.function}' not found. "
                        f"Available: {list(self.FIELD_FUNCTIONS.keys())}"
                    )
    
    def execute(self, records: list[ProcessedRecord]) -> dict[str, list[ProcessedRecord]]:
        """
        Add fields to all records.
        
        Args:
            records: Records to transform
            
        Returns:
            Dictionary with transformation name as key and transformed records
        """
        transformed_records = []
        
        for record in records:
            # Add fields
            self._add_fields(record)
            transformed_records.append(record)
        
        return {
            self.name: transformed_records
        }
    
    def _add_fields(self, record: ProcessedRecord) -> None:
        """
        Add fields to a record.
        Modifies record.data in place.
        
        Args:
            record: Record to modify
        """
        if not self.params.addFields:
            return
        
        for field_rule in self.params.addFields:
            field_name = field_rule.name
            function_name = field_rule.function
            
            # Get the function and call it
            func = self.FIELD_FUNCTIONS[function_name]
            field_value = func()
            
            # Add to record data
            record.data[field_name] = field_value
    
    @classmethod
    def add_field_function(cls, function_name: str, func: Callable[[], Any]) -> None:
        """
        Add a custom field generation function dynamically.
        
        Args:
            function_name: Name of the function
            func: Function that generates the field value
            
        Example:
            FieldAdderTransformation.add_field_function(
                'random_id',
                lambda: random.randint(1000, 9999)
            )
        """
        cls.FIELD_FUNCTIONS[function_name] = func