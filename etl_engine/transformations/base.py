"""
Base class for all transformations.
Uses Strategy Pattern - each transformation type is a separate class.
"""

from abc import ABC, abstractmethod
from typing import Any

from etl_engine.models import ProcessedRecord, TransformationParams


class BaseTransformation(ABC):
    """
    Abstract base for all transformations.
    Each transformation must implement execute() method.
    """
    
    def __init__(self, name: str, params: TransformationParams):
        """
        Initialize transformation.
        
        Args:
            name: Transformation name (for output naming)
            params: Transformation parameters from metadata
        """
        self.name = name
        self.params = params
    
    @abstractmethod
    def execute(self, records: list[ProcessedRecord]) -> dict[str, list[ProcessedRecord]]:
        """
        Execute transformation on records.
        
        Args:
            records: List of records to transform
            
        Returns:
            Dictionary with output names as keys and transformed records as values.
            Example: {
                "validation_ok": [record1, record2],
                "validation_ko": [record3]
            }
        """
        pass
    
    def _get_output_name(self, suffix: str = "") -> str:
        """
        Generate output name for this transformation.
        
        Args:
            suffix: Optional suffix (e.g., "_ok", "_ko")
            
        Returns:
            Output name like "validation_ok"
        """
        return f"{self.name}{suffix}"