"""
Reads and validates metadata configuration from JSON files.
"""

import json
from pathlib import Path
from typing import Union

from pydantic import ValidationError

from etl_engine.models import MetadataConfig
from etl_engine.exceptions import MetadataValidationError


class MetadataReader:
    """
    Loads metadata from JSON and validates structure.
    Uses Pydantic for automatic validation.
    """
    
    @staticmethod
    def load(metadata_path: Union[str, Path]) -> MetadataConfig:
        """
        Load and validate metadata from JSON file.
        
        Args:
            metadata_path: Path to metadata JSON file
            
        Returns:
            MetadataConfig: Validated metadata configuration
            
        Raises:
            MetadataValidationError: If file doesn't exist or JSON is invalid
        """
        path = Path(metadata_path)
        
        # Check file exists
        if not path.exists():
            raise MetadataValidationError(
                f"Metadata file not found: {path}"
            )
        
        # Read JSON
        try:
            with open(path, 'r', encoding='utf-8') as f:
                metadata_dict = json.load(f)
        except json.JSONDecodeError as e:
            raise MetadataValidationError(
                f"Invalid JSON in metadata file: {e}"
            )
        except Exception as e:
            raise MetadataValidationError(
                f"Error reading metadata file: {e}"
            )
        
        # Validate with Pydantic
        try:
            config = MetadataConfig(**metadata_dict)
        except ValidationError as e:
            # Pydantic gives detailed error messages
            raise MetadataValidationError(
                f"Metadata validation failed:\n{e}"
            )
        
        return config

        """
        Load metadata from JSON string (useful for testing).
        
        Args:
            json_string: JSON string containing metadata
            
        Returns:
            MetadataConfig: Validated metadata configuration
            
        Raises:
            MetadataValidationError: If JSON is invalid
        """
        try:
            metadata_dict = json.loads(json_string)
        except json.JSONDecodeError as e:
            raise MetadataValidationError(
                f"Invalid JSON string: {e}"
            )
        
        try:
            config = MetadataConfig(**metadata_dict)
        except ValidationError as e:
            raise MetadataValidationError(
                f"Metadata validation failed:\n{e}"
            )
        
        return config