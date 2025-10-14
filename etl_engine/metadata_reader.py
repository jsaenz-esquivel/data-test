"""Load and validate metadata configuration."""

import json
from pathlib import Path
from typing import Dict, Any

from pydantic import ValidationError

from .models import MetadataConfig
from .exceptions import MetadataError


class MetadataReader:
    """Loads metadata from JSON files."""
    
    @staticmethod
    def read(metadata_path: str | Path) -> MetadataConfig:
        path = Path(metadata_path)
        
        if not path.exists():
            raise MetadataError(f"Metadata file not found: {path}")
        
        try:
            with open(path, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
            
            # Let Pydantic validate the structure
            config = MetadataConfig(**raw_data)
            return config
            
        except json.JSONDecodeError as e:
            raise MetadataError(f"Invalid JSON in metadata file: {e}")
        
        except ValidationError as e:
            raise MetadataError(f"Metadata validation failed: {e}")
        
        except Exception as e:
            raise MetadataError(f"Error reading metadata: {e}")