"""Load source data from JSON files."""

import json
from pathlib import Path
from typing import List, Dict, Any

from .exceptions import SourceLoadError


class SourceLoader:
    """Loads data from source files."""
    
    @staticmethod
    def load(source_path: str, format: str = "JSON") -> List[Dict[str, Any]]:
        """Load data from JSON files matching the path pattern."""
        
        if format.upper() != "JSON":
            raise SourceLoadError(f"Unsupported format: {format}")
        
        path = Path(source_path)
        parent_dir = path.parent
        pattern = path.name
        
        if not parent_dir.exists():
            raise SourceLoadError(f"Directory not found: {parent_dir}")
        
        json_files = list(parent_dir.glob(pattern))
        
        if not json_files:
            raise SourceLoadError(f"No files found matching: {source_path}")
        
        records = []
        
        for file_path in json_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if isinstance(data, dict):
                    records.append({
                        'record': data,
                        'source_file': file_path.name
                    })
                else:
                    raise SourceLoadError(
                        f"Invalid JSON in {file_path.name}: expected object, "
                        f"got {type(data).__name__}"
                    )
                    
            except json.JSONDecodeError as e:
                raise SourceLoadError(f"Invalid JSON in {file_path.name}: {e}")
            
            except Exception as e:
                raise SourceLoadError(f"Error reading {file_path.name}: {e}")
        
        return records