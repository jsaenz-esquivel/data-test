"""
Loads source data from files.
Supports JSON format with wildcard paths.
"""

import json
from pathlib import Path
from typing import Any

from etl_engine.models import Source, ProcessedRecord
from etl_engine.exceptions import SourceLoadError


class SourceLoader:
    """
    Loads data from source files into ProcessedRecord objects.
    """
    
    @staticmethod
    def load(source: Source) -> list[ProcessedRecord]:
        """
        Load data from source definition.
        
        Args:
            source: Source configuration from metadata
            
        Returns:
            List of ProcessedRecord objects
            
        Raises:
            SourceLoadError: If files cannot be read
        """
        # Get format handler
        if source.format.upper() == "JSON":
            return SourceLoader._load_json(source)
        else:
            raise SourceLoadError(
                f"Unsupported format: {source.format}. Only JSON is supported."
            )
    
    @staticmethod
    def _load_json(source: Source) -> list[ProcessedRecord]:
        """
        Load JSON files (supports wildcards and JSONL format).
        
        Args:
            source: Source configuration
            
        Returns:
            List of ProcessedRecord objects
        """
        path = Path(source.path)
        records = []
        
        # Handle wildcards (e.g., /data/input/events/person/*)
        if '*' in source.path:
            # Get parent directory and pattern
            parent = path.parent
            pattern = path.name
            
            if not parent.exists():
                raise SourceLoadError(
                    f"Source directory does not exist: {parent}"
                )
            
            # Find matching files
            files = list(parent.glob(pattern))
            
            if not files:
                raise SourceLoadError(
                    f"No files found matching pattern: {source.path}"
                )
            
            # Load each file
            for file_path in files:
                records.extend(SourceLoader._read_json_file(file_path))
        
        else:
            # Single file
            if not path.exists():
                raise SourceLoadError(
                    f"Source file does not exist: {path}"
                )
            
            records = SourceLoader._read_json_file(path)
        
        return records
    
    @staticmethod
    def _read_json_file(file_path: Path) -> list[ProcessedRecord]:
        """
        Read a single JSONL file (one JSON object per line).
        
        Format expected:
        {"name":"Xabier","age":39,"office":""}
        {"name":"Miguel","office":"RIO"}
        
        Args:
            file_path: Path to JSONL file
            
        Returns:
            List of ProcessedRecord objects
        """
        records = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    
                    # Skip empty lines
                    if not line:
                        continue
                    
                    try:
                        data = json.loads(line)
                        records.append(ProcessedRecord(data=data))
                    except json.JSONDecodeError as e:
                        raise SourceLoadError(
                            f"Invalid JSON on line {line_num} in {file_path}: {e}"
                        )
        
        except SourceLoadError:
            raise
        except Exception as e:
            raise SourceLoadError(
                f"Error reading file {file_path}: {e}"
            )
        
        return records