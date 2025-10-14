"""Write output data to sinks."""

import json
from pathlib import Path
from typing import List, Dict, Any

from .exceptions import SinkWriteError


def write_output(
    records: List[Dict[str, Any]],
    output_path: str,
    format: str = "JSON",
    save_mode: str = "OVERWRITE"
) -> None:
    """Write records to output file."""
    
    if format.upper() != "JSON":
        raise SinkWriteError(f"Unsupported format: {format}")
    
    path = Path(output_path)
    
    # Create directory if it doesn't exist
    path.mkdir(parents=True, exist_ok=True)
    
    # Output file path
    output_file = path / "output.json"
    
    try:
        if save_mode.upper() == "OVERWRITE":
            # Overwrite: write mode
            mode = 'w'
        elif save_mode.upper() == "APPEND":
            # Append: append mode
            mode = 'a'
        else:
            raise SinkWriteError(f"Unknown save mode: {save_mode}")
        
        with open(output_file, mode, encoding='utf-8') as f:
            for record in records:
                json_line = json.dumps(record, ensure_ascii=False)
                f.write(json_line + '\n')
    
    except Exception as e:
        raise SinkWriteError(f"Error writing output: {e}")