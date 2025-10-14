# Metadata-Driven ETL Engine

ETL framework driven by JSON metadata configuration. Built for SDG Group technical assessment.

## Project Structure
```
sdg-etl-project/
├── etl_engine/          # Core ETL modules
│   ├── exceptions.py    # Custom exceptions
│   ├── models.py        # Pydantic data models
│   ├── metadata_reader.py
│   ├── source_loader.py
│   ├── transformations.py
│   ├── sink_writer.py
│   └── processor.py     # Main orchestrator
├── config/
│   └── metadata.json    # ETL configuration
├── dags/
│   └── etl_dag.py       # Airflow DAG
├── data/
│   ├── input/           # Source data
│   └── output/          # Results (ok/ko)
├── docker-compose.yaml
├── requirements.txt
└── run_etl.py          # Local execution script
```

## Quick Start

### Local Execution
```bash
# Setup
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate   # Windows

pip install -r requirements.txt

# Create symlink for /data paths
sudo ln -s $(pwd)/data /data

# Run ETL
python run_etl.py
```

### Docker/Airflow
```bash
# Start Airflow
docker-compose up -d

# Access UI
http://localhost:8080

# Trigger DAG: person_etl
```

## How It Works

The ETL reads `config/metadata.json` and executes:

1. **Load sources** - Reads JSON files from specified paths
2. **Validate** - Checks fields (notNull, notEmpty)
3. **Transform** - Adds computed fields (timestamps, etc)
4. **Write outputs** - Separates valid/invalid records

### Metadata Structure
```json
{
  "dataflows": [{
    "sources": [{"name": "...", "path": "...", "format": "JSON"}],
    "transformations": [
      {"type": "validate_fields", "params": {...}},
      {"type": "add_fields", "params": {...}}
    ],
    "sinks": [{"input": "...", "paths": [...], "saveMode": "OVERWRITE|APPEND"}]
  }]
}
```

## Design Decisions

**Functional approach**: Transformations are stateless functions, not classes. Easier to test and compose.

**Dictionary-based rules**: Validation rules and field functions use dicts for extensibility. Adding new rules = adding dict entries.

**Explicit error tracking**: Invalid records include `arraycoderrorbyfield` with specific validation failures.

**Separate outputs**: Valid records (OVERWRITE) vs invalid records (APPEND) for different use cases.

## Extending the System

### Add new validation rule
```python
# etl_engine/transformations.py
VALIDATION_RULES = {
    "notNull": lambda value: value is not None,
    "notEmpty": lambda value: value not in [None, "", []],
    "isEmail": lambda value: "@" in str(value) if value else False,  # New rule
}
```

### Add new field function
```python
# etl_engine/transformations.py
FIELD_FUNCTIONS = {
    "current_timestamp": lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "uuid": lambda: str(uuid.uuid4()),  # New function
}
```

## Testing
```bash
# Run locally with test data
python run_etl.py

# Check outputs
cat /data/output/events/person/output.json
cat /data/output/discards/person/output.json
```

## Requirements

- Python 3.12+
- Pydantic 2.9+
- Docker (for Airflow)
