"""Generate large dataset for ETL testing."""

import json
import random
from pathlib import Path
from faker import Faker

# Configuration
NUM_RECORDS = 1000
INVALID_PERCENTAGE = 0.25  # 25% invalid records
SEED = 42  # Reproducible results

# Spanish locale
fake = Faker('es_ES')
random.seed(SEED)
Faker.seed(SEED)

# Output directory
OUTPUT_DIR = Path("data/input/events/person")

# Spanish offices
OFFICES = ["MADRID", "BARCELONA", "VALENCIA", "SEVILLA", "BILBAO", "ZARAGOZA"]


def clean_generated_files():
    """Remove previously generated files, keep originals."""
    if not OUTPUT_DIR.exists():
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        return
    
    # Delete only person_NNNN.json files
    for file in OUTPUT_DIR.glob("person_*.json"):
        file.unlink()
    
    print(f"✓ Cleaned generated files, kept originals")


def generate_record(index: int, force_invalid: bool = False) -> dict:
    """Generate a single person record."""
    
    # Base valid record
    record = {
        "name": fake.name(),
        "age": random.randint(22, 65),
        "office": random.choice(OFFICES)
    }
    
    if force_invalid:
        # Choose error type
        error_type = random.choice(["missing_age", "empty_office", "both"])
        
        if error_type == "missing_age":
            del record["age"]
        elif error_type == "empty_office":
            record["office"] = ""
        else:  # both errors
            del record["age"]
            record["office"] = ""
    
    return record


def main():
    """Generate dataset."""
    print(f"Generating {NUM_RECORDS} records...")
    print(f"Invalid records: ~{int(NUM_RECORDS * INVALID_PERCENTAGE)}")
    
    # Clean previous generated files
    clean_generated_files()
    
    # Calculate how many invalid records
    num_invalid = int(NUM_RECORDS * INVALID_PERCENTAGE)
    invalid_indices = random.sample(range(NUM_RECORDS), num_invalid)
    
    # Generate records
    for i in range(NUM_RECORDS):
        force_invalid = i in invalid_indices
        record = generate_record(i, force_invalid)
        
        # Write to individual file
        filename = OUTPUT_DIR / f"person_{i+1:04d}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=4)
    
    print(f"✓ Generated {NUM_RECORDS} files in {OUTPUT_DIR}")
    print(f"  - Valid: ~{NUM_RECORDS - num_invalid}")
    print(f"  - Invalid: ~{num_invalid}")


if __name__ == "__main__":
    main()