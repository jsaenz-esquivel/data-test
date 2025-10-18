def get_next_file_number():
    """Get the next available file number."""
    if not OUTPUT_DIR.exists():
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        print(f"Created directory: {OUTPUT_DIR}")
        return 1
    
    existing = list(OUTPUT_DIR.glob("person_*.json"))
    print(f"Found {len(existing)} existing person_*.json files")  # ← DEBUG
    
    if not existing:
        print("No existing files found, starting at 1")
        return 1
    
    # Extract numbers from filenames
    numbers = []
    for f in existing:
        try:
            num = int(f.stem.split('_')[1])
            numbers.append(num)
        except (IndexError, ValueError) as e:
            print(f"Could not parse number from {f.name}: {e}")
            continue
    
    max_num = max(numbers) if numbers else 0
    next_num = max_num + 1
    print(f"Max existing number: {max_num}, next will be: {next_num}")  # ← DEBUG
    return next_num