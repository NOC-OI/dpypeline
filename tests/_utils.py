def create_dummy_files(create_temp_dir, n, suffix=".nc"):
    # Create dummy files

    files = [create_temp_dir / f"test_{i}.{suffix.rstrip('.', -1)}" for i in range(n)]
    for f in files:
        f.touch()
