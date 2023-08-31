import pytest
import tempfile
import os

CACHE_DIR_ENV_VAR_NAME = "CACHE_DIR"

@pytest.fixture(scope="session")
def cache_dir() -> str:
    """Create a temporary cache directory with files and set the environmental variable."""
    # Create the cache directory and set the environmental variable
    cache = tempfile.mkdtemp()
    os.environ[CACHE_DIR_ENV_VAR_NAME] = cache

    yield cache

    # Remove the cache directory and its contents
    for root, dirs, files in os.walk(cache, topdown=False):
        for file in files:
            os.remove(os.path.join(root, file))
        for dir in dirs:
            os.rmdir(os.path.join(root, dir))
    os.rmdir(cache)
    
    del os.environ[CACHE_DIR_ENV_VAR_NAME]