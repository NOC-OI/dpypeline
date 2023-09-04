"""Test suite for the dpypeline.akita.directory_state module."""
import os
import tempfile
from typing import Generator, List

import pytest

from dpypeline.akita.directory_state import DirectoryState

FILENAMES: List[str] = ["file1.txt", "file2.txt", "file3.txt", "file4.txt"]


@pytest.fixture(scope="module")
def temp_directory_with_files() -> Generator[str, None, None]:
    """Create a temporary directory with files."""
    # Create a temporary directory
    temp_dir = tempfile.mkdtemp()

    # Create mock files inside the temporary directory
    for filename in FILENAMES:
        file_path = os.path.join(temp_dir, filename)
        with open(file_path, "w") as file:
            file.close()

    yield temp_dir

    # Remove the temporary directory and its contents
    for root, dirs, files in os.walk(temp_dir, topdown=False):
        for file in files:  # type: ignore
            os.remove(os.path.join(root, str(file)))
        for dir in dirs:
            os.rmdir(os.path.join(root, dir))

    os.rmdir(temp_dir)


def test_get_current_directory_state(
    temp_directory_with_files: str,
    cache_dir: str,
) -> None:
    """Test the _get_directory_state method and current_state property."""
    # The test can use the prepared temporary directory
    directory_state = DirectoryState(path=temp_directory_with_files, patterns=["*.txt"])

    assert isinstance(
        directory_state, DirectoryState
    ), f"{directory_state} is not an instance of DirectoryState"

    # Test the current and stored state of the directory
    assert (
        directory_state._current_state is None
    ), f"{directory_state.current_state} != None"
    assert (
        directory_state._stored_state is None
    ), f"{directory_state.stored_state} != None"
    assert directory_state.stored_state == [], f"{directory_state.stored_state} != []"
    assert directory_state._stored_state == [], f"{directory_state.stored_state} != []"

    # Get the current state of the directory
    dir_state = directory_state._get_current_directory_state()

    # Expected results
    expected_state = []
    for filename in FILENAMES:
        expected_state.append(os.path.join(temp_directory_with_files, filename))

    assert len(dir_state) == 4
    for file in dir_state:
        assert os.path.isfile(file), f"{file} not in {dir_state}"
        assert file in expected_state, f"{file} not in {expected_state}"

    # Test the current state of the directory
    assert (
        dir_state == directory_state._current_state
    ), f"{dir_state} != {directory_state.current_state}"
    assert (
        dir_state == directory_state.current_state
    ), f"{dir_state} != {directory_state.current_state}"
    assert (
        dir_state == directory_state._current_state
    ), f"{dir_state} != {directory_state.current_state}"
    assert directory_state._stored_state == [], f"{directory_state.stored_state} != []"


def test_save_load_state(
    temp_directory_with_files: str,
    cache_dir: str,
) -> None:
    """Test the _save_state and _load_state methods and the stored_state property."""
    # The test can use the prepared temporary directory
    directory_state = DirectoryState(path=temp_directory_with_files, patterns=["*.txt"])

    assert isinstance(
        directory_state, DirectoryState
    ), f"{directory_state} is not an instance of DirectoryState."

    # Get the current state of the directory
    dir_state = directory_state._get_current_directory_state()

    # Save the state of the directory
    directory_state._save_state()

    # Assert that the state file was saved
    assert os.path.isfile(
        os.path.join(cache_dir, directory_state._state_file)
    ), f"{os.path.join(cache_dir, directory_state._state_file)} not found"

    # Load the state of the directory
    directory_state._load_state()

    assert (
        dir_state == directory_state._stored_state
    ), "State after loading is not equal to the current state of the directory."
    assert (
        dir_state == directory_state.stored_state
    ), "State after loading is not equal to the current state of the directory."

    # Create a new file in the temporary directory
    new_file_path = os.path.join(temp_directory_with_files, "file5.txt")
    with open(new_file_path, "w") as new_file:
        new_file.close()

    assert (
        dir_state == directory_state._stored_state
    ), "State is not equal to the stored state of the directory."
    assert (
        dir_state == directory_state.stored_state
    ), "State is not equal to the current state of the directory."

    assert (
        len(directory_state.stored_state) == 4
    ), "Wrong number of files in the stored state of the directory."
    assert (
        len(directory_state._stored_state) == 4
    ), "Wrong number of files in the stored state of the directory."
    assert (
        len(directory_state._current_state) == 4
    ), "Wrong number of files in the current state of the directory."

    # Test the current state of the directory
    dir_state_2 = directory_state._get_current_directory_state()
    assert (
        dir_state_2 == directory_state._current_state
    ), f"{dir_state_2} != {directory_state._current_state}"
    assert (
        dir_state_2 == directory_state.current_state
    ), f"{dir_state_2} != {directory_state.current_state}"

    # Test the stored state of the directory
    assert (
        dir_state_2 != directory_state._stored_state
    ), f"{dir_state_2} != {directory_state._current_state}"
    assert (
        dir_state_2 == directory_state.stored_state
    ), f"{dir_state_2} == {directory_state.current_state}"
    assert (
        dir_state_2 == directory_state._stored_state
    ), f"{dir_state_2} != {directory_state._current_state}"

    # Test the number of files
    assert (
        len(dir_state_2) == 5
    ), "Wrong number of files in the current state of the directory."
    assert (
        len(directory_state.current_state) == 5
    ), "Wrong number of files in the current state of the directory."
    assert (
        len(directory_state._current_state) == 5
    ), "Wrong number of files in the current state of the directory."
    assert (
        len(directory_state.stored_state) == 5
    ), "Wrong number of files in the stored state of the directory."
    assert (
        len(directory_state._stored_state) == 5
    ), "Wrong number of files in the stored state of the directory."
