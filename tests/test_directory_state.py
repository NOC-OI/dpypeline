import os

import pytest

from dpypeline.akita.directory_state import DirectoryState


@pytest.fixture
def create_temp_dir(tmp_path_factory):
    path = tmp_path_factory.mktemp("temp_dir", numbered=True)
    os.environ["CACHE_DIR"] = str(path)
    return path


def test_directory_state(create_temp_dir):
    dir_state = DirectoryState(path=create_temp_dir, patterns=["*.nc", "*.txt"])

    assert dir_state.stored_state == []
    assert dir_state._get_current_directory_state() == []
    assert dir_state.current_state == []

    # Create dummy NetCDF files
    nc_files = [create_temp_dir / f"test_{i}.nc" for i in range(10)]
    for nc_file in nc_files:
        nc_file.touch()
    nc_files = [os.path.realpath(nc_file) for nc_file in nc_files]

    assert len(dir_state.current_state) == len(nc_files)
    assert len(dir_state.stored_state) == len(nc_files)

    assert set(nc_files) == set(dir_state.stored_state)
    # assert dir_state.stored_state == nc_files

    # Create dummy txt files
    txt_files = [create_temp_dir / f"test_{i}.txt" for i in range(5)]
    for txt_file in txt_files:
        txt_file.touch()
    txt_files = [os.path.realpath(txt_file) for txt_file in txt_files]

    assert len(dir_state.current_state) == len(txt_files + nc_files)
    assert len(dir_state.stored_state) == len(txt_files + nc_files)
    assert set(dir_state.current_state) == set(txt_files + nc_files)
    assert set(dir_state.stored_state) == set(txt_files + nc_files)
