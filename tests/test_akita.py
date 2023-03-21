import os

import pytest

from dpypeline.akita.core import Akita
from dpypeline.akita.factory import AkitaFactory

from ._utils import create_dummy_files


@pytest.fixture
def create_temp_dir(tmp_path_factory):
    path = tmp_path_factory.mktemp("temp_dir", numbered=True)
    os.environ["CACHE_DIR"] = str(path)
    return path


def test_akita(create_temp_dir):
    akita_dependencies = AkitaFactory(
        path=create_temp_dir,
        patterns=["*.nc", "*.txt"],
        ignore_patterns=None,
        ignore_directories=True,
        case_sensitive=True,
        glob_kwargs=None,
    )

    path, queue, event_handler, directory_state, polling_observer = akita_dependencies

    # Test cases
    # 1. Akita monitor a director with no files, and add files on the fly and check the behaviou
    # 2. Akita monitor a directory with files that have not been enqueued before
    # 2. Akita monitoring a directory where some files were already enqeueued,
    # but new ones were added after the previous session terminated

    akita = Akita(
        path=path,
        queue=queue,
        event_handler=event_handler,
        directory_state=directory_state,
        polling_observer=polling_observer,
    )
    akita.run()

    create_dummy_files(create_temp_dir, 10, suffix=".nc")
