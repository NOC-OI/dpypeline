"""Directory state class."""
import glob
import logging
import os
import pickle
from typing import Protocol

class Queue(Procotol):
    """Queue class."""

    def enqueue(self, event) -> bool:
        ...

class DirectoryState:
    """DirectoryState class."""
    _state_file: str = "directory_state.pickle"

    def __init__(self, path: str, patterns: str | list[str]) -> None:
        """
        Initiate the DirectoryState class.
        
        Parameters
        ----------
        path
            Path to watch.
        patterns
            Patterns for matching files.
        """
        self._path = path
        self._patterns = patterns if patterns else list(patterns)
        self._current_state = None
        self._enqueued_state = None

        if os.path.isfile(os.getenv("CACHE_DIR") + self._state_file):
            logging.info(
                f"Found directory state file {os.getenv('CACHE_DIR') + self._state_file}."
            )
            self._load_state()
        else:
            logging.info(
                f"No queue state file {os.getenv('CACHE_DIR') + self._state_file} was found."
            )
            self._stored_state = []

    def _save_state(self) -> None:
        """Save the state of the directory by only saving files that were already enqueued."""
        logging.info("Saving state of the directory.")
        with open(os.getenv("CACHE_DIR") + self._state_file, "wb") as f:
            pickle.dump(self._enqueued_state, f)
        
        self._stored_state = self._enqueued_state

    def _load_state(self) -> None:
        """Load the stored state of the directory."""
        logging.info("Loading state of the directory.")
        with open(os.getenv("CACHE_DIR") + self._state_file, "rb") as f:
            self._stored_state = pickle.load(f)

    def _get_not_enqueued_files(self) -> list[str]:
        """Return a list of files that are not enqueued."""
        return list(set(self._current_state) - set(self._stored_state))

    def _get_current_state(self, glob_kwargs: dict = None) -> list[str]:
        """
        Get the current state of the directory. Also sets the current state.

        Parameters
        ----------
        glob_kwargs, optional
            Kwargs to pass to glob.

        Returns
        -------
            Curren state of the directory.
        """
        glob_kwargs = glob_kwargs if glob_kwargs is not None else {}
        
        self._current_state = []
        for pattern in self._patterns:
            found_files = glob.glob(self._path + pattern, **glob_kwargs)
            self._current_state.extend(found_files)

        return self._current_state

        # JM glob.glob("/gws/nopw/j04/nemo_vol1/ORCA0083-N006/means/**/ORCA*-N06_*m*T.nc", recursive=True)

    def _enqueue_file(self, queue: Queue, file_path: str) -> None:
        """Enqueue a file to the queue.
        
        Parameters
        ----------
        queue
            Queue to enqueue the file to.
        file_path
            Path of the file to enqueue.    
        """
        queue.enqueue(file_path)

    def enqueue_new_files(self, queue: Queue) -> None:
        """
        Enqueue files previously unqueued.

        Parameters
        ----------
        queue
            Queue to enqueue files to.
        """
        not_enqueued_state = self.get_not_enqueued_files()
        logging.info(f"Founded {len(not_enqueued_state)} files not enqueued.")
        self._enqueued_state = self._stored_state
        for f in not_enqueued_state:
            self._enqueue_file(queue, f)
            self._enqueued_state.append(f)
            self._save_state()

    def add_enqueued_file(self, file_path: str) -> None:
        """Add a file enqueued elsewhere to ensure correct state.
        
        Parameters
        ----------
        file
            File to add to the enqueued state and current state.
        """
        logging.info(f"Adding enqueued file {file_path} to directory state.")
        self._current_state.append(file_path)
        self._enqueued_state.append(file_path)
        self._save_state()
