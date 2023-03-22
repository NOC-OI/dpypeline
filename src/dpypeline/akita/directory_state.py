"""Directory state class."""
import glob
import logging
import os
import pickle


class DirectoryState:
    """DirectoryState class."""

    _state_file_suffix: str = "directory_state.pickle"

    def __init__(
        self, path: str, patterns: str | list[str], glob_kwargs: dict = None
    ) -> None:
        """
        Initiate the DirectoryState class.

        Parameters
        ----------
        path
            Path to watch.
        patterns
            Patterns for matching files.
        """
        self._current_state: list[str] = None
        self._stored_state: list[str] = None

        self._path = path
        self._patterns = [patterns] if isinstance(patterns, str) else patterns
        self._glob_kwargs = glob_kwargs if glob_kwargs is not None else {}

        assert (
            os.getenv("CACHE_DIR") is not None
        ), "CACHE_DIR environmental variable is not set."

        self._state_file = os.path.join(os.getenv("CACHE_DIR"), self._state_file_suffix)

    @property
    def stored_state(self) -> list[str]:
        """
        Return the stored state of the directory.

        Returns
        -------
            Store state of the directory.
        """
        self._load_state()
        return self._stored_state

    @property
    def current_state(self) -> list[str]:
        """
        Return the current state of the directory.

        Returns
        -------
            Current state of the directory.
        """
        self._get_current_directory_state()
        return self._current_state

    def _save_state(self) -> None:
        """
        Save the directory state.

        Notes
        -----
        Save the state of the directory by only saving
        files that were already enqueued.
        """
        logging.info("-" * 79)
        logging.info("Saving state of the directory.")

        with open(self._state_file, "wb") as f:
            pickle.dump(self._current_state, f)

    def _load_state(self) -> None:
        """Load the stored state of the directory."""
        logging.info("-" * 79)
        logging.info("Loading state of the directory.")

        if os.path.isfile(self._state_file):
            logging.info(f"Found stored directory state file {self._state_file}.")
            with open(self._state_file, "rb") as f:
                self._stored_state = pickle.load(f)
        else:
            logging.info(f"No directory state file {self._state_file} was found.")
            self._stored_state = []

    def _get_current_directory_state(self) -> list[str]:
        """
        Get and set the current state of the directory.

        Notes
        -----
        Everytime this method is called, the current state of the directory is saved.

        Returns
        -------
            Current state of the directory.
        """
        self._current_state = []
        for pattern in self._patterns:
            found_files = glob.glob(
                os.path.join(self._path, pattern), **self._glob_kwargs
            )
            self._current_state.extend(found_files)

        self._save_state()

        return self._current_state
