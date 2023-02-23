"""Akita, the watchdog class."""

import logging
import time

from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver

from .file_handler import FileHandler
from .directory_state import DirectoryState
from .queue_events import EventsQueue


class Akita:
    """
    Akita, the watchdog.

    The Akita watchdog holds the queue, observer, and event handler instances.
    The in-memory Queue is created upon instantiation and should be a singleton to provide a global point of access.

    Attributes
    ----------
    _path
        Directory to watch.
    _queue
        Queue where events are placed by the FileHandler.
    _observer
        Observer thread that schedules watching directories and dispatches calls to event handlers.
    _event_handler
        Handler responsible for matching given patterns with file paths associated with occurring events.
    """

    def __init__(
        self,
        path: str,
        patterns: str | list[str],
        queue: EventsQueue = None,
        observer: Observer | PollingObserver = PollingObserver(),
        run_init: bool = False,
        create_event_handler_kwargs: dict = None,
        create_queue_kwargs: dict = None,
    ) -> None:
        """
        Initialize the Akita watchdog.

        Parameters
        ----------
        path
            Directory to watch.
        patterns
            Patterns for matching files.
        queue
            Queue where events are placed by the FileHandler.
        observer
            Observer thread that schedules watching directories and dispatches calls to event handlers.
        run_init
            If `True` runs the watchdog. `False` otherwise.
        create_event_handler_kwargs
            Kwargs to pass to the _create_event_handler function.
        create_queue_kwargs
            Kwargs to use when creating the queue.
        glob_kwargs
            Glob kwargs.
        """
        create_queue_kwargs = create_queue_kwargs if create_queue_kwargs is not None else {}
        create_event_handler_kwargs = create_event_handler_kwargs if create_event_handler_kwargs is not None else {}
        glob_kwargs = glob_kwargs if glob_kwargs is not None else {}

        self._path = path
        self._patterns = patterns if patterns else list(patterns)
        self._queue = (
            queue if queue is not None else EventsQueue(**create_queue_kwargs)
        )
        self._observer = observer
        self._event_handler = self._create_event_handler(self._patterns, **create_event_handler_kwargs)
        self._directory_state = self._create_directory_state(self._path, self._patterns)

        if run_init:
            self.run()

    @property
    def queue(self):
        """Return the queue."""
        return self._queue

    @queue.deleter
    def queue(self):
        """Delete the queue."""
        logging.info("Deleting in-memory queue.")
        del self._queue

    def _create_event_handler(
        self,
        patterns: str | list[str] = ["*.nc"],
        ignore_patterns: str | list[str] | None = None,
        ignore_directories: bool = True,
        case_sensitive: bool = True,
    ) -> FileHandler:
        """
        Create the handler responsible for matching given patterns with file paths associated with occurring events.

        Parameters
        ----------
        patterns
            Patterns to allow matching events.
        ignore_patterns
            Patterns to ignore matching event paths.
        ignore_directories
            If `True` directories are ignored; `False` otherwise.
        case_sensitive
            If `True` path names are matched sensitive to case; `False` otherwise.

        Returns
        -------
            Instance of :obj:`FileHandler`.
        """
        self._event_handler = FileHandler(
            queue=self._queue,
            patterns=patterns,
            ignore_patterns=ignore_patterns,
            ignore_directories=ignore_directories,
            case_sensitive=case_sensitive,
        )

        return self._event_handler
    
    def _create_directory_state(self, path: str, patterns: str | list[str] = ["*.nc"]) -> DirectoryState:
        """
        Create the state of the directory.
        """
        self._directory_state = DirectoryState(
            path=path, patterns=patterns
        )
        return self._directory_state

    def run(self) -> None:
        """Run the Akita watchdog."""
        self._observer.schedule(self._event_handler, self._path, recursive=True)
        self._observer.start()

        try:
            while True:
                time.sleep(1)
        finally:
            self._observer.stop()

        self._observer.join()
