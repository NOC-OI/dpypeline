import time
from queue import Queue

from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver
from .file_handler import FileHandler


class Akita:
    """
    The Akita watchdog is a composite object that contains the queue, observer, and event handler instances.

    Attributes
    ----------
    _path
        Directory to watch.
    _observer
        Observer thread that schedules watching directories and dispatches calls to event handlers.
    _queue
        Queue where events are placed.
    _event_handler
        Handler responsible for matching given patterns with file paths associated with occurring events.
    """
    def __init__(self,
                 path: str,
                 observer: Observer | PollingObserver = PollingObserver(),
                 queue: Queue = Queue(),
                 run_init: bool = False,
                 create_event_handler_kwargs: dict = {}) -> None:
        """
        Parameters
        ----------
        path
            Directory to watch.
        observer
            Observer thread that schedules watching directories and dispatches calls to event handlers.
        queue
            Queue where events are placed.
        run_init
            If `True` runs the watchdog; `False` otherwise.
        create_event_handler_kwargs
            Kwargs to pass to the _create_event_handler function.
        """

        self._path = path
        self._observer = observer
        self._queue = queue
        self._event_handler = self._create_event_handler(**create_event_handler_kwargs)

        if run_init:
            self.run()

    def _create_event_handler(self, patterns: str | list[str] = ["*.nc"],
                              ignore_patterns: str | list[str] | None = None,
                              ignore_directories: bool = True, case_sensitive: bool = True) -> FileHandler:
        """
        Creates the handler responsible for matching given patterns with file paths associated with occurring events.

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
        self._event_handler = FileHandler(patterns=patterns,
                                          ignore_patterns=ignore_patterns,
                                          ignore_directories=ignore_directories,
                                          case_sensitive=case_sensitive,
                                          queue=self._queue)

        return self._event_handler

    def run(self) -> None:
        """
        Run the Akita watchdog.
        """
        self._observer.schedule(self._event_handler, self._path, recursive=True)
        self._observer.start()

        try:
            while True:
                time.sleep(1)
        finally:
            self._observer.stop()

        self._observer.join()

    def get_number_events(self) -> int:
        """
        Get the number of events in the queue.

        Returns
        -------
        Integer corresponding to the number of events in the queue.
        """
        return self._queue.qsize()

    def get_event(self):
        """
        Get the first event in the queue.
        """
        return self._queue.get()
