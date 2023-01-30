import time
from queue import Queue

from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver
from .file_handler import FileHandler


class Akita:
    def __init__(self,
                 path: str,
                 observer: Observer | PollingObserver = PollingObserver(),
                 queue: Queue = Queue(),
                 run_init: bool = False):

        self._path = path
        self._observer = observer
        self._queue = queue
        self._event_handler = self._create_event_handler()

        if run_init:
            self.run()

    def _create_event_handler(self, patterns: str | list[str] = ["*.nc"], ignore_patterns: str | list[str] | None = None,
                              ignore_directories: bool = True, case_sensitive: bool = True) -> FileHandler:

        self._event_handler = FileHandler(patterns=patterns,
                                          ignore_patterns=ignore_patterns,
                                          ignore_directories=ignore_directories,
                                          case_sensitive=case_sensitive,
                                          queue=self._queue)

        return self._event_handler

    def run(self):
        """
        Run Akita watchdog.
        """
        self._observer.schedule(self._event_handler, self._path, recursive=True)
        self._observer.start()

        try:
            while True:
                time.sleep(1)
        finally:
            self._observer.stop()

        self._observer.join()

    def get_number_events(self):
        return self._queue.qsize()

    def get_event(self) -> str:
        """
        Get the first event on the queue.
        """
        return self._queue.get()
