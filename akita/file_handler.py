import logging
import datetime
from queue import Queue
from watchdog.events import PatternMatchingEventHandler
from watchdog.events import FileCreatedEvent
from watchdog.events import FileDeletedEvent
from watchdog.events import FileModifiedEvent
from watchdog.events import FileMovedEvent


class FileHandler(PatternMatchingEventHandler):
    """
    Watchdog that sniffs and guards a given path in the local filesystem.
    """
    def __init__(self, patterns: str | list[str] = None, ignore_patterns: str | list[str] | None = None,
                 ignore_directories: bool = False, case_sensitive: bool = True, queue: Queue = Queue()):

        super().__init__(patterns=patterns,
                         ignore_patterns=ignore_patterns,
                         ignore_directories=ignore_directories,
                         case_sensitive=case_sensitive)

        self._queue = queue

    @property
    def queue(self) -> Queue:
        return self._queue

    @queue.setter
    def queue(self, queue: Queue) -> None:
        self._queue = queue

    @queue.deleter
    def queue(self) -> None:
        del self._queue

    @staticmethod
    def _logging_message(event) -> str:
        """

        Parameters
        ----------
        event

        Returns
        -------
        logging_msg: str
            Message to be logged.
        """
        logging_msg = f"{event}"
        logging.info(logging_msg)
        return logging_msg

    def _process_event(self, event: FileCreatedEvent) -> Queue:
        self._queue.put(event)

        return self._queue

    def on_created(self, event) -> FileCreatedEvent:
        self._logging_message(event)
        return event

    def on_deleted(self, event) -> FileDeletedEvent:
        self._logging_message(event)
        return event

    def on_modified(self, event) -> FileModifiedEvent:
        self._logging_message(event)
        self._process_event(event)

        return event

    def on_moved(self, event) -> FileMovedEvent:
        logging.info(f"{datetime.datetime.now()}:{event}")
        return event

