import logging
import os
import time
from queue import Queue

from watchdog.events import *


class FileHandler(PatternMatchingEventHandler):
    """
    Child class of PatternMatchingEventHandler.
    PatternMatchingEventHandler matches given patterns with file paths associated with occurring events.
    Whenever a file in the target path is modified, FileHandler puts the associated event in a queue.

    Attributes
    ----------
    _queue
        Queue where events are placed.
    """

    def __init__(
        self,
        patterns: str | list[str] = None,
        ignore_patterns: str | list[str] | None = None,
        ignore_directories: bool = False,
        case_sensitive: bool = True,
        queue: Queue = Queue(),
    ) -> None:
        """
        Calls the init method of the `PatternMatchingEventHandler` class and sets the queue.

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
        queue
            Queue where events are placed.
        """

        super().__init__(
            patterns=patterns,
            ignore_patterns=ignore_patterns,
            ignore_directories=ignore_directories,
            case_sensitive=case_sensitive,
        )

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
        Message to log when event is triggered.

        Parameters
        ----------
        event
            Event representing file or directory creation, deletion, modification or moving.

        Returns
        -------
        logging_msg
            Message to be logged.
        """
        logging_msg = f"{event}"
        logging.info(logging_msg)

        return logging_msg

    @staticmethod
    def is_file_size_stable(path: str, sleep_time: int = 1) -> bool:
        """
        Checks if the size of a given file is stable.

        Parameters
        ----------
        path
            Absolute or relative filepath of the file.
        sleep_time
            Sleep time between consecutive file size checks.

        Returns
        -------
        `True` if the file size is stable between two consecutive checks.
        """
        filesize = -1.0

        while filesize != os.path.getsize(path):
            filesize = os.path.getsize(path)
            time.sleep(sleep_time)

        return True

    def _process_event(self, event) -> Queue:
        """
        Puts the event in the queue of events.

        Parameters
        ----------
        event
            Event representing file or directory creation, deletion, modification or moving.

        Returns
        -------
        _queue
            Queue of events.
        """
        self._queue.put(event)

        return self._queue

    def on_created(
        self, event: FileCreatedEvent | DirCreatedEvent
    ) -> FileCreatedEvent | DirCreatedEvent:
        """
        Called when a file or directory is created.

        Parameters
        ----------
        event
            Event representing file or directory creation.

        Returns
        -------
        event
        """
        self._logging_message(event)

        if self.is_file_size_stable(event.src_path):
            self._process_event(event)

        return event

    def on_deleted(
        self, event: FileDeletedEvent | DirDeletedEvent
    ) -> FileDeletedEvent | DirDeletedEvent:
        """
        Called when a file or directory is deleted.

        Parameters
        ----------
        event
            Event representing file or directory deletion.

        Returns
        -------
        event
        """
        self._logging_message(event)

        return event

    def on_modified(
        self, event: FileModifiedEvent | DirModifiedEvent
    ) -> FileModifiedEvent | DirModifiedEvent:
        """
        Called when a file or directory is modified.

        Parameters
        ----------
        event
            Event representing file or directory modification.

        Returns
        -------
        event
        """
        self._logging_message(event)

        return event

    def on_moved(
        self, event: FileMovedEvent | DirMovedEvent
    ) -> FileMovedEvent | DirMovedEvent:
        """
        Called when a file or directory is moved.

        Parameters
        ----------
        event
            Event representing file or directory move/renaming.

        Returns
        -------
        event
        """
        self._logging_message(event)

        return event
