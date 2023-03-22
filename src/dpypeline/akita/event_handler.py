"""Filehandler for the watchdog."""

import logging
import os
import time
from typing import Any, Protocol

from watchdog.events import (
    DirCreatedEvent,
    DirDeletedEvent,
    DirModifiedEvent,
    DirMovedEvent,
    FileCreatedEvent,
    FileDeletedEvent,
    FileModifiedEvent,
    FileMovedEvent,
    PatternMatchingEventHandler,
)


class Queue(Protocol):
    """Queue interface."""

    def enqueue(self, event: Any) -> Any:
        """Add an event to the queue."""
        ...


class EventHandler(PatternMatchingEventHandler):
    """
    EventHandler for the watchdog.

    Child class of PatternMatchingEventHandler.
    PatternMatchingEventHandler matches given patterns with file paths
    associated with occurring events.
    Whenever a file in the target path is modified, EventHandler puts the
    associated event in a queue.
    """

    def __init__(
        self,
        queue: Queue,
        patterns: str | list[str] = None,
        ignore_patterns: str | list[str] | None = None,
        ignore_directories: bool = False,
        case_sensitive: bool = True,
    ) -> None:
        """
        Initialize the EventHandler.

        Notes
        -----
        Call the init method of the `PatternMatchingEventHandler`
        class and set the queue.

        Parameters
        ----------
        queue
            Queue where events are placed.
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
        self._queue = queue

        super().__init__(
            patterns=patterns,
            ignore_patterns=ignore_patterns,
            ignore_directories=ignore_directories,
            case_sensitive=case_sensitive,
        )

    @staticmethod
    def _logging_message(event) -> str:
        """
        Message to log when event is triggered.

        Parameters
        ----------
        event
            Event representing file or directory creation, deletion,
            modification or moving.

        Returns
        -------
        logging_msg
            Message to be logged.
        """
        logging_msg = f"{event}"
        logging.info("-" * 79)
        logging.info(logging_msg)

        return logging_msg

    @staticmethod
    def is_file_size_stable(path: str, sleep_time: int = 1) -> bool:
        """
        Check if the size of a given file is stable.

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

    def _process_event(self, event) -> None:
        """
        Put the event in the queue of events.

        Parameters
        ----------
        event
            Event representing file or directory creation, deletion,
            modification or moving.
        """
        self._queue.enqueue(event.src_path)

    def on_created(
        self, event: FileCreatedEvent | DirCreatedEvent
    ) -> FileCreatedEvent | DirCreatedEvent:
        """
        Return event when a file or directory is created.

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
        Return event when a file or directory is deleted.

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
        Return event when a file or directory is modified.

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
        Return event when a file or directory is moved.

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
