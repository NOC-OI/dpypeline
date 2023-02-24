"""Akita, the watchdog class."""
import logging
import time
from itertools import chain
from typing import Any, Protocol


class Queue(Protocol):
    """Queue class protocol."""

    def enqueue(self, event: Any) -> bool:
        """Enqueue an event."""
        ...

    @property
    def queue_list(self) -> list[Any]:
        """Return the list of events in the queue."""
        ...

    @property
    def processed_events(self) -> list[Any]:
        """Return the list of processed events."""
        ...


class DirectoryState(Protocol):
    """DirectoryState class protocol."""

    @property
    def stored_state(self) -> list[str]:
        """Return the stored state of the directory."""
        ...

    @property
    def current_state(self) -> list[str]:
        """Return the current state of the directory."""


class EventHandler(Protocol):
    """EventHandler class protocol."""

    def on_created(self, event: Any) -> None:
        """Return event when a file or directory is created."""
        ...

    def on_deleted(self, event: Any) -> None:
        """Return event when a file or directory is deleted."""
        ...

    def on_modified(self, event: Any) -> None:
        """Return event when a file or directory is modified."""
        ...

    def on_moved(self, event: Any) -> None:
        """Return event when a file or directory is moved."""
        ...


class Observer(Protocol):
    """Observer class protocol."""

    def start(self) -> None:
        """Start the obsever."""
        ...

    def stop(self) -> None:
        """Stop the observer."""
        ...

    def join(self) -> None:
        """Join the observer."""
        ...

    def schedule(self, event_handler: EventHandler, path: str, recursive: bool) -> None:
        """Schedules watching a path."""
        ...


class Akita:
    """
    Akita, the watchdog.

    The Akita watchdog holds the queue, observer, and event handler instances.
    The in-memory Queue is created upon instantiation and should be a singleton to provide a global point of access.

    Attributes
    ----------
    _path
        Path to watch.
    _queue
        Queue where events are placed by the event handler.
    _event_handler
        Handler responsible for matching given patterns with file paths associated with occurring events.
    _directory_state
        Instance that holds the state of the directory.
    _observer
        Observer thread that schedules watching directories and dispatches calls to event handlers.
    _event_handler
        Handler responsible for matching given patterns with file paths associated with occurring events.
    """

    def __init__(
        self,
        path: str,
        queue: Queue,
        event_handler: EventHandler,
        directory_state: DirectoryState,
        observer: Observer,
        run_init: bool = False,
    ) -> None:
        """
        Initialize the Akita watchdog.

        Parameters
        ----------
        path
            Path to watch.
        queue
            Queue where events are placed by the event handler.
        event_handler
            Handler responsible for matching given patterns with file paths associated with occurring events.
        directory_state
            Instance that holds the state of the directory.
        observer
            Observer thread that schedules watching directories and dispatches calls to event handlers.
        run_init
            If `True` runs the watchdog. `False` otherwise.
        """
        self._path = path
        self._queue = queue
        self._observer = observer
        self._event_handler = event_handler
        self._directory_state = directory_state

        if run_init:
            self.run()

    @property
    def queue(self):
        """Return the queue."""
        return self._queue

    @queue.deleter
    def queue(self):
        """Delete the queue."""
        logging.info("-" * 79)
        logging.info("Deleting in-memory queue.")
        logging.info("-" * 79)

        del self._queue

    def _get_unenqueued_files(self) -> list[str]:
        """
        Return a list of files that are not enqueued.

        Notes
        -----
        At the beggining of the run,
          Akita determines which files in the the directory being monitored have never been enqueued.
        This is done as follows:

        n = d - (s V q V p)

        where

        - n: events not enqueued.
        - s: events in the stored directory state.
        - d: events in the current directory state.
        - q: events in the queue when the previous session terminated.
        - p: events processed in the previous session.

        Returns
        -------
            List of files to enqueue.
        """
        s = self._directory_state.stored_state
        d = self._directory_state.current_state
        q = self._queue.queue_list
        p = self._queue.processed_events

        n = set(d) - set(chain.from_iterable([s, q, p]))

        logging.info("-" * 79)
        logging.info(f"Events in the stored directory state (n={len(s)}):")
        for event in s:
            logging.info(f"{event}")

        logging.info(f"Events in the current directory state (n={len(d)}):")
        for event in d:
            logging.info(f"{event}")

        logging.info(
            f"Events in the queue when the previous session terminated (n={len(q)}):"
        )
        for event in q:
            logging.info(f"{event}")

        logging.info(
            f"Events processed before the previous session terminated (n={len(p)}):"
        )
        for event in p:
            logging.info(f"{event}")

        logging.info(f"Events unenqueued (n={len(n)}):")
        for event in n:
            logging.info(f"{event}")

        logging.info(
            f"Found {len(n)} files not enqueued in the current state of the directory."
        )

        logging.info("-" * 79)

        return list(n)

    def _enqueue_new_files(self) -> None:
        """Enqueue events previously unqueued."""
        # Get current state of the directory
        not_enqueued_state = sorted(self._get_unenqueued_files())

        for event in not_enqueued_state:
            self._queue.enqueue(event)

    def run(self) -> None:
        """Run the Akita watchdog."""
        # Add new files found in the directory since the last time the watchdog was run.
        self._enqueue_new_files()

        logging.info("Starting the Akita watchdog.")
        self._observer.schedule(self._event_handler, self._path, recursive=True)
        self._observer.start()

        try:
            while True:
                time.sleep(1)
        finally:
            self._observer.stop()

        self._observer.join()
