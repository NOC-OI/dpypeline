"""Akita, the watchdog class."""
import logging
import time
from itertools import chain
from threading import Thread
from typing import Any, Protocol

logger = logging.getLogger(__name__)


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

    def set_sentinel_state(self, active: bool) -> bool:
        """Set the state of the end-of-queue sentinel."""
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

    def is_alive(self) -> bool:
        """Return whether the thread is alive."""
        ...


class Akita:
    """
    Akita, the watchdog.

    The Akita watchdog holds the queue, observer, and event handler instances.
    The in-memory Queue is created upon instantiation and should be a singleton
    to provide a global point of access.

    Attributes
    ----------
    _path
        Path to watch.
    _queue
        Queue where events are placed by the event handler.
    _event_handler
        Handler responsible for matching given patterns with file paths associated
        with occurring events.
    _directory_state
        Instance that holds the state of the directory.
    _observer
        Observer thread that schedules watching directories and dispatches calls to
        event handlers.
    _event_handler
        Handler responsible for matching given patterns with file paths associated with
        occurring events.
    _monitor
        Whether or not to run the watchdog.
    _worker
        Thread that runs the watchdog.
    """

    def __init__(
        self,
        path: str,
        queue: Queue,
        event_handler: EventHandler,
        directory_state: DirectoryState,
        observer: Observer,
        monitor: bool = True,
        worker: Thread = None,
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
            Handler responsible for matching given patterns with file paths
            associated with occurring events.
        directory_state
            Instance that holds the state of the directory.
        observer
            Observer thread that schedules watching directories and dispatches
            calls to event handlers.
        monitor
            If `True`, the watchdog will monitor the given path.
        worker
            Thread that runs the watchdog.
        """
        self._path = path
        self._queue = queue
        self._observer = observer
        self._event_handler = event_handler
        self._directory_state = directory_state
        self._worker = worker
        self._monitor = monitor

    @property
    def queue(self):
        """Return the queue."""
        return self._queue

    @queue.deleter
    def queue(self):
        """Delete the queue."""
        logger.info("-" * 79)
        logger.info("Deleting in-memory queue.")
        logger.info("-" * 79)

        del self._queue

    def _get_unenqueued_files(self) -> list[str]:
        """
        Return a list of files that are not enqueued.

        Notes
        -----
        At the beggining of the run,
          Akita determines which files in the the directory being
          monitored have never been enqueued.
        This is done as follows:

        n = d - (s V q V p)

        where

        - n / unqueued_events: events not enqueued.
        - s / stored_states: events in the stored directory state.
        - d / curr_states: events in the current directory state.
        - q / queue: events in the queue when the previous session terminated.
        - p / prev_sess_states: events processed in the previous session.

        Returns
        -------
            List of files to enqueue.
        """
        stored_states = self._directory_state.stored_state
        curr_states = self._directory_state.current_state
        queue = self._queue.queue_list
        prev_sess_states = self._queue.processed_events
        unqueued_events = set(curr_states) - set(
            chain.from_iterable([queue, prev_sess_states])
        )

        logger.info("-" * 79)

        logger.info(f"Events in the stored directory state (n={len(stored_states)}):")
        for event in stored_states:
            logger.info(f"{event}")

        logger.info(f"Events in the current directory state (n={len(curr_states)}):")
        for event in curr_states:
            logger.info(f"{event}")

        logger.info(
            "Events in the queue when the previous session terminated "
            + f"(n={len(queue)}):"
        )
        for event in queue:
            logger.info(f"{event}")

        logger.info(
            "Events processed before the previous session terminated "
            f"(n={len(prev_sess_states)}):"
        )
        for event in prev_sess_states:
            logger.info(f"{event}")

        logger.info(f"Events unenqueued (n={len(unqueued_events)}):")
        for event in unqueued_events:
            logger.info(f"{event}")

        logger.info(
            f"Found {len(unqueued_events)} files not enqueued "
            + "in the current state of the directory."
        )

        logger.info("-" * 79)

        return list(unqueued_events)

    def enqueue_new_files(self) -> None:
        """Enqueue events previously unqueued."""
        # Get current state of the directory
        # TODO: Check if delimiter has to be "\" when running on Windows.
        not_enqueued_state = sorted(
            self._get_unenqueued_files(), key=lambda f: f.rsplit("/", 1)[-1]
        )

        for event in not_enqueued_state:
            self._queue.enqueue(event)

    def _run_watchdog(self) -> None:
        """Run the watchdog."""
        logger.debug("Starting the watchdog...")
        self._observer.schedule(self._event_handler, self._path, recursive=True)
        self._observer.start()

        try:
            while self._observer.is_alive():
                time.sleep(1)
        finally:
            self._observer.stop()
            self._observer.join()

    def _create_worker(self, daemon: bool = False) -> Thread:
        """
        Create the worker thread.

        Parameters
        ----------
        daemon
            If `True`, runs the thread as a daemon; otherwise
            thread is not created as a daemon.

        Returns
        -------
            Worker thread that consumes events from the in-memory queue
            and processes them to produce jobs.
        """
        # Set up a worker thread to process database load
        self._worker = Thread(target=self._run_watchdog, daemon=daemon)

        return self._worker

    def _stop_worker(self, max_attempts=10) -> None:
        """
        Stop the worker thread.

        Parameters
        ----------
        max_attempts
            Maximum number of attempts to stop the worker thread.
        """
        while self._worker.is_alive() and max_attempts > 0:
            logger.debug(
                f"{max_attempts} attempts remaining to stop the worker thread."
            )
            if self._observer.is_alive():
                logger.debug("Stopping the watchdog...")
                self._observer.stop()
                self._observer.join()
                logger.debug("Watchdog has stopped successfully.")

            logger.debug("Attempting to stop the worker thread...")
            self._worker.join(timeout=1)
            max_attempts -= 1

        if max_attempts == 0:
            raise RuntimeError("Failed to stop the worker thread.")
        else:
            logger.debug("Worker thread has stopped successfully.")

    def stop(self, max_attempts=10) -> None:
        """
        Stop the observer and worker thread.

        Parameters
        ----------
        max_attempts
            Maximum number of attempts to stop the worker thread.
        """
        if self._worker is not None:
            logger.info("Stopping Akita...")
            self._stop_worker(max_attempts)
            logger.info("Akita has stopped successfully.")
        else:
            logger.info("Attempted to stop akita but no worker thread has been found.")

    def run(
        self, monitor: bool = True, enqueue_new_files: bool = True, daemon: bool = False
    ) -> None:
        """
        Run the Akita watchdog.

        Parameters
        ----------
        monitor
            If `True` monitors runs the watchdog. `False` otherwise.
        enqueue_new_files
            If `True`, enqueues events previously unqueued.
        daemon
            If `True`, runs the thread as a daemon.
        """
        if enqueue_new_files:
            # Add new files found in the directory since the last
            # time the watchdog was run.
            self.enqueue_new_files()

        if self._monitor:
            logger.info("Starting Akita in monitor mode...")
            self._queue.set_sentinel_state(active=False)

            try:
                if self._worker is None:
                    self._create_worker(daemon=daemon)

                self._worker.start()
            except Exception as excpt:
                self._worker.join()
                raise RuntimeError(f"Error while running the watchdog: {excpt}")
        else:
            self._queue.set_sentinel_state(active=True)
