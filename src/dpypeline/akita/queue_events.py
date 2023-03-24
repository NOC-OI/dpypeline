"""Queue of events class."""
from __future__ import annotations

import logging
import os
import pickle
from queue import Empty, Full, Queue
from typing import Any

logger = logging.getLogger(__name__)


class Sentinel:
    """A Sentinel object to sinalise whether to close the queue when it is empty."""

    def __init__(self, active: bool = False) -> None:
        """Initialize the Sentinel object."""
        self._active = active

    def set_state(self, active: bool) -> bool:
        """Set the state of the sentinel."""
        self._active = active
        return self._active

    def __call__(self, *args: Any, **kwargs: Any) -> bool:
        """Return the state of the sentinel."""
        return self._active


class EventsQueue(Queue):
    """EventsQueue singleton class."""

    _instance: EventsQueue = None
    _initialized: bool = False
    _state_file_suffix: str = "queue_state.pickle"
    _processed_events_file_suffix: str = "processed_events.pickle"

    def __init__(self, maxsize: int = 0) -> None:
        """Initiate the EventsQueue singleton."""
        assert (
            os.getenv("CACHE_DIR") is not None
        ), "CACHE_DIR environmental variable is not set."

        if not self._initialized:
            logger.debug("Initializing singleton instance of EventsQueue.")
            super().__init__(maxsize=maxsize)
            self._initialized: bool = True
            self._processed_events: list[str] = None
            self._state_file = os.path.join(
                os.getenv("CACHE_DIR"), self._state_file_suffix
            )
            self._load_state()

        self._processed_events_file = str = os.path.join(
            os.getenv("CACHE_DIR"), self._processed_events_file_suffix
        )

        self._sentinel = Sentinel()

    def __new__(cls, maxsize: int = 0) -> EventsQueue:
        """
        Create a new instance of the EventsQueue class using the Singleton pattern.

        Returns
        -------
            Instance of the EventsQueue class.
        """
        if cls._instance is None:
            logger.debug("Creating singleton instance of EventsQueue.")
            cls._instance = super(EventsQueue, cls).__new__(cls)
            logger.debug(
                "Creation of singleton instance of EventsQueue has been succesful."
            )

        return cls._instance

    @property
    def queue_list(self) -> list[Any]:
        """
        Return the list of events in the queue.

        Returns
        -------
            List of events in the queue.
        """
        return list(self.queue)

    @property
    def sentinel(self) -> Any:
        """Return the sentinel."""
        return self._sentinel

    def set_sentinel_state(self, active: bool) -> bool:
        """
        Set the sentinel state.

        Parameters
        ----------
        active
            The new state of the sentinel.
        """
        return self._sentinel.set_state(active)

    @property
    def processed_events(self) -> list[Any]:
        """
        Return the list of events processed thus far.

        Returns
        -------
            List of events processed thus far.
        """
        if self._processed_events is None:
            # Only load processed events if first time called.
            self._load_processed_events()

        return self._processed_events

    def _save_state(self, logger_prefix="") -> None:
        """Save the state of the queue."""
        logger.debug(f"{logger_prefix}Saving state of the queue.")

        with open(self._state_file, "wb") as f:
            pickle.dump(self.queue, f)

        logger.debug(
            f"{logger_prefix}Save of the state of the queue has been saved succesful."
        )

    def _load_state(self) -> None:
        """Load the state of the queue."""
        logger.debug("Loading state of the queue.")

        if os.path.isfile(self._state_file):
            logger.debug(f"Found queue state file {self._state_file}")

            with open(self._state_file, "rb") as f:
                self.queue = pickle.load(f)
        else:
            logger.debug(f"No queue state file {self._state_file} was found.")

    def _load_processed_events(self) -> None:
        """Load the processed events."""
        logger.debug("Loading processed events.")

        if os.path.isfile(self._processed_events_file):
            logger.debug(f"Found processed events file {self._processed_events_file}.")

            with open(self._processed_events_file, "rb") as f:
                self._processed_events = pickle.load(f)
        else:
            logger.debug(
                f"No processed events file {self._processed_events_file} was found."
            )
            self._processed_events = []

    def _save_processed_events(self) -> None:
        """Save the events processed so far in this session."""
        logger.debug("Saving processed events.")
        with open(self._processed_events_file, "wb") as f:
            pickle.dump(self._processed_events, f)
        logger.debug("Save of processed events has been successful.")

    def enqueue(self, event: Any) -> bool:
        """Add an event to the queue.

        Everytime an event is added to the queue, the state of the queue is saved.

        Parameters
        ----------
        event
            Event to add to the queue.

        Returns
        -------
        True if the event was added to the queue.
        """
        try:
            logger.debug(f"Enqueuing event '{event}'.")
            self.put(event, block=False)
            self._save_state(logger_prefix="Enqueueing event: ")
            logger.debug(f"Event '{event}' has been enqueued succesfully.")
            return True
        except Full:
            raise Full("Queue is full.")

    def _set_as_processed(self, event: Any) -> None:
        """Set the event as processed."""
        # Set as processesed event
        if self._processed_events is None:
            self._processed_events = []

        self._processed_events.append(event)
        self._save_processed_events()

    def dequeue(self) -> Any:
        """
        Remove and return an event from the queue.

        If the queue is empty, returns None.

        Returns
        -------
        First event in the queue or None if the queue is empty.
        """
        try:
            logger.debug("-" * 79)
            logger.debug("Dequeuing event: Starting.")
            event = self.get(block=False)
            logger.debug(f"Dequeued event: {event}.")
            self._save_state(logger_prefix="Dequeuing event: ")
            self._set_as_processed(event)
            return event
        except Empty:
            return None

    def peek(self) -> Any:
        """
        Peek first item in the queue withou removing it.

        If the queue is empty, returns None.

        Returns
        -------
        First event in the queue or None if the queue is empty.
        """
        if self.get_queue_size():
            logger.debug("-" * 79)
            logger.debug("Peeking first item in the queue.")
            return self.queue[0]

    def get_queue_size(self) -> int:
        """
        Get the size of the queue.

        Returns
        -------
        Number of events in the queue.
        """
        return self.qsize()

    def remove(self, event: Any) -> int:
        """
        Remove an event from the queue.

        Notes
        -----
        Removing elments from arbitrary positions in the queue should be avoided,
        as it goes against the FIFO principle.
        However, this method is useful for removing events processed by
        parallel workers.

        Parameters
        ----------
        event
            Event to add to the queue.

        Returns
        -------
            True if the event was removed from the queue, False otherwise.
        """
        try:
            logger.debug("-" * 79)
            logger.debug(f"Removing event: {event}.")
            self.queue.remove(event)
            self._save_state(logger_prefix="Removing event: ")
            self._set_as_processed(event)
            return True
        except ValueError as e:
            logger.error(f"{e}")
            return False

    @classmethod
    def clear_instance(cls):
        """Clear the singleton instance."""
        cls._instance = None
