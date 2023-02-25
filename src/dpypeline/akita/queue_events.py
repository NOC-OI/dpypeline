"""Queue of events class."""
from __future__ import annotations

import logging
import os
import pickle
from queue import Empty, Full, Queue
from typing import Any


class EventsQueue(Queue):
    """EventsQueue singleton class."""

    _instance: EventsQueue = None
    _initialized: bool = False
    _state_file: str = os.path.join(os.getenv("CACHE_DIR"), "queue_state.pickle")
    _processed_events_file: str = os.path.join(
        os.getenv("CACHE_DIR"), "processed_events.pickle"
    )

    def __init__(self, maxsize: int = 0) -> None:
        """Initiate the EventsQueue singleton."""
        if not self._initialized:
            logging.info("Initializing singleton instance of EventsQueue.")
            super().__init__(maxsize=maxsize)
            self._initialized: bool = True
            self._processed_events: list[str] = None
            self._load_state()

        assert (
            os.getenv("CACHE_DIR") is not None
        ), "CACHE_DIR environmental variable is not set."

    def __new__(cls, maxsize: int = 0) -> EventsQueue:
        """
        Create a new instance of the EventsQueue class using the Singleton pattern.

        Returns
        -------
            Instance of the EventsQueue class.
        """
        if cls._instance is None:
            logging.info("-" * 79)
            logging.info("Creating singleton instance of EventsQueue.")
            cls._instance = super(EventsQueue, cls).__new__(cls)

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

    def _save_state(self, logging_prefix="") -> None:
        """Save the state of the queue."""
        logging.info("-" * 79)
        logging.info(f"{logging_prefix}Saving state of the queue.")

        with open(self._state_file, "wb") as f:
            pickle.dump(self.queue, f)

    def _load_state(self) -> None:
        """Load the state of the queue."""
        logging.info("-" * 79)
        logging.info("Loading state of the queue.")

        if os.path.isfile(self._state_file):
            logging.info(f"Found queue state file {self._state_file}")

            with open(self._state_file, "rb") as f:
                self.queue = pickle.load(f)
        else:
            logging.info(f"No queue state file {self._state_file} was found.")

    def _load_processed_events(self) -> None:
        """Load the processed events."""
        logging.info("-" * 79)
        logging.info("Loading processed events.")

        if os.path.isfile(self._processed_events_file):
            logging.info(f"Found processed events file {self._processed_events_file}.")

            with open(self._processed_events_file, "rb") as f:
                self._processed_events = pickle.load(f)
        else:
            logging.info(
                f"No processed events file {self._processed_events_file} was found."
            )
            self._processed_events = []

    def _save_processed_events(self) -> None:
        """Save the events processed so far in this session."""
        logging.info("-" * 79)
        logging.info("Saving processed events.")

        with open(self._processed_events_file, "wb") as f:
            pickle.dump(self._processed_events, f)

    def enqueue(self, event) -> bool:
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
            logging.info("-" * 79)
            logging.info(f"Enqueuing event: {event}.")
            self.put(event, block=False)
            self._save_state(logging_prefix="Enqueueing event: ")
            return True
        except Full:
            raise Full("Queue is full.")

    def dequeue(self) -> Any:
        """
        Remove and return an event from the queue.

        If the queue is empty, returns None.

        Returns
        -------
        First event in the queue or None if the queue is empty.
        """
        try:
            logging.info("-" * 79)
            logging.info("Dequeuing event: Starting.")
            event = self.get(block=False)
            logging.info(f"Dequeued event: {event}.")
            self._save_state(logging_prefix="Dequeuing event: ")

            # Set as processesed event
            if self._processed_events is None:
                self._processed_events = []

            self._processed_events.append(event)
            self._save_processed_events()
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
            logging.info("-" * 79)
            logging.info("Peeking first item in the queue.")
            return self.queue[0]

    def get_queue_size(self) -> int:
        """
        Get the size of the queue.

        Returns
        -------
        Number of events in the queue.
        """
        return self.qsize()

    @classmethod
    def clear_instance(cls):
        """Clear the singleton instance."""
        cls._instance = None
