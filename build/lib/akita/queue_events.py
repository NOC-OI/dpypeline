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
    _state_file: str = "queue_state.pickle"

    def __init__(self, maxsize: int = 0) -> None:
        """Initiate the EventsQueue singleton."""
        if not self._initialized:
            logging.info("Initializing singleton instance of EventsQueue.")
            super().__init__(maxsize=maxsize)
            self._initialized: bool = True

            if os.path.isfile(os.getenv("CACHE_DIR") + self._state_file):
                logging.info(
                    f"Found queue state file {os.getenv('CACHE_DIR') + self._state_file}"
                )
                self._load_state()
            else:
                logging.info(
                    f"No queue state file {os.getenv('CACHE_DIR') + self._state_file}"
                )

    def __new__(cls, maxsize: int = 0) -> EventsQueue:
        """
        Create a new instance of the EventsQueue class using the Singleton pattern.

        Returns
        -------
            Instance of the EventsQueue class.
        """
        if cls._instance is None:
            logging.info("Creating singleton instance of EventsQueue.")
            cls._instance = super(EventsQueue, cls).__new__(cls)

        return cls._instance

    def _save_state(self) -> None:
        """Save the state of the queue."""
        logging.info("Saving state of the queue.")
        with open(os.getenv("CACHE_DIR") + self._state_file, "wb") as f:
            pickle.dump(self.queue, f)

    def _load_state(self) -> None:
        """Load the state of the queue."""
        logging.info("Loading state of the queue.")
        with open(os.getenv("CACHE_DIR") + self._state_file, "rb") as f:
            self.queue = pickle.load(f)

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
            logging.info(f"Enqueuing event:{event}.")
            self.put(event, block=False)
            self._save_state()
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
            logging.info("Dequeuing event.")
            return self.get(block=False)
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
