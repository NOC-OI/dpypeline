"""Queue of events class."""
from __future__ import annotations

import logging
from queue import Empty, Full, Queue
from typing import Any


class EventsQueue(Queue):
    """EventsQueue singleton class."""

    _instance: EventsQueue = None
    _initialized: bool = False

    def __init__(self, maxsize: int = 0) -> None:
        """Initiate the EventsQueue singleton."""
        if not self._initialized:
            logging.info("Initializing singleton instance of EventsQueue.")
            super().__init__(maxsize=maxsize)
            self._initialized: bool = True

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

    def enqueue(self, event) -> bool:
        """Add an event to the queue.

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
        try:
            logging.info("Peeking first item in the queue.")
            return self.queue[0]
        except Empty:
            return None

    def get_queue_size(self) -> int:
        """
        Get the size of the queue.

        Returns
        -------
        Number of events in the queue.
        """
        return self.qsize()
