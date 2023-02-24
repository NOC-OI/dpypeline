"""Event consumer. Acts as an interface between Akita and the ETL pipeline."""
import logging
import time
from threading import Thread
from typing import Any, Protocol


class JobProducer(Protocol):
    """Job producer interface."""

    def produce_jobs(self, event: Any) -> None:
        """Produce jobs triggered by an event."""
        ...


class Queue(Protocol):
    """Queue interface."""

    def dequeue(self) -> Any:
        """Remove and return an event from the queue."""
        ...

    def peek(self) -> Any:
        """Peek first item in the queue withou removing it."""
        ...

    def get_queue_size(self) -> int:
        """Return the size of the queue."""
        ...


class EventConsumer:
    """
    Event consumer that runs on a thread as a daemon process.

    This event consumer consumes events from an in-memory queue and processes them to produce jobs.

    Attributes
    ----------
    _queue
        Queue where events are placed.
    _job_producer
        Producer of jobs.
    _worker, optional
        Worker thread that consumes events from the and processes them to produce jobs.
    """

    def __init__(
        self, queue: Queue, job_producer: JobProducer, worker: Thread = None
    ) -> None:
        """
        Initialize the event consumer.

        Parameters
        ----------
        queue
            Queue where events are placed.
        job_producer
            Producer of jobs.
        worker, optional
            Worker thread that consumes events from the and processes them to produce jobs.
        """
        self._job_producer = job_producer
        self._queue = queue
        self._worker = worker

    def _consume_event(self, event) -> None:
        """
        Consume event by producing jobs.

        Parameters
        ----------
        event
            Event representing file or directory creation, deletion, modification or moving.
        """
        self._job_producer.produce_jobs(event=event)

    def _run_worker(self, sleep_time: int = 1) -> None:
        """
        Run the worker thread.

        Callback function to be used as a target by Thread.

        Parameters
        ----------
        sleep_time
            Sleep time in seconds for which the thread is idle.
        """
        while True:
            if self._queue.get_queue_size():
                event = self._queue.peek()
                logging.info("-" * 79)
                logging.info(f"Consuming event: {event}")
                self._consume_event(event)
                logging.info(f"Event consumed: {event}")
                self._queue.dequeue()
            else:
                time.sleep(sleep_time)

    def _create_worker(self, daemon: bool = True) -> Thread:
        """
        Create the worker thread.

        Parameters
        ----------
        daemon
            If `True`, runs the thread as a daemon; otherwise thread is not created as a daemon.

        Returns
        -------
            Worker thread that consumes events from the in-memory queue and processes them to produce jobs.
        """
        # Set up a worker thread to process database load
        self._worker = Thread(target=self._run_worker, daemon=daemon)

        return self._worker

    def run(self) -> None:
        """Run the event consumer."""
        try:
            if self._worker is None:
                self._create_worker()

            self._worker.start()
        except Exception:
            self._worker.join()
