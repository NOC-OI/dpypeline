"""Event consumer. Acts as an interface between Akita and the ETL pipeline."""
from threading import Thread
from typing import Any, Protocol


class JobProducer(Protocol):
    """Job producer interface."""

    def produce_jobs(self, event: Any) -> Any:
        """Produce jobs triggered by an event."""
        ...


class Queue(Protocol):
    """Queue interface."""

    @property
    def queue_list(self) -> list[Any]:
        """Return list of events in the queue."""
        ...

    @property
    def sentinel(self) -> Any:
        """Return the sentinel value."""
        ...

    def dequeue(self) -> Any:
        """Remove and return an event from the queue."""
        ...

    def peek(self) -> Any:
        """Peek first item in the queue withou removing it."""
        ...

    def get_queue_size(self) -> int:
        """Return the size of the queue."""
        ...

    def enqueue(self, event: Any) -> bool:
        """Add an event to the queue."""
        ...

    def remove(self, event: Any) -> bool:
        """Remove an event from the queue."""
        ...


class EventConsumer:
    """
    Base class for event consumers.

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
            Worker thread that consumes events from the and
            processes them to produce jobs.
        """
        self._job_producer = job_producer
        self._queue = queue
        self._worker = worker

    def _is_sentinel_active(self) -> bool:
        """Return True if the sentinel is active."""
        return self._queue.sentinel()

    def _run_event_loop(self, *args, **kwargs) -> None:
        """
        Run the worker thread.

        Callback function to be used as a target by Thread.
        """
        raise NotImplementedError("Run worker not implemented.")

    def _create_worker(self, daemon: bool = True) -> Thread:
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
        self._worker = Thread(target=self._run_event_loop, daemon=daemon)

        return self._worker

    def run(self, daemon: bool = False) -> None:
        """Run the event consumer.

        daemon
            If `True` and the worker has not been passed to self
              runs the thread as a daemon; otherwise thread is non-daemon.
        """
        try:
            if self._worker is None:
                self._create_worker(daemon=daemon)

            self._worker.start()
        except Exception as excpt:
            self._worker.join()
            raise RuntimeError(f"Error while running event consumer: {excpt}")

        self._worker.join()
