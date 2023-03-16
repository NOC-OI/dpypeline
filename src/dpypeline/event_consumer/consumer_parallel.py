"""ConsumerParallel. Acts as an interface between Akita and the ETL pipeline."""
import logging
import time
from typing import Any

import dask
from dask.distributed import Client  # , as_completed

from .core import EventConsumer


class ConsumerParallel(EventConsumer):
    """
    ConsumerParallel that runs on a thread as a daemon process.

    This event consumer produce futures that are consumed by multiple Dask workers in parallel.
    Each future corresponds to one or multiple jobs that are run in serial by each worker.
    """

    def __init__(
        self, client: Client, workers_per_future: int = 1, *args, **kwargs
    ) -> None:
        """
        Initialize the event consumer.

        Parameters
        ----------
        client
            Dask client.
        args
            Arguments to pass to EventConsumer.__init__.
        kwargs
            Keyword arguments to pass to EventConsumer.__init__.
        """
        super().__init__(*args, **kwargs)

        self._client = client
        self._workers_per_future = workers_per_future
        self._futures: dict[str, Any] = None
        self._max_futures: int = (
            len(self._client.scheduler_info()["workers"]) // self._workers_per_future
        )

    def _purge_future(self, future: dask.Future) -> None:
        """Purge a future.

        Parameters
        ----------
        future
            Future that has succeeded.
        """
        del self._futures[future]

    def _submit_future(self, event: Any) -> dask.Future:
        """
        Submit a future to the Dask cluster.

        Parameters
        ----------
        event
            Event to be submitted.

        Returns
        -------
        dask.Future
        """
        if self._futures is None:
            self._futures = {}

        logging.info(f"Submitting future for: {event}")

        # Submit the future to the Dask cluster
        future = self._client.submit(self._job_producer.produce_jobs, event)

        # Add the future to the list of futures and to the events dictionary
        self._futures[future] = event

        return future

    def _create_futures(self) -> None:
        """Create futures."""
        # Get the events from the queue for which a future has not yet been created
        diff = set(self._queue.queue_list) - set(self._futures.values())
        for event in diff:
            if len(self._futures) < self._max_futures:
                self._submit_future(event)

    def _process_succeeded_future(self, future: dask.Future) -> None:
        """
        Process a future that has succeeded.

        Parameters
        ----------
        future
            Future that has succeeded.
        """
        event = self._futures[future]
        logging.info(f"Event consumed: {event}")

        # Remove event from the queue and purge the future
        self._queue.remove(event)
        self._purge_future(future)

    def _process_failed_future(self, future: dask.Future) -> None:
        """
        Process a future that has failed.

        Parameters
        ----------
        future
            Future that has failed.
        """
        event = self._futures[future]
        logging.warning(
            f"Event {event} finished with error: {future.result()}. Future will be retried.."
        )
        future.retry()

    def _process_finished_future(self, future: dask.Future) -> None:
        """
        Process a future that has finished.

        Parameters
        ----------
        future
            Future that has finished.

        Raises
        ------
        ValueError
            If the status of the future is not "finished" or "error".
        """
        if future.status == "finished":
            self._process_succeeded_future(future)
            # self._submit_future(future)
        elif future.status == "error":
            self._process_failed_future(future)
        elif future.status == "pending":
            pass
        else:
            raise ValueError(f"Invalid future status: {future.status}")

    def _process_futures(self) -> None:
        """Process futures as they finish."""
        # completed_futures = as_completed(self._futures, with_results=True, raise_errors=False)
        # for future in completed_futures:
        #    self._process_finished_future(future)

        for future in self._futures.copy():
            self._process_finished_future(future)

    def _run_worker(self, sleep_time: int = 5) -> None:
        """
        Run the worker thread.

        Callback function to be used as a target by Thread.

        Parameters
        ----------
        sleep_time
            Sleep time in seconds for which the thread is idle.
        """
        while True:
            # Create futures if there are events in the queue
            # that have not been 'futurized' yet and if the current number of futures
            # is less than the maximum number of futures
            self._create_futures()

            # Process the futures when they are finished
            self._process_futures()

            time.sleep(sleep_time)
