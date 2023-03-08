"""ConcurrentConsumer. Acts as an interface between Akita and the ETL pipeline."""
import logging
import os
import pickle
import time
from typing import Any

from dask.distributed import Client, as_completed

from .core import EventConsumer


class ConcurrentConsumer(EventConsumer):
    """
    ConcurrentConsumer that runs on a thread as a daemon process.

    This event consumer consumes events from an in-memory queue and puts them in a local list.
    This local list is processed to produce futures that are consumed by multiple Dask workers in parallel.
    Each future corresponds to one or multiple jobs that are run in serial by each worker.
    Whenever new events are added to the queue, they are put in the local list and processed to produce futures.
    """

    _events_file: str = os.path.join(os.getenv("CACHE_DIR"), "consumer_events.pickle")

    def __init__(self, client: Client, *args, **kwargs) -> None:
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
        self._events_list: list[Any] = []

    def _load_events_list(self) -> None:
        """Load the events list."""
        logging.info("-" * 79)
        logging.info("Loading events list for concurrent event consumer.")

        if os.path.isfile(self._events_file):
            logging.info(f"Found events list file {self._events_file}.")

            with open(self._events_file, "rb") as f:
                self._events_list = pickle.load(f)
        else:
            logging.info(f"No events list file {self._events_file} was found.")
            self._events_list = []

    def _save_events_list(self) -> None:
        """Save the events list."""
        logging.info("-" * 79)
        logging.info("Saving events list.")

        with open(self._events_file, "wb") as f:
            pickle.dump(self._events_list, f)

    def _run_worker(self, sleep_time: int = 1) -> None:
        """
        Run the worker thread.

        Callback function to be used as a target by Thread.

        Parameters
        ----------
        sleep_time
            Sleep time in seconds for which the thread is idle.
        """
        futures = []
        events_processed = []

        self._load_events_list()

        while True:
            if self._queue.get_queue_size():
                logging.info("-" * 79)
                # If there are events in the queue, consume them by adding them to a local list
                event = self._queue.dequeue()
                self._events_list.append(event)
                self._save_events_list()
                time.sleep(sleep_time)
            else:
                # If there are no events in the queue
                # process the local list of events and check for finished futures

                for event in self._events_list:
                    if event not in events_processed:
                        logging.info(f"Creating future for: {event}")
                        futures.append(
                            self._client.submit(self._job_producer.produce_jobs, event)
                        )
                        events_processed.append(event)

                for batch in as_completed(futures, with_results=True).batches():
                    for future, result in batch:
                        if future.status == "finished":
                            logging.info(f"Event consumed: {event}")
                            self._events_list.remove(event)
                            events_processed.remove(event)
                            futures.remove(future)
                            del future
                        elif future.status == "error":
                            logging.warning(
                                f"Event {event} finished with error: {result}. Event will be processed again."
                            )
                            events_processed.remove(event)
                            futures.remove(future)
                            del future
                        else:
                            raise ValueError(f"Unknown future status: {future.status}")
