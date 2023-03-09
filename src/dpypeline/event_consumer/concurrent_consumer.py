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

    _events_dict_file_suffix: str = "consumer_dict.pickle"

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
        self._events_dict: dict[Any, str] = None

        assert (
            os.getenv("CACHE_DIR") is not None
        ), "CACHE_DIR environmental variable is not set."

        self._events_dict_file = str = os.path.join(
            os.getenv("CACHE_DIR"), self._events_dict_file_suffix
        )

    def _load_events_dict(self) -> None:
        """Load the events dictionary."""
        logging.info("-" * 79)
        logging.info("Loading events dictionary for concurrent event consumer.")

        if os.path.isfile(self._events_dict_file):
            logging.info(f"Found events dictionary file {self._events_dict_file}.")

            with open(self._events_dict_file, "rb") as f:
                self._events_dict = pickle.load(f)

            # Set future keys to None
            for event in self._events_dict:
                self._events_dict[event] = None
        else:
            logging.info(
                f"No events dictionary file {self._events_dict_file} was found."
            )
            self._events_dict = {}

    def _save_events_dict(self) -> None:
        """Save the events dictionary."""
        logging.info("-" * 79)
        logging.info("Saving events dictionary.")

        with open(self._events_dict_file, "wb") as f:
            pickle.dump(self._events_dict, f)

    def _remove_event_from_dict(self, ref_future_key: str) -> Any:
        """
        Remove an event from the events dictionary given a future key.

        Parameters
        ----------
        ref_future_key
            Future key corresponding to the event to be removed.

        Returns
        -------
        event
            Removed event.
        """
        for event, future_key in self._events_dict.items():
            if future_key == ref_future_key:
                del self._events_dict[event]
                return event

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
        self._load_events_dict()

        while True:
            if self._queue.get_queue_size():
                # If there are events in the queue, consume them by adding them to a local list
                event = self._queue.dequeue()
                self._events_dict[event] = None
                self._save_events_dict()
            else:
                # If there are no events in the queue
                # process the local list of events and check for finished futures
                for event, future_key in self._events_dict.items():
                    if future_key is None:
                        logging.info(f"Creating future for: {event}")
                        future = self._client.submit(
                            self._job_producer.produce_jobs, event
                        )
                        futures.append(future)
                        self._events_dict[event] = future.key

                for batch in as_completed(futures, with_results=True).batches():
                    for future, result in batch:
                        if future.status == "finished":
                            event = self._remove_event_from_dict(future.key)
                            logging.info(f"Event consumed: {event}")
                            futures.remove(future)
                            del future
                        elif future.status == "error":
                            logging.warning(
                                f"Event {event} finished with error: {result}. Event will be processed again."
                            )
                            futures.remove(future)
                            del future
                        else:
                            raise ValueError(f"Unknown future status: {future.status}")

                time.sleep(sleep_time)
