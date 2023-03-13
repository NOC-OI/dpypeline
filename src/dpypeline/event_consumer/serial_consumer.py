"""SerialConsumer. Acts as an interface between Akita and the ETL pipeline."""
import logging
import time

from .core import EventConsumer


class SerialConsumer(EventConsumer):
    """
    SerialConsumer that runs on a thread as a daemon process.

    This event consumer consumes events from an in-memory queue and processes them to produce jobs that are run in serial.
    """

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
