"""ConsumerSerial. Acts as an interface between Akita and the ETL pipeline."""
import logging
import time

from .core import EventConsumer

logger = logging.getLogger(__name__)


class ConsumerSerial(EventConsumer):
    """
    ConsumerSerial that runs on a thread as a daemon process.

    This event consumer consumes events from an in-memory queue and processes
    them to produce jobs that are run in serial.
    """

    def _consume_event(self, event) -> None:
        """
        Consume event by producing jobs.

        Parameters
        ----------
        event
            Event representing file or directory creation, deletion,
            modification or moving.
        """
        logger.info(f"Consuming event '{event}'...")
        self._job_producer.produce_jobs(event=event)
        logger.info(f"Event '{event}' has been consumed successfully.")

    def _run_event_loop(self, sleep_time: int = 5) -> None:
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
                logger.info("-" * 79)
                self._consume_event(event)
                self._queue.dequeue()
            elif self._is_sentinel_active():
                logger.info("The queue is empty and got an end-of-queue sentinel")
                logger.info("The event consumer is exiting...")
                break
            else:
                time.sleep(sleep_time)
