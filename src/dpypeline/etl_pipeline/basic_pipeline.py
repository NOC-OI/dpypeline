"""Basic pipeline module."""
import logging
from typing import Any

from .core import ETLPipeline, Job

logger = logging.getLogger(__name__)


class BasicPipeline(ETLPipeline):
    """Basic pipeline class."""

    def __init__(self, jobs: list[Job] = None) -> None:
        """Initialise the pipeline."""
        super().__init__(jobs)

    def produce_jobs(self, event: Any) -> list[Any]:
        """
        Produce jobs to be run sequentially.

        Parameters
        ----------
        event
            Triggering event.

        Returns
        -------
            List of results of the jobs.
        """
        logger.debug(f"Producing jobs for event {event}.")
        results = [job.run(event) for job in self.jobs]
        logger.debug(f"Jobs for event {event} have been produced successfully.")

        return results
