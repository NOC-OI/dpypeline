"""Thread-based ETL pipeline."""
import logging
from typing import Any

from .core import ETLPipeline, Job

logger = logging.getLogger(__name__)

class BasicPipeline(ETLPipeline):
    """Basic ETL pipeline."""

    def __init__(self, jobs: list[Job] = None) -> None:
        """Initialize the ETL pipeline."""
        super().__init__(jobs)

    def produce_jobs(self, event: Any) -> list[Any]:
        """
        Run the jobs sequentially.

        Parameters
        ----------
        event
            Triggering event.

        Returns
        -------
            List of results of the jobs.
        """
        results = []
        for job in self._jobs:
            logger.debug(f"Running job {job.name} for event {event}.")
            results.append(job.run(event))
            logger.debug(f"Job {job.name} for event {event} has run successfully.")

        return results
