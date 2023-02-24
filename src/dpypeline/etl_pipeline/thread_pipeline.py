"""Thread-based ETL pipeline."""
import logging
from typing import Any

from .core import ETLPipeline, Job


class ThreadPipeline(ETLPipeline):
    """Thread-based ETL pipeline."""

    def __init__(self, jobs: list[Job] = None) -> None:
        """Initialize the ETL pipeline."""
        super().__init__(jobs)

    def produce_jobs(self, event: Any) -> Any:
        """
        Produce tasks triggered by an event.

        Parameters
        ----------
        event
            Triggering event.
        """
        for job in self._jobs:
            logging.info(f"Running job {job.name}.")
            logging.info(f"Running function {job.tasks[0].function.__name__}.")
            result = job.tasks[0].function(
                event, *job.tasks[0].args, **job.tasks[0].kwargs
            )
            for task in job.tasks[1:]:
                logging.info(f"Running function {task.function.__name__}.")
                result = task.function(result, *task.args, **task.kwargs)
