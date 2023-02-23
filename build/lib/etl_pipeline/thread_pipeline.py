"""Thread-based ETL pipeline."""
from typing import Any

from etl_pipeline.etl_pipeline import ETLPipeline, Job


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
            result = job.tasks[0].function(
                event, *job.tasks[0].args, **job.tasks[0].kwargs
            )
            for task in job.tasks[1:]:
                result = task.function(result, *task.args, **task.kwargs)
