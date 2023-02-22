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

        Notes
        -----
        https://docs.python.org/3/library/threading.html
        """
