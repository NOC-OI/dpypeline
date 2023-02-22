"""Thread-based ETL pipeline."""
from typing import Any

from etl_pipeline.etl_pipeline import ETLPipeline


class ThreadPipeline(ETLPipeline):
    """Thread-based ETL pipeline."""

    def __init__(self) -> None:
        """Initialize the thread-based ETL pipeline."""
        pass

    def produce_jobs(self, event: Any) -> Any:
        """
        Produce tasks triggered by an event.

        Notes
        -----
        https://docs.python.org/3/library/threading.html
        """
