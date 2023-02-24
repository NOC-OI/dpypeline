"""Celer-based ETL pipeline."""
import logging
from typing import Any

from celery import chain, group

from .core import ETLPipeline, Job


class CeleryPipeline(ETLPipeline):
    """Celery-based ETL pipeline."""

    def __init__(sel, jobs: list[Job] = None) -> None:
        """Initialize the celery-based ETL pipeline."""
        super().__init__(jobs)

    def _create_chain(self, event, job: Job) -> chain:
        """
        Create a Celery chain associated with a job.

        Parameters
        ----------
        event
            Triggering event.
        job
            Job to associate the task to.

        Returns
        -------
            Celery chain associated with job.
        """
        chained_tasks = chain(
            [job.tasks[0].function.si(event, *job.tasks[0].args, **job.tasks[0].kwargs)]
            + [task.function.s(*task.args, **task.kwargs) for task in job.tasks[1:]]
        )

        return chained_tasks

    def _chain_all_jobs(self, event: Any) -> list[chain]:
        """
        Chain tasks within all jobs.

        Parameters
        ----------
        event
            Triggering event.

        Returns
        -------
            List of Celery chains associated with all jobs.
        """
        if self._jobs is None or len(self._jobs) == 0:
            raise ValueError("No jobs to chain.")
        else:
            return [self._create_chain(event, job) for job in self._jobs]

    def _group_jobs(self, event: Any) -> group:
        """
        Group all jobs by first chaining tasks within all jobs.

        Parameters
        ----------
        event
            Triggering event.

        Returns
        -------
            Celery group associated with all jobs.
        """
        return group(self._chain_all_jobs(event))

    def produce_jobs(self, event: Any) -> Any:
        """
        Produce tasks triggered by an event.

        Parameters
        ----------
        event
            Triggering event.
        """
        group = self._group_jobs(event)
        logging.info("-" * 79)
        logging.info(f"Celery group created: {group}")
        return group.apply_async()
