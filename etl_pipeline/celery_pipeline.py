"""Celer-based ETL pipeline."""
from typing import Any

from celery import chain, group

from .etl_pipeline import ETLPipeline, Job


class CeleryPipeline(ETLPipeline):
    """Celery-based ETL pipeline."""

    def __init__(self) -> None:
        """Initialize the celery-based ETL pipeline."""
        pass

    def _create_chain(self, event, job: Job) -> chain:
        """
        Create a Celery chain associated with a job.

        Parameters
        ----------
        event
            Event representing file or directory creation.
        job
            Job to associate the task to.

        Returns
        -------
            Celery chain associated with job.
        """
        chained_task = [
            job.tasks[0].function.si(event, *job.tasks[0].args, **job.tasks[0].kwargs)
        ]
        for task in job.tasks[1:]:
            chained_task.append(task.function.s(event, *task.args, **task.kwargs))

        return chain(*chained_task)

    def _chain_all_jobs(self, event: Any) -> list[chain]:
        """
        Chain tasks within all jobs.

        Parameters
        ----------
        event
            Event representing file or directory creation.

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
            Event representing file or directory creation.

        Returns
        -------
            Celery group associated with all jobs.
        """
        return group(*self._chain_all_jobs(event))

    def produce_jobs(self, event: Any) -> Any:
        """
        Produce tasks triggered by an event.

        Parameters
        ----------
        event
            Event representing file or directory creation.
        """
        return self._group_jobs(event).apply_async()
