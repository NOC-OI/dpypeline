"""ETL pipeline definitions."""
import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Task:
    """
    Task class.

    Attributes
    ----------
    function
        Task function.
    args
        Arguments to be passed to the function.
    kwargs
        Keyword arguments to be passed to the function.
    """

    function: Any
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)

    def run(self, *args, **kwargs) -> Any:
        """
        Run the task.

        Notes
        -----
        Args passed upon instantiation are passed first to the function.

        Parameters
        ----------
        args
            Further arguments to be passed to the function.
        kwargs
            Furhter keyword arguments to be passed to the function.

        Returns
        -------
            Result of the task.
        """
        logger.debug(
            f"Running task {self.function} with args {args} and kwargs {kwargs}"
        )
        new_args = self.args + args
        new_kwargs = {**self.kwargs, **kwargs}
        result = self.function(*new_args, **new_kwargs)
        logger.debug(f"Task {self.function} has been run successfully.")

        return result


@dataclass
class Job:
    """
    A job is a collection of tasks.

    Parameters
    ----------
    name
        Job name.
    tasks
        List of tasks.
    """

    name: str = field(default_factory=str)
    tasks: list[Task] = field(default_factory=list)

    def add_task(self, task: Task) -> None:
        """
        Add a task to the job.

        Parameters
        ----------
        task
            Task to add.
        """
        logger.debug(f"Adding task {task} to job {self.name}.")
        self.tasks.append(task)
        logger.debug(f"Task {task} has been successfully added to job {self.name}.")

    def remove_task(self, index: int, task: Task = None) -> Task:
        """
        Remove a task from the job.

        Parameters
        ----------
        index
            Index of the task to remove.
        task, optional
            Task to remove.

        Returns
        -------
            Removed task.
        """
        if task is Task:
            return self.tasks.pop(self.tasks.index(task))
        else:
            return self.tasks.pop(index)

    def run(self, *args, **kwargs) -> Any:
        """
        Run the job.

        Returns
        -------
            Result of the job.
        """
        logger.debug(f"Running job {self.name}.")

        result = self.tasks[0].run(*args, **kwargs)
        for task in self.tasks[1:]:
            result = task.run(result)

        logger.debug(f"Job {self.name} has run successfully.")

        return result


class ETLPipeline:
    """Base class for ETL pipelines."""

    def __init__(self, jobs: list[Job] = None) -> None:
        """Initialize the ETL pipeline."""
        self._jobs = jobs if jobs is not None else []

    @property
    def jobs(self) -> list[Job]:
        """List of jobs."""
        return self._jobs

    def add_job(self, job: Job) -> list[Job]:
        """
        Add a job to the pipeline.

        Parameters
        ----------
        job
            Job to add.

        Returns
        -------
            List of jobs.
        """
        logger.debug(f"Adding job {job.name} to pipeline.")
        self._jobs.append(job)
        logger.debug(f"Job {job.name} has been successfully added to pipeline.")
        return self._jobs

    def remove_job(self, index: int, job: Job = None) -> Any:
        """
        Remove a job from the pipeline.

        Parameters
        ----------
        index
            Index of the job to remove.
        job, optional
            Job to remove.

        Returns
        -------
            Removed job.
        """
        if job is Job:
            return self._jobs.pop(self._jobs.index(job))
        else:
            return self._jobs.pop(index)

    def produce_jobs(self, event: Any) -> Any:
        """Produce jobs triggered by an event."""
        raise NotImplementedError("produce_jobs must be implemented.")
