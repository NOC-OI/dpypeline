"""ETL pipeline definitions."""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


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
        self.tasks.append(task)

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


class ETLPipeline(ABC):
    """Abstract base class for ETL pipelines."""

    def __init__(self, jobs: list[Job] = None) -> None:
        """Initialize the ETL pipeline."""
        self._jobs = jobs if jobs is not None else []

    @property
    def jobs(self) -> list[Job]:
        """List of jobs."""
        return self._jobs

    def add_job(self, job: Job) -> None:
        """
        Add a job to the pipeline.

        Parameters
        ----------
        job
            Job to add.
        """
        return self._jobs.append(job)

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

    @abstractmethod
    def produce_jobs(self, event: Any) -> Any:
        """Abstraction for producing tasks triggered by an event."""
        pass
