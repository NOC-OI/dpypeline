"""Test suite for the ETLPipeline core."""
from dpypeline.etl_pipeline.core import ETLPipeline, Job, Task


def test_create_task() -> None:
    """Test the Task class."""
    function = min
    task = Task(function=function)
    assert isinstance(task, Task)
    assert task.function == function
    assert task.args == ()
    assert task.kwargs == {}

    args = ([1, 2, 3],)
    task = Task(function=sum, args=args)
    assert task.function == sum
    assert task.args == args
    assert task.kwargs == {}
    assert task.run() == 6

    task = Task(function=sum)
    assert task.function == sum
    assert task.args == ()
    assert task.kwargs == {}
    assert task.run([3, 4, 7]) == 14

    kwargs = {"a": 1, "b": 2, "c": 3}

    def dummy_func(a, b, c):
        return a + b + c

    task = Task(function=dummy_func, kwargs=kwargs)
    assert task.function == dummy_func
    assert task.args == ()
    assert task.kwargs == kwargs
    assert task.run() == 6


def test_create_job() -> None:
    """Test the Job class."""
    job = Job(name="test_job")

    assert isinstance(job, Job)
    assert job.name == "test_job"

    # Add a task
    job.add_task(Task(function=sum, args=([1, 2, 3],)))

    assert job.tasks[0].function == sum
    assert job.tasks[0].args == ([1, 2, 3],)
    assert job.tasks[0].kwargs == {}
    assert job.tasks[0].run() == 6
    assert len(job.tasks) == 1

    # Add another task
    job.add_task(Task(function=min, args=([5, 6, 7],)))
    assert len(job.tasks) == 2

    # Remove a task
    job.remove_task(0)
    assert len(job.tasks) == 1
    assert job.tasks[0].function == min
    assert job.tasks[0].run() == 5


def test_etl_pipeline() -> None:
    """Test the ETLPipeline base class."""
    etl_pipeline = ETLPipeline()
    assert etl_pipeline.jobs == []

    # Create a job
    job = Job(name="test_job")

    # Add a task
    job.add_task(Task(function=sum, args=([1, 2, 3],)))
    etl_pipeline.add_job(job)

    assert job in etl_pipeline.jobs
    assert len(etl_pipeline.jobs) == 1
    assert etl_pipeline.jobs[0].tasks[0].run() == 6

    # Remove a job
    etl_pipeline.remove_job(0)
    assert len(etl_pipeline.jobs) == 0
