"""Run the ETL pipeline explicitly defining everything."""
import logging

from akita.core import Akita
from celery_app.app import app
from celery_app.tasks.xarray import open_dataset, to_zarr
from etl_pipeline.celery_pipeline import CeleryPipeline
from etl_pipeline.etl_pipeline import Job, Task
from event_consumer.event_consumer import EventConsumer

if __name__ == "__main__":
    logging.basicConfig(
        format="%(levelname)s | %(asctime)s | %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Celery app
    app = app

    # Create akita, data pipeline, and event consumer
    akita = Akita(path=".")
    etl_pipeline = CeleryPipeline()
    event_consumer = EventConsumer(queue=akita.queue, job_producer=etl_pipeline)

    # Create a job and add tasks to it
    job = Job(name="test_job")
    job.add_task(Task(function=open_dataset))
    job.add_task(Task(function=to_zarr))

    # Add job to pipeline
    etl_pipeline.add_job(job)

    # Run the EventConsumer and Akita (must be in this order)
    event_consumer.run()
    akita.run()
