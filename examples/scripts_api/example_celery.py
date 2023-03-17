"""Run the ETL pipeline explicitly defining everything."""
import logging

from dpypeline.akita.core import Akita
from dpypeline.akita.factory import get_akita_dependencies
from dpypeline.celery.tasks.xarray import clean_dataset, open_dataset, to_zarr
from dpypeline.etl_pipeline.celery_pipeline import CeleryPipeline
from dpypeline.etl_pipeline.core import Job, Task
from dpypeline.event_consumer.consumer_serial import ConsumerSerial
from dpypeline.filesystems.object_store import ObjectStoreS3

if __name__ == "__main__":
    logging.basicConfig(
        format="%(levelname)s | %(asctime)s | %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Create Akita
    akita_dep = get_akita_dependencies(
        path="./",
        patterns=["*.nc"],
        ignore_patterns=None,
        ignore_directories=True,
        case_sensitive=True,
        glob_kwargs=None,
    )
    akita = Akita(*akita_dep)

    # Create the data pipeline
    etl_pipeline = CeleryPipeline()

    # Create the event consumer that bridges Akita and the data pipeline
    event_consumer = ConsumerSerial(queue=akita.queue, job_producer=etl_pipeline)

    # Create the jasmin instance
    # jasmin = ObjectStoreS3(
    #    anon=False, store_credentials_json="utils/jasmin_object_store_credentials.json"
    # )

    # Create a job and add tasks to it
    job = Job(name="send_to_jasmin_OS")
    job.add_task(Task(function=open_dataset, args=(), kwargs={}))
    job.add_task(Task(function=clean_dataset))
    """
    job.add_task(
        Task(
            function=to_zarr,
            kwargs={
                "store": jasmin.get_mapper("msm-repository/zarr-part/"),
                "mode": "a",
            },
        )
    )
    """
    # Add job to pipeline
    etl_pipeline.add_job(job)

    # Run the EventConsumer and Akita (must be in this order)
    event_consumer.run()
    akita.run()
