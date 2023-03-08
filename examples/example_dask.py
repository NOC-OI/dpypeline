"""Run the ETL pipeline explicitly defining everything."""
import logging
import time

from dask.distributed import Client, LocalCluster

from dpypeline.akita.core import Akita
from dpypeline.akita.factory import get_akita_dependencies
from dpypeline.etl_pipeline.core import Job, Task
from dpypeline.etl_pipeline.thread_pipeline import ThreadPipeline
from dpypeline.event_consumer.concurrent_consumer import ConcurrentConsumer


def print_event(event, region_dict):
    print(event, region_dict[event])
    time.sleep(1)


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
    etl_pipeline = ThreadPipeline()

    # Create the DASK LocalCluster and Client
    cluster = LocalCluster(n_workers=4)
    client = Client(cluster)

    # Create the event consumer that bridges Akita and the data pipeline
    event_consumer = ConcurrentConsumer(
        client=client, queue=akita.queue, job_producer=etl_pipeline
    )

    # Add to the queue
    region_dict = {}
    for i in range(1000):
        event = f"event_{i}"
        akita.queue.enqueue(event)
        region_dict[event] = event + "METADATA"

    # Create a job and add tasks to it
    job = Job(name="job_test")
    job.add_task(
        Task(function=print_event, args=(), kwargs={"region_dict": region_dict})
    )

    # Add job to pipeline
    etl_pipeline.add_job(job)

    # Run the EventConsumer and Akita (must be in this order)
    event_consumer.run(daemon=False)
