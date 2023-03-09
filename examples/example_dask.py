"""Run the ETL pipeline explicitly defining everything."""
import logging
import time

import xarray as xr
from dask.distributed import Client, LocalCluster
from thread_pipeline_tasks import (
    clean_cache_dir,
    clean_dataset,
    create_reference_names_dict,
    match_to_template,
    rename_vars,
    to_zarr,
)

from dpypeline.akita.core import Akita
from dpypeline.akita.factory import get_akita_dependencies
from dpypeline.etl_pipeline.core import Job, Task
from dpypeline.etl_pipeline.thread_pipeline import ThreadPipeline
from dpypeline.event_consumer.concurrent_consumer import ConcurrentConsumer
from dpypeline.filesystems.object_store import ObjectStoreS3


def print_event(event, region_dict):
    print(region_dict)
    # print(event, region_dict[event])
    time.sleep(1)


if __name__ == "__main__":
    logging.basicConfig(
        format="%(levelname)s | %(asctime)s | %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Create Akita
    akita_dep = get_akita_dependencies(
        path="/home/joaomorado/PycharmProjects/msm_project/nc_files/",
        patterns=["*.nc"],
        ignore_patterns=None,
        ignore_directories=True,
        case_sensitive=True,
        glob_kwargs=None,
    )
    akita = Akita(*akita_dep)
    akita.enqueue_new_files()

    # Create the data pipeline
    etl_pipeline = ThreadPipeline()

    # Create the DASK LocalCluster and Client
    cluster = LocalCluster(n_workers=1)
    client = Client(cluster)

    # Create the event consumer that bridges Akita and the data pipeline
    event_consumer = ConcurrentConsumer(
        client=client, queue=akita.queue, job_producer=etl_pipeline
    )

    # Add to the queue
    region_dict = {}
    idx = 0

    for event in akita.queue.queue_list:
        region_dict[event] = idx
        idx += 1

    # Create the jasmin instance
    logging.info("Jasmin OS")
    jasmin = ObjectStoreS3(
        anon=False,
        store_credentials_json="/home/joaomorado/git_repos/mynamespace/dpypeline/examples/credentials.json",
    )
    bucket = "new-workflow-test"
    # jasmin.rm(bucket + "/*", recursive=True)
    # jasmin.mkdir(bucket)

    template = xr.open_dataset("template.nc", cache=False)
    """
    total_template = xr.open_mfdataset(akita.queue.queue_list)
    total_template.to_zarr(jasmin.get_mapper(bucket+ "/n06.zarr"), compute=False)
    """

    print(template["sst"].dims)
    exit()
    # Create a job and add tasks to it
    job = Job(name="job_test")

    # Define the jobs and respective tasks
    # 1. Open the data set
    # 2. Fix variables' names
    # 3. Clean the dataset
    # 4. Combine dataset with template
    # 5. Send to zarr
    # 6. Clean cache directory
    job = Job(name="send_to_jasmin_OS")
    job.add_task(
        Task(
            function=xr.open_dataset,
        )
    )
    job.add_task(
        Task(
            function=rename_vars,
            kwargs={"reference_names": create_reference_names_dict(template)},
        )
    )
    job.add_task(Task(function=clean_dataset))
    job.add_task(
        Task(
            function=match_to_template,
            kwargs={"template": template},
        )
    )
    job.add_task(
        Task(
            function=to_zarr,
            kwargs={
                "store": jasmin.get_mapper(bucket + "/n06.zarr"),
                "region_dict": region_dict,
            },
        )
    )
    job.add_task(Task(function=clean_cache_dir))

    etl_pipeline.add_job(job)

    # Run the EventConsumer and Akita (must be in this order)
    event_consumer.run(daemon=False)
