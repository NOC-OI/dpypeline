"""Run the ETL pipeline explicitly defining everything."""
import logging
import sys

import xarray as xr
from dask.distributed import Client, LocalCluster
from dask_jobqueue import SLURMCluster
from thread_pipeline_tasks import (
    clean_dataset,
    create_reference_names_dict,
    match_to_template,
    open_dataset,
    rename_vars,
    to_zarr,
)

from dpypeline.akita.core import Akita
from dpypeline.akita.factory import get_akita_dependencies
from dpypeline.etl_pipeline.core import Job, Task
from dpypeline.etl_pipeline.thread_pipeline import ThreadPipeline
from dpypeline.event_consumer.concurrent_consumer import ConcurrentConsumer
from dpypeline.event_consumer.serial_consumer import SerialConsumer
from dpypeline.filesystems.object_store import ObjectStoreS3

if __name__ == "__main__":
    logging.basicConfig(
        stream=sys.stdout,
        format="%(levelname)s | %(asctime)s | %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Create Akita
    akita_dep = get_akita_dependencies(
        path="/gws/nopw/j04/nemo_vol1/ORCA0083-N006/means/",
        patterns=["**/ORCA*-N06_*m*T.nc"],
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
    # cluster = LocalCluster(n_workers=12, threads_per_worker=2)
    cluster = SLURMCluster(
        name="dask",
        processes=4,
        cores=16,
        memory="128 GB",
        queue="par-single",
        walltime="24:00:00",
    )
    cluster.scale(jobs=3)
    client = Client(cluster)

    # Create the event consumer that links Akita and the data pipeline
    # Since we are using a Dask Cluster, we need to use the ConcurrentConsumer and not the SerialConsumer
    event_consumer = ConcurrentConsumer(
        client=client, queue=akita.queue, job_producer=etl_pipeline
    )

    # Create dicitonary that contains the index of the region to write to on the destination zarr store
    region_dict = {}
    idx = 0
    for event in akita.queue.queue_list:
        region_dict[event] = idx
        idx += 1

    # Create the jasmin instance
    logging.info("Jasmin OS")
    jasmin = ObjectStoreS3(
        anon=False,
        store_credentials_json="credentials.json",
    )
    bucket = "joaomorado"
    # jasmin.mkdir(bucket)

    # Open template
    template = xr.open_zarr("template.zarr")

    # Create a job and add tasks to it
    job = Job(name="job_test")

    # Define the jobs and respective tasks
    # 1. Open the data set
    # 2. Fix the names of the variables
    # 3. Clean the dataset (uniformize fill and missing values)
    # 4. Project dataset onto template
    # 5. Send to zarr
    job = Job(name="send_to_jasmin_OS")
    job.add_task(
        Task(
            function=open_dataset,
            kwargs={
                "persist": False,
                "chunks": {"time_counter": 1, "deptht": 5, "x": 577, "y": 577},
            },
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
                "mode": "a",
                "region_dict": region_dict,
            },
        )
    )
    etl_pipeline.add_job(job)

    # Run the EventConsumer and Akita (must be in this order)
    event_consumer.run()
    akita.run()
