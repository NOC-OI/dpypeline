"""Run the ETL pipeline explicitly defining everything."""
import logging

import xarray as xr

from dpypeline.akita.core import Akita
from dpypeline.akita.factory import get_akita_dependencies
from dpypeline.etl_pipeline.core import Job, Task
from dpypeline.etl_pipeline.thread_pipeline import ThreadPipeline
from dpypeline.event_consumer.core import EventConsumer
from dpypeline.filesystems.object_store import ObjectStoreS3

from .thread_pipeline_tasks import (
    clean_cache_dir,
    clean_dataset,
    fix_name_vars,
    match_to_template,
    to_zarr,
)

if __name__ == "__main__":
    logging.basicConfig(
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

    # Create the data pipeline
    etl_pipeline = ThreadPipeline()

    # Create the event consumer that bridges Akita and the data pipeline
    event_consumer = EventConsumer(queue=akita.queue, job_producer=etl_pipeline)

    # Create the jasmin instance
    logging.info("Jasmin OS")
    jasmin = ObjectStoreS3(
        anon=False, store_credentials_json="jasmin_object_store_credentials.json"
    )
    bucket = "n06-dataset"
    # jasmin.rm("dpypline-test/*", recursive=True)
    #   jasmin.mkdir(bucket)

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
            kwargs={"chunks": {"x": 577, "y": 577, "time_counter": 1, "deptht": 5}},
        )
    )
    job.add_task(Task(function=fix_name_vars))
    job.add_task(Task(function=clean_dataset))
    job.add_task(
        Task(
            function=match_to_template,
            kwargs={"template": xr.open_dataset("template.nc")},
        )
    )
    job.add_task(
        Task(
            function=to_zarr,
            kwargs={
                "store": jasmin.get_mapper(bucket + "/n06.zarr"),
                "mode": "a",
                "append_dim": "time_counter",
            },
        )
    )
    job.add_task(Task(function=clean_cache_dir))

    # Add job to pipeline
    etl_pipeline.add_job(job)

    # Run the EventConsumer and Akita (must be in this order)
    event_consumer.run()
    akita.run()