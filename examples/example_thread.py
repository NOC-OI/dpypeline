"""Run the ETL pipeline explicitly defining everything."""
import logging
import os

import numpy as np
import xarray as xr

from dpypeline.akita.core import Akita
from dpypeline.akita.factory import get_akita_dependencies
from dpypeline.etl_pipeline.core import Job, Task
from dpypeline.etl_pipeline.thread_pipeline import ThreadPipeline
from dpypeline.event_consumer.core import EventConsumer
from dpypeline.filesystems.object_store import ObjectStoreS3


def clean_dataset(
    dataset: xr.Dataset,
    fill_value=np.nan,
    supression_value: float = 1e20,
    threshold: float = 1e-6,
    *args,
    **kwargs,
) -> str:
    """Clean a dataset."""
    fill_value = fill_value if fill_value is not None else np.NaN
    supression_value = supression_value if supression_value is not None else np.NaN

    for var in dataset.keys():
        tmp = dataset[var].load()
        tmp.values = tmp.fillna(fill_value)
        tmp.values = xr.where(
            np.abs(tmp - supression_value) < threshold, fill_value, tmp
        )
        tmp.encoding["_FillValue"] = fill_value

    cached_file = f"{os.environ['CACHE_DIR']}/transformed_{dataset.encoding['source'].rsplit('/', 1)[-1].rsplit('.', 1)[-2]}.nc"
    dataset.to_netcdf(cached_file)

    return cached_file


def to_zarr(dataset: xr.Dataset, *args, **kwargs):
    return dataset.to_zarr(*args, **kwargs)


def clean_cache_dir(_):
    import glob

    file_list = glob.glob(
        os.path.join(os.environ["CACHE_DIR"], "") + "*.nc", recursive=True
    )
    for file in file_list:
        try:
            os.remove(file)
        except OSError:
            logging.info(f"Failed to delete {file}")

    return


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

    # Create the event consumer that bridges Akita and the data pipeline
    event_consumer = EventConsumer(queue=akita.queue, job_producer=etl_pipeline)

    # Create the jasmin instance
    jasmin = ObjectStoreS3(
        anon=False, store_credentials_json="jasmin_object_store_credentials.json"
    )
    bucket = "dpypline-test"
    # jasmin.rm("dpypline-test/*", recursive=True)
    #   jasmin.mkdir(bucket)

    # Define the jobs and respective tasks
    job = Job(name="send_to_jasmin_OS")
    job.add_task(Task(function=xr.open_dataset))
    job.add_task(Task(function=clean_dataset))
    job.add_task(
        Task(
            function=xr.open_dataset,
            kwargs={"chunks": {"x": 577, "y": 577, "time_counter": 1, "deptht": 5}},
        )
    )
    job.add_task(
        Task(
            function=to_zarr, kwargs={"store": jasmin.get_mapper(bucket + "/n06.zarr")}
        )
    )
    job.add_task(Task(function=clean_cache_dir))

    # Add job to pipeline
    etl_pipeline.add_job(job)

    # Run the EventConsumer and Akita (must be in this order)
    event_consumer.run()
    akita.run()
