"""
Run the ETL pipeline using dpypeline's API-like interface.

In this example, we process a set of CSV files to convert the temperature column from Fahrenheit to Celsius.
The new dataframe with updated temperatures is then written to a new CSV file.



"""
import logging

from dask.distributed import Client, LocalCluster
from example_1_tasks import convert_fahrenheit_to_celsius, read_csv, write_csv

from dpypeline.akita.core import Akita
from dpypeline.akita.factory import get_akita_dependencies
from dpypeline.etl_pipeline.basic_pipeline import BasicPipeline
from dpypeline.etl_pipeline.core import Job, Task
from dpypeline.event_consumer.consumer_parallel import ConsumerParallel

if __name__ == "__main__":
    logging.basicConfig(
        format="%(levelname)s | %(asctime)s | %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Create Akita
    akita_dep = get_akita_dependencies(
        path="2023/",
        patterns=["*.csv"],
        ignore_patterns=None,
        ignore_directories=True,
        case_sensitive=True,
        glob_kwargs=None,
    )

    # Create the Akita (watchdog) instance
    # Monitor mode set to 'False' indicates that Akita will not actively
    # look for new files and will stop once all events are consumed.
    akita = Akita(*akita_dep, monitor=False)

    # Create a basic pipeline
    etl_pipeline = BasicPipeline()

    # Create a LocalCluster and connect a Dask Client to it
    cluster = LocalCluster(n_workers=32, threads_per_worker=1)
    client = Client(cluster)

    # Create the event consumer that bridges Akita and the data pipeline
    event_consumer = ConsumerParallel(
        cluster_client=client,
        workers_per_event=1,
        queue=akita.queue,
        job_producer=etl_pipeline,
    )

    # Define the jobs and respective tasks
    # 1. Open the csv as a pandas dataframe
    # 2. Convert the temperature column from Fahrenheit to Celsius
    # 3. Write the dataframe to a new csv file
    job = Job(name="convert_from_fahrenheit_to_celsius")
    job.add_task(
        Task(
            function=read_csv,
        )
    )
    job.add_task(
        Task(
            function=convert_fahrenheit_to_celsius,
            kwargs={"col_temp": "TEMP"},
        )
    )
    job.add_task(
        Task(
            function=write_csv,
            kwargs={"prefix": "celsius_2023/"},
        )
    )

    # Add job to pipeline
    # Note that multiple jobs can be added to the pipeline
    etl_pipeline.add_job(job)

    # Run the EventConsumer and Akita (must be in this order)
    akita.run()
    event_consumer.run()
