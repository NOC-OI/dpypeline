"""Celery app configuration."""
import os

assert (
    os.getenv("BROKER_URL") is not None
), "BROKER_URL environmental variable is not set."

broker_url = os.getenv("BROKER_URL")
# result_backend = "db+sqlite:///etl_pipeline.sqlite/"
imports = ("dpypeline.celery.tasks.xarray",)

task_serializer = "pickle"
result_serializer = "pickle"
accept_content = ["pickle"]

"""
task_routes = {
    "apds_file_mover.tasks.erddap_move": {"queue": "nrt_file_mover_erddap"},
    "apds_file_mover.tasks.archive_move": {"queue": "nrt_file_mover_archive"},
    "apds_file_mover.tasks.zip_files": {"queue": "nrt_zip_files"},
    "apds_file_mover.tasks.zip_and_move_file_to_erddap": {"queue": "nrt_file_zipper_mover"},
}

task_queues = {
    "nrt_file_mover_erddap": {"exchange": "data.outgoing", "routing_key": "NRT.file_mover.erddap"},
    "nrt_file_mover_archive": {"exchange": "data.outgoing", "routing_key": "NRT.file_mover.archive"},
    "nrt_zip_files": {"exchange": "data.outgoing", "routing_key": "NRT.zip_files"},
    "nrt_file_zipper_mover": {"exchange": "data.outgoing", "routing_key": "NRT.file.zipper_mover"},
}
"""

# Use extended results.
result_extended = True

# use custom table names for the database result backend.
# database_table_names = {"task": "etl_pipeline"}
