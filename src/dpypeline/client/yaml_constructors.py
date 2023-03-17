import importlib

import yaml
from dask.distributed import Client

from dpypeline.akita.core import Akita
from dpypeline.akita.factory import get_akita_dependencies
from dpypeline.etl_pipeline.basic_pipeline import BasicPipeline
from dpypeline.etl_pipeline.celery_pipeline import CeleryPipeline
from dpypeline.etl_pipeline.core import Job, Task
from dpypeline.event_consumer.consumer_parallel import ConsumerParallel
from dpypeline.event_consumer.consumer_serial import ConsumerSerial
from dpypeline.filesystems.object_store import ObjectStoreS3


def dask_client_constructor(loader: yaml.SafeLoader, node: yaml.MappingNode) -> Client:
    """Dask client constructor."""
    params = loader.construct_mapping(node)
    module_name, func_name = params["cluster"].rsplit(".", 1)
    module = importlib.import_module(module_name)

    cluster = getattr(module, func_name)
    kwargs = {key: params[key] for key in params if key not in ["cluster", "scale"]}
    cluster = cluster(**kwargs)
    cluster.scale(params["scale"])

    return Client(cluster)


def object_store_constructor(
    loader: yaml.SafeLoader, node: yaml.MappingNode
) -> ObjectStoreS3:
    """Construct an Object Store S3 instance."""
    return ObjectStoreS3(**loader.construct_mapping(node))


def akita_constructor(loader: yaml.SafeLoader, node: yaml.nodes.MappingNode) -> Akita:
    """Construct an Akita instance."""
    dependencies = get_akita_dependencies(**loader.construct_mapping(node))
    return Akita(*dependencies)


def consumer_serial_constructor(
    loader: yaml.SafeLoader, node: yaml.nodes.MappingNode
) -> ConsumerSerial:
    """Construct a ConsumerSerial instance."""
    params = loader.construct_mapping(node)
    kwargs = {key: params[key] for key in params if key != "akita"}
    return ConsumerSerial(queue=params["akita"].queue, **kwargs)


def consumer_parallel_constructor(
    loader: yaml.SafeLoader, node: yaml.nodes.MappingNode
) -> ConsumerParallel:
    """Construct a ConsumerParallel instance."""
    params = loader.construct_mapping(node)
    kwargs = {key: params[key] for key in params if key != "akita"}
    return ConsumerParallel(queue=params["akita"].queue, **kwargs)


def celery_pipeline_constructor(
    loader: yaml.SafeLoader, node: yaml.nodes.MappingNode
) -> CeleryPipeline:
    """Construct a CeleryPipeline instance."""
    return CeleryPipeline(**loader.construct_mapping(node))


def basic_pipeline_constructor(
    loader: yaml.SafeLoader, node: yaml.nodes.MappingNode
) -> BasicPipeline:
    """Construct a BasicPipeline instance."""
    return BasicPipeline(**loader.construct_mapping(node))


def job_constructor(loader: yaml.SafeLoader, node: yaml.nodes.MappingNode) -> Job:
    """Construct a Job."""
    return Job(**loader.construct_mapping(node))


def task_constructor(loader: yaml.SafeLoader, node: yaml.nodes.MappingNode) -> Task:
    """Construct a Job."""
    params = loader.construct_mapping(node)
    kwargs = {key: params[key] for key in params if key != "function"}
    return Task(function=params["function"], kwargs=kwargs)


constructors_dict = {
    "!Job": job_constructor,
    "!Task": task_constructor,
    "!Akita": akita_constructor,
    "!ConsumerSerial": consumer_serial_constructor,
    "!ConsumerParallel": consumer_parallel_constructor,
    "!BasicPipeline": basic_pipeline_constructor,
    "!CeleryPipeline": celery_pipeline_constructor,
    "!DaskClient": dask_client_constructor,
    "!ObjectStoreS3": object_store_constructor,
}
