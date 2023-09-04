"""pyyaml constructors."""
import importlib

import yaml
from dask.distributed import Client

from ..akita.core import Akita
from ..akita.factory import get_akita_dependencies
from ..etl_pipeline.basic_pipeline import BasicPipeline
from ..etl_pipeline.core import Job, Task
from ..event_consumer.consumer_parallel import ConsumerParallel
from ..event_consumer.consumer_serial import ConsumerSerial
from ..filesystems.object_store import ObjectStoreS3


def dask_client_constructor(loader: yaml.SafeLoader, node: yaml.MappingNode) -> Client:
    """Create a Dask client bound to a cluster ."""
    params = loader.construct_mapping(node, deep=True)
    module_name, func_name = params["cluster"].rsplit(".", 1)
    module = importlib.import_module(module_name)

    cluster = getattr(module, func_name)
    kwargs = {
        str(key): params[key] for key in params if key not in ["cluster", "scale"]
    }
    cluster = cluster(**kwargs)

    if "scale" in params:
        if isinstance(params["scale"], dict):
            cluster.scale(**params["scale"])
        elif isinstance(params["scale"], list):
            cluster.scale(*params["scale"])
        else:
            cluster.scale(params["scale"])

    return Client(cluster)


def object_constructor(loader: yaml.SafeLoader, node: yaml.MappingNode) -> object:
    """Create an object."""
    params = loader.construct_mapping(node, deep=True)
    kwargs = {str(key): params[key] for key in params if key != "class"}

    params = loader.construct_mapping(node, deep=True)
    module_name, class_name = params["class"].rsplit(".", 1)
    module = importlib.import_module(module_name)

    return getattr(module, class_name)(**kwargs)


def object_store_constructor(
    loader: yaml.SafeLoader, node: yaml.MappingNode
) -> ObjectStoreS3:
    """Construct an Object Store S3 instance."""
    kwargs = {
        str(key): val for key, val in loader.construct_mapping(node, deep=True).items()
    }
    return ObjectStoreS3(**kwargs)


def akita_constructor(loader: yaml.SafeLoader, node: yaml.nodes.MappingNode) -> Akita:
    """Construct an Akita instance."""
    params = loader.construct_mapping(node, deep=True)
    kwargs = {str(key): params[key] for key in params if key not in ["monitor"]}
    dependencies = get_akita_dependencies(**kwargs)
    return Akita(*dependencies, monitor=params["monitor"])


def consumer_serial_constructor(
    loader: yaml.SafeLoader, node: yaml.nodes.MappingNode
) -> ConsumerSerial:
    """Construct a ConsumerSerial instance."""
    params = loader.construct_mapping(node, deep=True)
    kwargs = {str(key): params[key] for key in params if key not in ["akita"]}
    akita = params["akita"]

    return ConsumerSerial(queue=akita.queue, **kwargs)


def consumer_parallel_constructor(
    loader: yaml.SafeLoader, node: yaml.nodes.MappingNode
) -> ConsumerParallel:
    """Construct a ConsumerParallel instance."""
    params = loader.construct_mapping(node, deep=True)
    kwargs = {str(key): params[key] for key in params if key not in ["akita"]}
    return ConsumerParallel(queue=params["akita"].queue, **kwargs)


# TODO: uncomment this when celery pipeline is ready
# def celery_pipeline_constructor(
#     loader: yaml.SafeLoader, node: yaml.nodes.MappingNode
# ) -> CeleryPipeline:
#     """Construct a CeleryPipeline instance."""
#     kwargs = {
#         str(key): val for key, val in loader.construct_mapping(node, deep=True).items()
#     }
#     return CeleryPipeline(**kwargs)


def basic_pipeline_constructor(
    loader: yaml.SafeLoader, node: yaml.nodes.MappingNode
) -> BasicPipeline:
    """Construct a BasicPipeline instance."""
    kwargs = {
        str(key): val for key, val in loader.construct_mapping(node, deep=True).items()
    }
    return BasicPipeline(**kwargs)


def job_constructor(loader: yaml.SafeLoader, node: yaml.nodes.MappingNode) -> Job:
    """Construct a Job."""
    kwargs = {
        str(key): val for key, val in loader.construct_mapping(node, deep=True).items()
    }
    return Job(**kwargs)


def task_constructor(loader: yaml.SafeLoader, node: yaml.nodes.MappingNode) -> Task:
    """Construct a Job."""
    params = loader.construct_mapping(node, deep=True)
    kwargs = {key: params[key] for key in params if key != "function"}

    params = loader.construct_mapping(node, deep=True)
    module_name, func_name = params["function"].rsplit(".", 1)
    module = importlib.import_module(module_name)

    return Task(function=getattr(module, func_name), kwargs=kwargs)


constructors_dict = {
    "!Job": job_constructor,
    "!Task": task_constructor,
    "!Akita": akita_constructor,
    "!ConsumerSerial": consumer_serial_constructor,
    "!ConsumerParallel": consumer_parallel_constructor,
    "!BasicPipeline": basic_pipeline_constructor,
    # "!CeleryPipeline": celery_pipeline_constructor,
    "!DaskClient": dask_client_constructor,
    "!ObjectStoreS3": object_store_constructor,
}
