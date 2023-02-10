import json
import logging

from akita.core import Akita
from argument_parser import args
from etl.etl_pipeline import ETLPipeline
from etl.extract import setup_extract_action
from etl.load import setup_load_action
from etl.transform import setup_transform_action
from filesystems.object_store import ObjectStoreS3

if __name__ == "__main__":
    logging.basicConfig(
        format="%(levelname)s | %(asctime)s | %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with open(args.input_file) as f:
        etl = json.load(f)

    try:
        # Create an instance of the object store
        obj_store = ObjectStoreS3(**etl["ObjectStore"])
    except KeyError:
        raise KeyError("ObjectStore")

    try:
        # Create an instance of Akita, the watchdog
        akita = Akita(**etl["Akita"])
    except KeyError:
        raise KeyError("Akita")

    exit()
    try:
        etl_pipeline = etl["ETLPipeLine"]

        for task in etl_pipeline:
            if task["stage"].lowercase() == "extract":
                setup_extract_action(task["task"], **task["kwargs"])
            elif task["stage"].lowercase() == "transform":
                setup_transform_action(task["task"], **task["kwargs"])
            elif task["stage"].lowercase() == "load":
                setup_load_action(task["task"], **task["kwargs"])
            else:
                raise ValueError(f"{task['stage']}")

        etl = ETLPipeline(watchdog=akita)
        etl.run()
    except KeyError:
        raise KeyError("No ETLPipeline was defined in the input file.")
