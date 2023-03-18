import importlib.util
import logging
import os
import sys

import yaml

from .argument_parser import __version__, create_parser
from .yaml_loader import get_loader

logger = logging.getLogger(__name__)


def banner():
    logger.info(
        r"""
      _                             _  _
     | |                           | |(_)
   __| | _ __   _   _  _ __    ___ | | _  _ __    ___
  / _` || '_ \ | | | || '_ \  / _ \| || || '_ \  / _ \
 | (_| || |_) || |_| || |_) ||  __/| || || | | ||  __/
  \__,_|| .__/  \__, || .__/  \___||_||_||_| |_| \___|
        | |      __/ || |
        |_|     |___/ |_|
""",
        extra={"simple": True},
    )
    logger.info(f"version: {__version__}\n", extra={"simple": True})


def load_imports_yaml(imports_file: str) -> dict:
    """Load the imports yaml file."""
    with open(imports_file, "r") as stream:
        try:
            imports_settings = yaml.load(stream, Loader=get_loader())
        except yaml.YAMLError as exc:
            raise yaml.YAMLError(exc)

    # Import dependencies
    try:
        imports = imports_settings["imports"]
        for name, path in imports.items():
            spec = importlib.util.spec_from_file_location(name, path)
            module = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = module
            spec.loader.exec_module(module)
    except KeyError:
        pass

    # Set environment variables
    try:
        for env_var, value in imports_settings["environment_variables"].items():
            os.environ[env_var] = value
    except KeyError:
        pass

    return imports_settings


def load_pipeline_yaml(pipeline_file: str) -> dict:
    """Load the pipeline yaml file."""
    with open(pipeline_file, "r") as stream:
        try:
            pipeline_settings = yaml.load(stream, Loader=get_loader())
        except yaml.YAMLError as exc:
            raise yaml.YAMLError(exc)

    return pipeline_settings


def dpypeline():
    """Run the dpypeline."""
    logging.basicConfig(
        format="dpypeline | %(levelname)s | %(asctime)s | %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    banner()

    parser = create_parser()
    args = parser.parse_args()

    import_settings = load_imports_yaml(args.imports_file)
    pipeline_settings = load_pipeline_yaml(args.input_file)

    akita = pipeline_settings["akita"]
    event_consumer = pipeline_settings["event_consumer"]
    pipeline = pipeline_settings["pipeline"]

    # Run the EventConsumer and Akita (must be in this order)
    event_consumer.run()
    akita.run()
