"""dpypeline command line interface."""
import logging

import yaml

from .argument_parser import __version__, create_parser
from .yaml_loader import get_loader

logger = logging.getLogger(__name__)


def banner():
    """Log the dpypeline banner."""
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

    pipeline_settings = load_pipeline_yaml(args.input_file)

    akita = pipeline_settings["akita"]
    event_consumer = pipeline_settings["event_consumer"]
    pipeline_settings["pipeline"]

    # Run the EventConsumer and Akita (must be in this order)
    event_consumer.run()
    akita.run()
