"""dpypeline command line interface."""
import logging
import sys
import time

from .argument_parser import __version__, create_parser
from .yaml_loader import get_loader, load_yaml

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


def start_banner():
    """Log the dpypeline start banner."""
    banner()
    logger.info(f"version: {__version__}", extra={"simple": True})


def exit_banner():
    """Log the dpypeline exit banner."""
    banner()
    logger.info("dpypeline has terminated succesfully! :)", extra={"simple": True})


def dpypeline():
    """Run the dpypeline."""
    logging.basicConfig(
        format="dpypeline | %(levelname)s | %(asctime)s | %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )
    start_banner()

    parser = create_parser()
    args = parser.parse_args()

    settings = load_yaml(args.input_file, loader=get_loader())

    akita = settings["akita"]
    event_consumer = settings["event_consumer"]
    settings["pipeline"]

    akita.run()
    event_consumer.run()

    if "dask_client" in settings:
        # Close the Dask client if it exists.
        logger.info("Closing Dask client...")
        settings["dask_client"].shutdown()
        settings["dask_client"].close()
        logger.info("Dask client has been closed successfully.")
        time.sleep(1)

    exit_banner()
