"""dpypeline command line interface."""
import logging

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
    logger.info(f"version: {__version__}\n", extra={"simple": True})


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

    pipeline_settings = load_yaml(args.input_file, loader=get_loader())

    akita = pipeline_settings["akita"]
    event_consumer = pipeline_settings["event_consumer"]
    pipeline_settings["pipeline"]

    # Run the EventConsumer and Akita (must be in this order)
    event_consumer.run()
    akita.run()
