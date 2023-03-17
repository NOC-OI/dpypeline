"""Argument parser module."""
import argparse

from ..__init__ import __version__


def create_parser():
    parser = argparse.ArgumentParser(
        description=f"dpypeline {__version__} command line interface",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "-i",
        "--input",
        dest="input_file",
        default="pipeline.yaml",
        help="filepath to the pipeline YAML file",
    )

    parser.add_argument(
        "-m",
        "--imports",
        dest="imports_file",
        default="imports.yaml",
        help="filepath to the imports YAML file",
    )

    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version="%(prog)s {version}".format(version=__version__),
    )

    return parser
