import argparse

parser = argparse.ArgumentParser(
    description="Extraction-transform-load Pipeline",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)

parser.add_argument(
    "-i",
    "--input-file",
    dest="input_file",
    default="input.json",
    help="Filepath to JSON input file",
)

args = parser.parse_args()
