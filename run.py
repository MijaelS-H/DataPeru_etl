#!/usr/bin/env python3

"""Executes all pipelines for the ETL process.
"""

from argparse import ArgumentParser
from os import path
from pathlib import Path

from etl import run_pipeline

__dirname = path.realpath(path.dirname(__file__))

argparser = ArgumentParser(description="Execute the pipeline.")
argparser.add_argument(
    "datasets",
    help="The path to the datasets folder.",
    metavar="path/to/dataset_folder",
    type=Path,
)
argparser.add_argument(
    "-c",
    "--connector",
    default=path.join(__dirname, "etl/conns.yaml"),
    help="The path to the database connection info for bamboo-lib's Connector class.",
    type=Path,
)
argparser.add_argument(
    "-r",
    "--reports",
    default="./reports/",
    help="The path to the folder to store the ingestion reports.",
)

if __name__ == "__main__":
    args = argparser.parse_args()

    if not args.datasets.exists():
        raise IOError("Path %s does not exist in the filesystem." % args.datasets)
    if not args.datasets.is_dir():
        raise IOError("Path %s is not a directory." % args.datasets)

    params = {
        "connector": args.connector,
        "datasets": args.datasets,
        "reports": args.reports,
    }
    run_pipeline(params)
