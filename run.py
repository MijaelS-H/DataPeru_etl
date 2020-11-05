#!/usr/bin/env python3

import sys
from importlib import import_module
from pathlib import Path

from bamboo_cli.main import get_pipeline_class

from etl import run_pipeline

if __name__ == "__main__":
    datasets = Path(sys.argv[1] if len(sys.argv) > 1 else "null").resolve()

    if not datasets.exists():
        raise IOError("Path %s does not exist in the filesystem." % datasets)
    if not datasets.is_dir():
        raise IOError("Path %s is not a directory." % datasets)

    run_pipeline({
        "connector": Path("etl/conns.yaml").resolve(),
        "datasets": datasets
    })
