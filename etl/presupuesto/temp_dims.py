
import glob
import os
import shutil
from pathlib import Path

import pandas as pd
from bamboo_lib.logger import logger

from .helpers import get_dimension, process_exceptions
from .static import BASE, DIMENSIONS, FOLDER


def run_pipeline(params: dict):
    # delete dims folder
    shutil.rmtree(FOLDER, ignore_errors=True)
    os.makedirs(os.path.join('etl', 'presupuesto', '{}'.format(FOLDER)), exist_ok=True)

    download_folder = Path(params["datasets"]).joinpath("download")
    download_folder.mkdir(exist_ok=True)

    for PREFIX in ['GN', 'GR', 'GL']:
        data = glob.glob(os.path.join(download_folder, 'G_{}_*.csv'.format(PREFIX)))
        base = BASE[PREFIX]

        temp = pd.DataFrame()
        for filename in data:
            logger.debug('current file: %s / current level: %s', filename, PREFIX)
            with open(filename, 'r', encoding='latin-1') as f:
                df = pd.read_csv(f)
            df.columns = df.columns.str.lower()
            df = df[base].copy()
            temp = temp.append(df)

        df = temp.copy()
        temp = []

        for file_name, dimension in DIMENSIONS[PREFIX].items():
            if (dimension != 'ejecutora') | (dimension != 'pliego'):
                result = get_dimension(df, dimension)
                result.to_csv(os.path.join('etl', 'presupuesto', '{}'.format(FOLDER), 'G_{}_{}'.format(PREFIX, file_name)), index=False)

        for column_exception in ['pliego', 'ejecutora', 'producto_proyecto', 'programa_ppto']:
            df_ = df.copy()
            try:
                if (PREFIX == 'GL') & (column_exception == 'pliego'):
                    pass
                else:
                    df_.drop_duplicates(subset=[column_exception], inplace=True)
                    temp = pd.read_csv(os.path.join('etl', 'presupuesto', '{}'.format(FOLDER), 'dim_{}.csv'.format(column_exception)))
                    df_ = df_.append(temp, sort=False)
                    df_.drop_duplicates(subset=[column_exception], inplace=True)
                    df_.to_csv(os.path.join('etl', 'presupuesto', '{}'.format(FOLDER), 'dim_{}.csv'.format(column_exception)), index=False)
            except FileNotFoundError:
                df_.to_csv(os.path.join('etl', 'presupuesto', '{}'.format(FOLDER), 'dim_{}.csv'.format(column_exception)), index=False)

    # exceptions
    process_exceptions('pliego', ['GN', 'GR'], folder=os.path.join('etl', 'presupuesto', '{}'.format(FOLDER)))
    process_exceptions('ejecutora', ['GN', 'GR', 'GL'], folder=os.path.join('etl', 'presupuesto', '{}'.format(FOLDER)))
    process_exceptions('producto_proyecto', ['GN', 'GR', 'GL'], folder=os.path.join('etl', 'presupuesto', '{}'.format(FOLDER)))
    process_exceptions('programa_ppto', ['GN', 'GR', 'GL'], folder=os.path.join('etl', 'presupuesto', '{}'.format(FOLDER)))


if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"), 
        "datasets": sys.argv[1]
    })
