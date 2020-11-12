
import glob
import os
import shutil

import pandas as pd

from .helpers import get_dimension, process_exceptions
from .static import BASE, DATA_FOLDER, DIMENSIONS, FOLDER

# delete dims folder
shutil.rmtree(FOLDER, ignore_errors=True)

os.makedirs(os.path.join('etl', 'presupuesto', '{}'.format(FOLDER)), exist_ok=True)

def run_pipeline():

    for PREFIX in ['GN', 'GR', 'GL']:

        data = glob.glob(os.path.join('{}'.format(DATA_FOLDER), 'G_{}_*.csv'.format(PREFIX)))

        base = BASE[PREFIX]

        temp = pd.DataFrame()

        for file in data:
            print('current file:', file, 'current level:', PREFIX)
            df = pd.read_csv(file, encoding='latin-1')
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

    run_pipeline()
