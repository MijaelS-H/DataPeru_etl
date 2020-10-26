
import os
import glob
import shutil
import pandas as pd
from helpers import get_dimension, process_exceptions
from static import FOLDER, DATA_FOLDER, DIMENSIONS, BASE

# delete dims folder
shutil.rmtree(FOLDER, ignore_errors=True)

os.makedirs('{}'.format(FOLDER), exist_ok=True)

for PREFIX in ['GN', 'GR', 'GL']:

    data = glob.glob('{}/G_{}_*.csv'.format(DATA_FOLDER, PREFIX))

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
            result.to_csv('{}/G_{}_{}'.format(FOLDER, PREFIX, file_name), index=False)

    for column_exception in ['pliego', 'ejecutora']:
        df_ = df.copy()
        try:
            if (PREFIX == 'GL') & (column_exception == 'pliego'):
                pass
            else:
                df_.drop_duplicates(subset=[column_exception], inplace=True)
                temp = pd.read_csv('{}/dim_{}.csv'.format(FOLDER, column_exception))
                df_ = df_.append(temp, sort=False)
                df_.drop_duplicates(subset=[column_exception], inplace=True)
                df_.to_csv('{}/dim_{}.csv'.format(FOLDER, column_exception), index=False)
        except FileNotFoundError:
            df_.to_csv('{}/dim_{}.csv'.format(FOLDER, column_exception), index=False)

# exceptions
process_exceptions('pliego', ['GN', 'GR'], folder=FOLDER)
process_exceptions('ejecutora', ['GN', 'GR', 'GL'], folder=FOLDER)


# dim gastos 2
df = pd.DataFrame()
for PREFIX in ['GN', 'GR', 'GL']:
    data = glob.glob('{}/gasto_gobierno_*_{}.csv'.format(DATA_FOLDER, PREFIX))
    for file in data:
        print(file)
        temp = pd.read_csv(file)
        temp.columns = temp.columns.str.lower()
        temp = temp[BASE[PREFIX]].copy()
        df = df.append(temp, sort=False)

for col in ['pliego', 'programa_ppto', 
            'producto_proyecto', 'funcion', 'division_funcional']:
    temp = df.copy()
    temp.dropna(subset=[col], inplace=True)
    temp[col] = temp[col].str.strip()
    result = get_dimension(temp, col)
    result.to_csv('{}/dim_gasto_gobierno_{}.csv'.format(FOLDER, col), index=False)

col = 'ejecutora'
df[col] = df[col].str.strip()
df['name'] = df[col]
temp = df[['name', col]].copy()
temp.drop_duplicates(inplace=True)
temp['name'] = temp['name'].str.split('. ', n=1, expand=True)[1].str.strip().str.title() \
    .str.replace('ã', 'ñ').str.replace('ã', 'ó').str.replace('ã', 'á').str.title()
temp['id'] = range(1, len(temp['name']) + 1)
temp.to_csv('{}/dim_gasto_gobierno_{}.csv'.format(FOLDER, col), index=False)