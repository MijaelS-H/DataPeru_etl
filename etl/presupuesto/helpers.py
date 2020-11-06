import pandas as pd

from .static import FOLDER


def get_dimension(df, target_column):
    dimension = df[[target_column]].drop_duplicates().copy()
    temp = dimension[target_column].str.split('. ', n=1, expand=True)
    dimension['id'] = temp[0].str.strip().astype(int)
    dimension['name'] = temp[1].str.strip().str.title() \
        .str.replace('ã', 'ñ').str.replace('ã', 'ó').str.replace('ã', 'á').str.capitalize()

    return dimension

def return_dimension(prefix, target_column):
    df = pd.read_csv('{}/G_{}_dim_{}.csv'.format(FOLDER, prefix, target_column))
    return df

def process_exceptions(target_column, levels, folder):
    df = pd.read_csv('{}/dim_{}.csv'.format(folder, target_column))
    df['name'] = df[target_column]
    temp = df[['name', target_column]].copy()
    temp.drop_duplicates(inplace=True)
    temp['name'] = temp['name'].str.split('. ', n=1, expand=True)[1].str.strip().str.title() \
        .str.replace('ã', 'ñ').str.replace('ã', 'ó').str.replace('ã', 'á').str.title()
    temp['id'] = range(1, len(temp['name']) + 1)
    for prefix in levels:
        temp.to_csv('{}/G_{}_dim_{}.csv'.format(folder, prefix, target_column), index=False)
    print('Success!: {}'.format(target_column))
