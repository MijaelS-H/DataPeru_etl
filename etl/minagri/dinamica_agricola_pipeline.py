import nltk
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from etl.helpers import format_text
from dinamica_agricola_static import DTYPES, PRIMARY_KEYS, RENAME_COLUMNS, REPLACE_VALUES


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        # Reads 01. DINAMICA AGRICOLA file
        path = '../../../datasets/20201020/07. Socios Estratégicos - Ministerio de Agricultura (20-10-2020)/01. DINAMICA AGRICOLA.xlsx'
        df = pd.read_excel(path, dtype='str')

        # Rename columns to unique name
        df.rename(columns=RENAME_COLUMNS, inplace=True)

        if params.get('level') == 'dimension_table':
            df = df[['cultivo_id', 'cultivo_name']].copy()
            df = df.drop_duplicates(subset='cultivo_id')

            text_cols= ['cultivo_name']

            nltk.download('stopwords')
            stopwords_es = nltk.corpus.stopwords.words('spanish')
            df = format_text(df, text_cols, stopwords=stopwords_es)

            return df

        # Creates and standarizes columns
        df['month'] = df['month'].str.zfill(2)
        df['month_id'] = df['year'] + df['month']
        df['district_id'] = df['district_id'].str.zfill(6)

        # Transforms measures to float type
        df['superficie_sembrada'] = df['superficie_sembrada'].str.replace(',', '.').astype(float)
        df['superficie_cosechada'] = df['superficie_cosechada'].str.replace(',', '.').astype(float)
        df['produccion'] = df['produccion'].str.replace(',', '.').astype(float)
        df['rendimiento'] = df['rendimiento'].str.replace(',', '.').astype(float)
        df['precio'] = df['precio'].str.replace(',', '.').astype(float)

        # Replace values in each column
        for item in REPLACE_VALUES:
            df[item].replace(REPLACE_VALUES[item], inplace=True)

        # Select columns to ingest
        df = df[['cultivo_id', 'district_id', 'month_id', 'tipo', 'superficie_sembrada', 'superficie_sembrada_unidad', 'superficie_cosechada', 'superficie_cosechada_unidad', 'produccion', 'produccion_unidad', 'rendimiento', 'rendimiento_unidad', 'precio', 'precio_unidad']].copy()

        # Return DataFrame
        return df

class MINAGRIAgricolaPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        transform_step = TransformStep()
        load_step = LoadStep(params.get('table_name'), db_connector, if_exists='drop', 
                             pk=PRIMARY_KEYS[params.get('level')], dtype=DTYPES[params.get('level')],
                             nullable_list=[])

        return [transform_step, load_step]

if __name__ == '__main__':
   pp = MINAGRIAgricolaPipeline()
   
   for k, v in {
        'dimension_table':  'dim_shared_dinamica_agricola',
        'fact_table': 'minagri_dinamica_agricola'
            }.items():
        pp.run({
            'level': k,
            'table_name': v
        })
   
