import nltk
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from etl.helpers import format_text
from dinamica_pecuaria_static import DTYPES, PRIMARY_KEYS, RENAME_COLUMNS, REPLACE_VALUES

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        # Reads 02. DINAMICA PECUARIA file
        path = '../../../datasets/20201020/07. Socios Estratégicos - Ministerio de Agricultura (20-10-2020)/02. DINAMICA PECUARIA.xlsx'
        df = pd.read_excel(path, dtype='str')

        # Rename columns to unique name
        df.rename(columns=RENAME_COLUMNS, inplace=True)

        # Creates and standarizes columns
        df['department_id'] = df['department_id'].str[0:2]
        df['month_id'] = df['year'] + df['month']

        # Replace values in each column
        for item in REPLACE_VALUES:
            df[item].replace(REPLACE_VALUES[item], inplace=True)

        # Creates product dimension table
        dim = df[['producto_name']].copy().drop_duplicates()
        dim["producto_id"] = range(1,len(dim['producto_name'].unique())+1)

        # Merges fact table and dimension table
        df = pd.merge(df, dim, on='producto_name')

        df = df[['producto_id', 'department_id', 'month_id', 'produccion', 'produccion_unidad']].copy()

        df['produccion'] = df['produccion'].astype(float)

        if params.get('level') == 'dimension_table':
            text_cols= ['producto_name']

            nltk.download('stopwords')
            stopwords_es = nltk.corpus.stopwords.words('spanish')
            dim = format_text(dim, text_cols, stopwords=stopwords_es)

            return dim
        
        return df

class MINAGRIPecuariaPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        transform_step = TransformStep()
        load_step = LoadStep(params.get('table_name'), db_connector, if_exists='drop', 
                             pk=PRIMARY_KEYS[params.get('level')], dtype=DTYPES[params.get('level')],
                             nullable_list=[])

        return [transform_step, load_step]

if __name__ == '__main__':
   pp = MINAGRIPecuariaPipeline()
   
   for k, v in {
        'dimension_table':  'dim_shared_dinamica_pecuaria',
        'fact_table': 'minagri_dinamica_pecuaria'
            }.items():
        pp.run({
            'level': k,
            'table_name': v
        })
