from os import path

import nltk
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from etl.consistency import AggregatorStep
from etl.helpers import format_text

from .dinamica_pecuaria_static import (DTYPES, PRIMARY_KEYS, RENAME_COLUMNS,
                                       REPLACE_VALUES)


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        # Reads 02. DINAMICA PECUARIA file
        df = pd.read_excel(
            path.join(params["datasets"], "20201020", "07. Socios Estratégicos - Ministerio de Agricultura (20-10-2020)", "02. DINAMICA PECUARIA.xlsx"),
            dtype='str'
        )

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
    def parameter_list():
        return [
            Parameter(name='level', dtype=str),
            Parameter(name='table_name', dtype=str)
        ]

    @staticmethod
    def steps(params):
        level = params["level"]
        table_name = params["table_name"]
        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))

        transform_step = TransformStep()
        agg_step = AggregatorStep(table_name, measures=["produccion"])
        load_step = LoadStep(table_name, db_connector, if_exists='drop', 
                             pk=PRIMARY_KEYS[level], dtype=DTYPES[level],
                             nullable_list=[])

        if level == "fact_table":

            return [transform_step, agg_step, load_step]

        else:

            return [transform_step, load_step]

def run_pipeline(params: dict):
    pp = MINAGRIPecuariaPipeline()
    levels = {
        "dimension_table": "dim_shared_dinamica_pecuaria",
        "fact_table": "minagri_dinamica_pecuaria"
    }

    for k, v in levels.items():
        pp_params = {"level": k, "table_name": v}
        pp_params.update(params)
        pp.run(pp_params)


if __name__ == "__main__":
    import sys
    from os import path
    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
