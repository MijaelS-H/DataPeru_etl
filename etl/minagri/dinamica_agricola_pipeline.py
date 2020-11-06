from os import path

import nltk
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from etl.consistency import AggregatorStep
from etl.helpers import format_text

from .dinamica_agricola_static import (DTYPES, PRIMARY_KEYS, RENAME_COLUMNS,
                                       REPLACE_VALUES)


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        # Reads 01. DINAMICA AGRICOLA file
        df = pd.read_excel(
            path.join(params["datasets"], "20201020", "07. Socios Estratégicos - Ministerio de Agricultura (20-10-2020)", "01. DINAMICA AGRICOLA.xlsx"),
            dtype='str'
        )

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
    def parameter_list():
        return [
            Parameter(name='level', dtype=str),
            Parameter(name='table_name', dtype=str)
        ]

    @staticmethod
    def steps(params):
        level = params['level']
        table_name = params['table_name']
        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))

        transform_step = TransformStep()

        agg_step = AggregatorStep(table_name, measures=[])

        load_step = LoadStep(table_name, db_connector, if_exists='drop',
                             pk=PRIMARY_KEYS[level], dtype=DTYPES[level],
                             nullable_list=[])

        return [transform_step, agg_step, load_step]


def run_pipeline(params: dict):
    pp = MINAGRIAgricolaPipeline()
    levels = {
        "dimension_table": "dim_shared_dinamica_agricola",
        "fact_table": "minagri_dinamica_agricola"
    }

    for k, v in levels.items():
        pp_params = {"level": k, "table_name": v}
        pp_params.update(params)
        pp.run(pp_params)


if __name__ == "__main__":
    import sys

    run_pipeline({
        "connector": "../conns.yaml",
        "datasets": sys.argv[1]
    })
