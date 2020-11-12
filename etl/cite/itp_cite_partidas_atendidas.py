import numpy as np
import pandas as pd
import os
from os import path
from functools import reduce
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import grab_connector
from etl.consistency import AggregatorStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_excel(path.join(params["datasets"],"20201001", "01. Información ITP red CITE  (01-10-2020)", "07 PARTIDAS ARANCELARIAS", "TABLA_07_N01.xlsx"))

        df = df[df['cadena_productiva'].notna()]

        for column in ['cadena_atencion','cadena_pip','cadena_resolucion']:
            df[column] = df[column].replace(1, df['cadena_productiva'])

        cite_list = list(df["cite"].unique())
        cite_map = {k:v for (k,v) in zip(sorted(cite_list), list(range(1, len(cite_list) +1)))}
        df['cite_id'] = df['cite'].map(cite_map)
        df[['cite_id']] = df[['cite_id']].fillna(0).astype(int)
        df['hs6_id'] = df['partida_arancelaria'].astype(str).str[:-6].str.zfill(6)

        cadena_dim = dict(zip(df['cadena_productiva'].dropna().unique(), range(1, len(df['cadena_productiva'].unique()) + 1 )))
        cadena_dim.update({'No Aplica' : 0})

        df['cadena_atencion_id'] = df['cadena_atencion'].map(cadena_dim)
        df['cadena_pip_id'] = df['cadena_pip'].map(cadena_dim)
        df['cadena_resolucion_id'] = df['cadena_resolucion'].map(cadena_dim)

        df[['cadena_atencion_id', 'cadena_pip_id', 'cadena_resolucion_id']] = df[['cadena_atencion_id', 'cadena_pip_id', 'cadena_resolucion_id']].fillna(0).astype(int)

        df.rename(columns ={'descripcion_partida ' : 'hs10_name', 'partida_arancelaria' : 'hs10_id'}, inplace=True)
        df['hs10_name'] = df['hs10_name'].str.title()
        df['hs10_id'] = df['hs10_id'].fillna(0)
        df['hs10_id'] = df['hs10_id'].astype(int).astype(str).str.zfill(10)

        df['cantidad_cite'] = 1

        df = df[['cite_id', 'hs10_id', 'cadena_atencion_id', 'cadena_pip_id', 'cadena_resolucion_id', 'cantidad_cite']]

        return df

class CitePartidasPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtypes = {
            'cite_id':                        'UInt8',
            'hs10_id':                        'String',
            'cadena_atencion_id':             'UInt8',
            'cadena_pip_id':                  'UInt8',
            'cadena_resolucion_id':           'UInt8',
            'cantidad_cite':                  'UInt8'
         }

        transform_step = TransformStep()
        agg_step = AggregatorStep('itp_cite_partidas_atendidas', measures=['cantidad_cite'])
        load_step = LoadStep('itp_cite_partidas_atendidas', connector=db_connector, if_exists='drop', pk=['cite_id'], dtype=dtypes, nullable_list=['hs10_id'])

        return [transform_step, agg_step, load_step]

def run_pipeline(params: dict):
    pp = CitePartidasPipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })