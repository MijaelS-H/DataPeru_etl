import numpy as np
import pandas as pd
import os
from functools import reduce
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import grab_connector

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_excel('../../../datasets/20201001/01. Información ITP red CITE  (01-10-2020)/07 PARTIDAS ARANCELARIAS/TABLA_07_N01.xlsx')
        df = df[df['cadena_productiva'].notna()]
        cadena_productiva_list = list(df["cadena_productiva"].dropna().unique())
        cadena_productiva_map = {k:v for (k,v) in zip(sorted(cadena_productiva_list), list(range(1, len(cadena_productiva_list) +1)))}
        
        df['cadena_productiva_id'] = df['cadena_productiva'].map(cadena_productiva_map)
        df['cadena_productiva_id'] = df['cadena_productiva_id'].fillna(0).astype(int)
        
        df = df[['cadena_productiva', 'cadena_productiva_id']]
    
        df = df.drop_duplicates()
        
        return df

class CitePartidasPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter("output-db", dtype=str),
            Parameter("ingest", dtype=bool)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtypes = {
            'cadena_productiva':                'String',
            'cadena_productiva_id':              'UInt8',
         }

        transform_step = TransformStep()  
        load_step = LoadStep(
          'dim_shared_cadenas_productivas_atendidas', connector=db_connector, if_exists='drop',
          pk=['cadena_productiva_id'], dtype=dtypes, nullable_list=[])

        if params.get("ingest")==True:
            steps = [transform_step, load_step]
        else:
            steps = [transform_step]

        return steps

if __name__ == "__main__":
    pp = CitePartidasPipeline()
    pp.run(
        {
            "output-db": "clickhouse-local",
            "ingest": True
        }
    )