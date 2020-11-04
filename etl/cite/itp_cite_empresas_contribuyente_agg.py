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

        df = pd.read_csv("../../../datasets/20201001/01. Información ITP red CITE  (01-10-2020)/02 CLIENTES ATENDIDOS//TABLA_02_N06.csv")
        
        contribuyente_list = list(df["tipo_contribuyente"].unique())
        contribuyente_map = {k:v for (k,v) in zip(sorted(contribuyente_list), list(range(1, len(contribuyente_list) +1)))}
        df['contribuyente_id'] = df['tipo_contribuyente'].map(contribuyente_map).astype(int)
       
        df = df[['contribuyente_id', 'anio', 'empresas']]

        df['anio'] = df['anio'].astype(int)
        df['empresas'] = df['empresas'].astype(float) 
        
        return df

class CiteContribuyentePipeline(EasyPipeline):
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
            'contribuyente_id':       'UInt8',
            'anio':                   'UInt16',
            'empresas':               'Float32',
         }

        transform_step = TransformStep()  
        load_step = LoadStep(
          'itp_cite_empresas_contribuyente_agg', connector=db_connector, if_exists='drop',
          pk=['contribuyente_id'], dtype=dtypes, nullable_list=['empresas'])

        if params.get("ingest")==True:
            steps = [transform_step, load_step]
        else:
            steps = [transform_step]

        return steps

if __name__ == "__main__":
    cite_contribuyente_pipeline = CiteContribuyentePipeline()
    cite_contribuyente_pipeline.run(
        {
            "output-db": "clickhouse-local",
            "ingest": True
        }
    )