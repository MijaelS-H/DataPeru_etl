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

        cite_list = list(df["cite"].unique())
        cite_map = {k:v for (k,v) in zip(sorted(cite_list), list(range(1, len(cite_list) +1)))}
        df['cite_id'] = df['cite'].map(cite_map)
        
        cadena_productiva_list = list(df["cadena_productiva"].dropna().unique())
        cadena_productiva_map = {k:v for (k,v) in zip(sorted(cadena_productiva_list), list(range(1, len(cadena_productiva_list) +1)))}
        df['cadena_productiva_id'] = df['cadena_productiva'].map(cadena_productiva_map)
        
        df['hs6_id'] = df['partida_arancelaria'].astype(str).str[:-6].str.zfill(6)
        
        df[['cite_id','cadena_productiva_id']] = df[['cite_id','cadena_productiva_id']].fillna(0).astype(int)
        df[['cadena_atencion','cadena_pip','cadena_resolucion']] = df[['cadena_atencion','cadena_pip','cadena_resolucion']].fillna(0).astype(int)
        
        df = df[['cite_id','hs6_id','cadena_productiva_id','cadena_atencion','cadena_pip','cadena_resolucion']]
        
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
            'cite_id':                'UInt8',
            'hs6_id':                 'String',
            'cadena_productiva_id':   'UInt8',
            'cadena_atencion':        'UInt8',
            'cadena_pip':             'UInt8',
            'cadena_resolucion':      'UInt8',
 
         }

        transform_step = TransformStep()  
        load_step = LoadStep(
          'itp_cite_partidas_atendidas', connector=db_connector, if_exists='drop',
          pk=['cite_id'], dtype=dtypes, nullable_list=[])

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