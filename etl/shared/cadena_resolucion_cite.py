import numpy as np
import pandas as pd
import os
from functools import reduce
from unidecode import unidecode
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import grab_connector


class TransformStep(PipelineStep):
    def run_step(self, prev, params):


        df = pd.read_csv('../../../datasets/20201001/01. Información ITP red CITE  (01-10-2020)/01 INFORMACIÓN INSTITUCIONAL/TABLA_01_N05.csv')

        df = df[['cadena_atencion','cadena_pip','cadena_resolucion']]
        
        cadena_resolucion_list = list(df["cadena_resolucion"].unique())
        cadena_resolucion_map = {k:v for (k,v) in zip(sorted(cadena_resolucion_list), list(range(1, len(cadena_resolucion_list) +1)))}
        df['cadena_resolucion_id'] = df["cadena_resolucion"].map(cadena_resolucion_map).astype(int)


        df = df[['cadena_resolucion', 'cadena_resolucion_id']]
   
        return df

class CiteCadenaAtencionPipeline(EasyPipeline):
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

            'cadena_resolucion':               'String',  
            'cadena_resolucion_id':            'UInt8',  

         }

        transform_step = TransformStep()  
        load_step = LoadStep(
          'dim_shared_cite_cadena_resolucion', connector=db_connector, if_exists='drop',
          pk=['cadena_resolucion_id'], dtype=dtypes, nullable_list=[])

        if params.get("ingest")==True:
            steps = [transform_step, load_step]
        else:
            steps = [transform_step]

        return steps

if __name__ == "__main__":
    pp = CiteCadenaAtencionPipeline()
    pp.run(
        {
            "output-db": "clickhouse-local",
            "ingest": True
        }
    )