import numpy as np
import pandas as pd
import os
from unidecode import unidecode
from functools import reduce
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep



class TransformStep(PipelineStep):
    def run_step(self, prev, params):


        df = pd.read_excel('../../../datasets/20201001/01. Información ITP red CITE  (01-10-2020)/07 PARTIDAS ARANCELARIAS/TABLA_08_N01 (18-10-2020).xlsx')

        df = df[df['cite'].notna()]
        df['sector'] = df['sector'].str.capitalize()
        df['cadena_productiva'] = df['cadena_productiva'].str.strip()


        cadena_productiva_list = list(df["cadena_productiva"].unique())
        cadena_productiva_map = {k:v for (k,v) in zip(sorted(cadena_productiva_list), list(range(1, len(cadena_productiva_list) +1)))}
        df['cad_prod_id'] = df['cadena_productiva'].map(cadena_productiva_map)

        df = df[['cad_prod_id','cadena_productiva']]
        df = df.drop_duplicates()

 
        return df
    

class CiteCadenaProductivaPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter("output-db", dtype=str),
            Parameter("ingest", dtype=bool)
        ]
    
    
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'cad_prod_id':                  'UInt8',
            'cadena_productiva':            'String',
        }

        transform_step = TransformStep()

        load_step = LoadStep(
            "dim_shared_cite_cad_prod", db_connector, if_exists="drop", pk=["cad_prod_id"], dtype=dtype)

        if params.get("ingest")==True:
            steps = [transform_step, load_step]
        else:
            steps = [transform_step]

        return steps

if __name__ == "__main__":

    pp = CiteCadenaProductivaPipeline()
    pp.run(
        {
            "output-db": "clickhouse-local",
            "ingest": True
        }
    )