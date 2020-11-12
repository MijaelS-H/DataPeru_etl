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

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_excel(path.join(params["datasets"],"20201001", "01. Información ITP red CITE  (01-10-2020)", "07 PARTIDAS ARANCELARIAS", "TABLA_07_N01.xlsx"))

        df = df[df['cadena_productiva'].notna()]

        cadena_dim = dict(zip(df['cadena_productiva'].dropna().unique(), range(1, len(df['cadena_productiva'].unique()) + 1 )))
        cadena_dim.update({'No Aplica' : 0})

        df = pd.DataFrame.from_dict(cadena_dim, orient='index').reset_index()
        df.columns = ['cadena_productiva', 'cadena_productiva_id']
        
        return df

class CitePartidasPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtypes = {
            'cadena_productiva':                'String',
            'cadena_productiva_id':              'UInt8',
        }

        transform_step = TransformStep()  
        load_step = LoadStep('dim_shared_cadenas_productivas_atendidas', connector=db_connector, if_exists='drop',
                            pk=['cadena_productiva_id'], dtype=dtypes)

        return [transform_step, load_step]

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
