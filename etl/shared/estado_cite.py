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

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_csv(path.join(params["datasets"], "01_Informacion_ITP_red_CITE", "04_PROYECTOS_DE_INVERSION_PUBLICA", "TABLA_04_N02.csv"), encoding="latin1")

        estado_list = list(df['estado'].dropna().unique())
        estado_map = {k:v for (k,v) in zip(sorted(estado_list), list(range(1, len(estado_list) +1)))}

        df['estado_id'] = df['estado'].map(estado_map)

        df = df[['estado','estado_id']]

        df = df.drop_duplicates()

        return df

class CiteAspectoPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtypes = {
            'estado':                 'String',
            'estado_id':              'UInt16',
        }

        transform_step = TransformStep()  
        load_step = LoadStep('dim_shared_cite_estado', connector=db_connector, if_exists='drop', pk=['estado_id'],
                            dtype=dtypes)

        return [transform_step, load_step]

def run_pipeline(params: dict):
    pp = CiteAspectoPipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
