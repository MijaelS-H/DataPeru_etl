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
        df = pd.read_csv(path.join(params["datasets"],"20201001", "01. Información ITP red CITE  (01-10-2020)", "04 PROYECTOS DE INVERSIÓN PÚBLICA", "TABLA_04_N03.csv"))

        df = df[df['componente'] != "Total"]

        cite_list = list(df["cite"].unique())
        cite_map = {k:v for (k,v) in zip(sorted(cite_list), list(range(1, len(cite_list) +1)))}

        componente_list = list(df["componente"].unique())
        componente_map = {k:v for (k,v) in zip(sorted(componente_list), list(range(1, len(componente_list) +1)))}

        df['cite_id'] = df['cite'].map(cite_map).astype(int)
        df['componente_id'] = df['componente'].map(componente_map).astype(int)
        df['inversion'] = df['inversion'].replace(',','', regex=True).astype(float)
        df['ejecucion'] = df['ejecucion'].replace(',','', regex=True).astype(float)

        df = df[['cite_id', 'componente_id', 'inversion', 'ejecucion']]

        return df

class CiteEjecucionPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtypes = {
            'cite_id':                 'UInt8',
            'componente_id':           'UInt8',
            'inversion':               'Float32',
            'ejecucion':               'Float32'
        }

        transform_step = TransformStep()
        agg_step = AggregatorStep('itp_cite_inversion', measures=['inversion', 'ejecucion'])
        load_step = LoadStep('itp_cite_inversion', connector=db_connector, if_exists='drop', pk=['cite_id'], dtype=dtypes, nullable_list=['inversion','ejecucion'])

        return [transform_step, agg_step, load_step]

def run_pipeline(params: dict):
    pp = CiteEjecucionPipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
