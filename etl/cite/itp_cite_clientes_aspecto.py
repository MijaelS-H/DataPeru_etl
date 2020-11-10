import numpy as np
import pandas as pd
from os import path
import os
from functools import reduce
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from etl.consistency import AggregatorStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv(path.join(params["datasets"],"20201001", "01. Información ITP red CITE  (01-10-2020)", "04 PROYECTOS DE INVERSIÓN PÚBLICA", "TABLA_04_N02.csv"))

        df['aspecto'] = df['aspecto'].str.strip()

        aspecto_list = list(df['aspecto'].unique())
        aspecto_map = {k:v for (k,v) in zip(sorted(aspecto_list), list(range(1, len(aspecto_list) + 1)))}

        cite_list = list(df['cite'].unique())
        cite_map = {k:v for (k,v) in zip(sorted(cite_list), list(range(1, len(cite_list) +1)))}

        estado_list = list(df['estado'].dropna().unique())
        estado_map = {k:v for (k,v) in zip(sorted(estado_list), list(range(1, len(estado_list) +1)))}

        df['cite_id'] = df['cite'].map(cite_map).astype(int)
        df['aspecto_id'] = df['aspecto'].map(aspecto_map).astype(int)
        df['estado_id'] = df['estado'].map(estado_map)

        df['cantidad_cite'] = 1

        df = df[['cite_id','aspecto_id','estado_id','cantidad_cite']]

        return df

class CiteAspectoPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open(params['connector']))

        dtypes = {
            'cite_id':                'UInt8',
            'aspecto_id':             'UInt8',
            'estado_id':              'UInt16',
            'cantidad_cite':          'UInt8'
        }

        transform_step = TransformStep()
        agg_step = AggregatorStep('itp_cite_clientes_aspecto', measures=['cantidad_cite'])
        load_step = LoadStep('itp_cite_clientes_aspecto', connector=db_connector, if_exists='drop', pk=['cite_id'], dtype=dtypes)

        return [transform_step, agg_step, load_step]


def run_pipeline(params: dict):
    pp = CiteAspectoPipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys

    run_pipeline({
        "connector": params["connector"],
        "datasets": sys.argv[1]
    })
