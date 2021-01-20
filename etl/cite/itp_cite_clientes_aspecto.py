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
from bamboo_lib.helpers import query_to_df
from bamboo_lib.helpers import grab_connector
from etl.consistency import AggregatorStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv(path.join(params["datasets"],"20201001", "01. Información ITP red CITE  (01-10-2020)", "04 PROYECTOS DE INVERSIÓN PÚBLICA", "TABLA_04_N02.csv"))

        df['aspecto'] = df['aspecto'].str.strip()

        aspecto_list = list(df['aspecto'].unique())
        aspecto_map = {k:v for (k,v) in zip(sorted(aspecto_list), list(range(1, len(aspecto_list) + 1)))}
        
        dim_estado_query = 'SELECT estado, estado_id FROM dim_shared_cite_estado'
        dim_estado = query_to_df(self.connector, raw_query=dim_estado_query)
        df = df.merge(dim_estado, on="estado")
        
        dim_cite_query = 'SELECT cite, cite_id FROM dim_shared_cite'
        dim_cite = query_to_df(self.connector, raw_query=dim_cite_query)
        
        df = df.merge(dim_cite, on="cite")
        
        df['aspecto_id'] = df['aspecto'].map(aspecto_map).astype(int)

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

        transform_step = TransformStep(connector=db_connector)
        agg_step = AggregatorStep('itp_cite_clientes_aspecto', measures=['cantidad_cite'])
        load_step = LoadStep('itp_cite_clientes_aspecto', connector=db_connector, if_exists='drop', pk=['cite_id'], dtype=dtypes)

        return [transform_step, agg_step, load_step]


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
