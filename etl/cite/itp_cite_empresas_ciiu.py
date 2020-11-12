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
        df = pd.read_csv(path.join(params["datasets"],"20201001", "01. Información ITP red CITE  (01-10-2020)", "02 CLIENTES ATENDIDOS", "TABLA_02_N03.csv"))

        cite_list = list(df["cite"].unique())
        cite_map = {k:v for (k,v) in zip(sorted(cite_list), list(range(1, len(cite_list) +1)))}

        df['cite_id'] = df['cite'].map(cite_map)

        df = df[['cite_id','cod_ciiu','anio','empresas']]

        df = df.rename(columns={'cod_ciiu' :'class_id'})

        df['cite_id'] = df['cite_id'].astype(int)
        df['anio'] = df['anio'].astype(int)
        df['empresas'] = df['empresas'].astype(float)
        df['class_id'] = df['class_id'].str[:-1].replace({"No determinado" : "0000"}).astype(str)

        return df

class CiteEmpresas2Pipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtypes = {
            'cite_id':               'UInt8',
            'class_id':              'String',
            'anio':                  'UInt16',
            'empresas':              'Float32'
         }

        transform_step = TransformStep()
        agg_step = AggregatorStep('itp_cite_empresas_ciiu', measures=['empresas'])
        load_step = LoadStep('itp_cite_empresas_ciiu', connector=db_connector, if_exists='drop', pk=['cite_id'], dtype=dtypes, nullable_list=['empresas'])

        return [transform_step, agg_step, load_step]


def run_pipeline(params: dict):
    pp = CiteEmpresas2Pipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
