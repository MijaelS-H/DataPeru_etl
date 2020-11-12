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

MONTHS_DICT = {'mes_01' :'1', 'mes_02' :'2', 'mes_03' :'3', 'mes_04' :'4','mes_05' :'5', 'mes_06' :'6', 'mes_07' :'7', 'mes_08' :'8', 'mes_09' :'9', 'mes_10' :'10', 'mes_11' :'11','mes_12':'12'}

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv(path.join(params["datasets"],"20201001", "01. Información ITP red CITE  (01-10-2020)", "02 CLIENTES ATENDIDOS", "TABLA_02_N01.csv"))

        df = df.drop(columns=['fuente','fecha'])
        df = pd.melt(df, id_vars=['cite','anio','tipo'], value_vars=['mes_01', 'mes_02', 'mes_03', 'mes_04',
               'mes_05', 'mes_06', 'mes_07', 'mes_08', 'mes_09', 'mes_10', 'mes_11',
               'mes_12'])
        df = df.rename(columns={'variable':'month_id','anio':'year','value':'empresas','tipo':'empresa_name'})

        df['month_id'] = df['month_id'].map(MONTHS_DICT)
        df['time'] = df['year'].astype(str) + df['month_id'].str.zfill(2)

        empresas_list = list(df["empresa_name"].unique())
        empresas_map = {k:v for (k,v) in zip(sorted(empresas_list), list(range(1, len(empresas_list) +1)))}
        df['empresa_id'] = df['empresa_name'].map(empresas_map)
        
        cite_list = list(df["cite"].unique())
        cite_map = {k:v for (k,v) in zip(sorted(cite_list), list(range(1, len(cite_list) +1)))}
        df['cite_id'] = df['cite'].map(cite_map)

        df[['cite_id','time','empresa_id']] = df[['cite_id','time','empresa_id']].astype(int)
        df[['empresas']] = df[['empresas']].astype(float)

        df = df[['cite_id', 'empresa_id', 'time', 'empresas']]

        return df

class CiteEmpresasPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtypes = {
            'cite_id':               'UInt8',
            'empresa_id':            'UInt8',
            'time':                  'UInt32',
            'empresas':              'Float32',
        }

        transform_step = TransformStep()
        agg_step = AggregatorStep('itp_cite_empresas_tipo', measures=['empresas'])
        load_step = LoadStep('itp_cite_empresas_tipo', connector=db_connector, if_exists='drop', pk=['cite_id'], dtype=dtypes, nullable_list=['empresas'])

        return [transform_step, agg_step, load_step]

def run_pipeline(params: dict):
    pp = CiteEmpresasPipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
