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
from static import MONTHS_DICT

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        
        df = pd.read_csv("../../../datasets/20201001/01. Información ITP red CITE  (01-10-2020)/02 CLIENTES ATENDIDOS//TABLA_02_N02.csv")
      
        empresas_list = list(df["tipo"].unique())
        empresas_map = {k:v for (k,v) in zip(sorted(empresas_list), list(range(1, len(empresas_list) +1)))}

        df = pd.melt(df, id_vars=['anio','tipo'], value_vars=['mes_01', 'mes_02', 'mes_03', 'mes_04',
               'mes_05', 'mes_06', 'mes_07', 'mes_08', 'mes_09', 'mes_10', 'mes_11',
               'mes_12'])
        df = df.rename(columns={'variable':'month_id','anio':'year','value':'empresas','tipo':'empresa_tipo'})

        df['month_id'] = df['month_id'].map(MONTHS_DICT)
        df['time'] = df['year'].astype(str) + df['month_id'].str.zfill(2)
        df['empresa_id'] = df['empresa_tipo'].map(empresas_map)
        df['empresas'] = df.apply(lambda x: int(x.empresas.replace(',', '')) if type(x.empresas) is str else x.empresas, axis=1)
        
        df[['time','empresa_id']] = df[['time','empresa_id']].astype(int)
        df[['empresas']] = df[['empresas']].astype(float)
        df = df[['time','empresa_id','empresas']]
        
        return df

class CiteEmpresasPipeline(EasyPipeline):
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

            'empresa_id':            'UInt8',
            'time':                  'UInt16',
            'empresas':              'Float32',
         }

        transform_step = TransformStep()  
        load_step = LoadStep(
          'itp_cite_empresas_tipo_agg', connector=db_connector, if_exists='drop',
          pk=['empresa_id'], dtype=dtypes, nullable_list=['empresas'])

        if params.get("ingest")==True:
            steps = [transform_step, load_step]
        else:
            steps = [transform_step]

        return steps

if __name__ == "__main__":
    cite_empresas_pipeline = CiteEmpresasPipeline()
    cite_empresas_pipeline.run(
        {
            "output-db": "clickhouse-local",
            "ingest": True
        }
    )