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

CARPETAS_DICT = {
    1: "01 INFORMACIÓN INSTITUCIONAL",
    2: "02 CLIENTES ATENDIDOS",
    3: "03 SERVICIOS BRINDADOS",
    4: "04 PROYECTOS DE INVERSIÓN PÚBLICA",
    5: "05 EJECUCIÓN PRESUPUESTAL",
    6: "06 RECURSOS HUMANOS",
    7: "07 PARTIDAS ARANCELARIAS",
}

MONTHS_DICT = {
    'mes_01' :'1', 
    'mes_02' :'2', 
    'mes_03' :'3', 
    'mes_04' :'4',
    'mes_05' :'5', 
    'mes_06' :'6', 
    'mes_07' :'7', 
    'mes_08' :'8', 
    'mes_09' :'9', 
    'mes_10' :'10', 
    'mes_11' :'11',
    'mes_12':'12'}

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        
        k = 1
        df = {}
        for i in range(2,2 +1):
            path, dirs, files = next(os.walk("../../data/01. Información ITP red CITE  (01-10-2020)/{}/".format(CARPETAS_DICT[i])))
            file_count = len(files)


            for j in range(2, 2 + 1 ):
                file_dir = "../../data/01. Información ITP red CITE  (01-10-2020)/{}/TABLA_0{}_N0{}.csv".format(CARPETAS_DICT[i],i,j)

                df = pd.read_csv(file_dir)
                k = k + 1
        
        empresas_list = list(df["tipo"].unique())
        empresas_map = {k:v for (k,v) in zip(sorted(empresas_list), list(range(1, len(empresas_list) +1)))}

        df = pd.melt(df, id_vars=['anio','tipo'], value_vars=['mes_01', 'mes_02', 'mes_03', 'mes_04',
               'mes_05', 'mes_06', 'mes_07', 'mes_08', 'mes_09', 'mes_10', 'mes_11',
               'mes_12'])
        df = df.rename(columns={'variable':'month_id','anio':'year','value':'empresas','tipo':'empresa_tipo'})

        df['month_id'] = df['month_id'].map(MONTHS_DICT)
        df['time_id'] = df['year'].astype(str) + df['month_id'].str.zfill(2)
        df['empresa_id'] = df['empresa_tipo'].map(empresas_map)
        df = df[['time_id','empresa_id','empresas']]


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
            'time_id':               'UInt32',
            'empresas':              'UInt32',
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
            "ingest": False
        }
    )