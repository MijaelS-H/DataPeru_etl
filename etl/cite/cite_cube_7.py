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

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        k = 1
        df = {}
        for i in range(4,4 +1):
            path, dirs, files = next(os.walk("../../data/01. Información ITP red CITE  (01-10-2020)/{}/".format(CARPETAS_DICT[i])))
            file_count = len(files)


            for j in range(3, 3 + 1 ):
                file_dir = "../../data/01. Información ITP red CITE  (01-10-2020)/{}/TABLA_0{}_N0{}.csv".format(CARPETAS_DICT[i],i,j)

                df = pd.read_csv(file_dir)
                k = k + 1

        ## Empresas dim
        cite_list = list(df["cite"].unique())
        cite_map = {k:v for (k,v) in zip(sorted(cite_list), list(range(1, len(cite_list) +1)))}
        
        ## componente dim
        componente_list = list(df["componente"].unique())
        componente_map = {k:v for (k,v) in zip(sorted(componente_list), list(range(1, len(componente_list) +1)))}
        
        df['cite_id'] = df['cite'].map(cite_map)
        df['componente_id'] = df['componente'].map(componente_map)
        df['inversion'] = df['inversion'].str[:-3].replace(',','', regex=True)
        df['ejecucion'] = df['ejecucion'].str[:-3].replace(',','', regex=True)
        
        df = df[['cite_id', 'componente_id', 'inversion', 'ejecucion']]
        
      
        return df

class CiteEjecucionPipeline(EasyPipeline):
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
            'cite_id':                 'UInt8',
            'componente_id':           'UInt8',
            'inversion':               'UInt64',
            'ejecucion':               'UInt64',
         }

        transform_step = TransformStep()  
        load_step = LoadStep(
          'cite_ejecucion', connector=db_connector, if_exists='drop',
          pk=['cite_id'], dtype=dtypes, nullable_list=['inversion','ejecucion'])

        if params.get("ingest")==True:
            steps = [transform_step, load_step]
        else:
            steps = [transform_step]

        return steps

if __name__ == "__main__":
    cite_ejecucion_pipeline = CiteEjecucionPipeline()
    cite_ejecucion_pipeline.run(
        {
            "output-db": "clickhouse-local",
            "ingest": False
        }
    )