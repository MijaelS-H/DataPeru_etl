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


            for j in range(1, file_count):
                file_dir = "../../data/01. Información ITP red CITE  (01-10-2020)/{}/TABLA_0{}_N0{}.csv".format(CARPETAS_DICT[i],i,j)

                df[k] = pd.read_csv(file_dir)
                k = k + 1

        df_list = [df[i] for i in range(1,3)]
        df = reduce(lambda df1,df2: pd.merge(df1,df2,on=['cite'],how='outer'), df_list)


        df["aspecto"] = df["aspecto"].str.strip()        
        aspecto_list = list(df["aspecto"].unique())
        aspecto_map = {k:v for (k,v) in zip(sorted(aspecto_list), list(range(1, len(aspecto_list) + 1)))}

        cite_list = list(df["cite"].unique())
        cite_map = {k:v for (k,v) in zip(sorted(cite_list), list(range(1, len(cite_list) +1)))}

        df['cite_id'] = df['cite'].map(cite_map)
        df['aspecto_id'] = df['aspecto'].map(aspecto_map)
        df = df[['cite_id','aspecto_id','estado']]


        return df   

class CiteAspectoPipeline(EasyPipeline):
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
            'cite_id':                'UInt8',
            'aspecto_id':             'UInt8',
            'estado':                 'String',
         }

        transform_step = TransformStep()  
        load_step = LoadStep(
          'itp_cite_clientes_aspecto', connector=db_connector, if_exists='drop',
          pk=['cite_id'], dtype=dtypes, nullable_list=['estado'])

        if params.get("ingest")==True:
            steps = [transform_step, load_step]
        else:
            steps = [transform_step]

        return steps

if __name__ == "__main__":
    cite_aspecto_pipeline = CiteAspectoPipeline()
    cite_aspecto_pipeline.run(
        {
            "output-db": "clickhouse-local",
            "ingest": False
        }
    )