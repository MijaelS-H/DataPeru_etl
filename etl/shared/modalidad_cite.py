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
        for i in range(6,6 +1):
            path, dirs, files = next(os.walk("../../data/01. Información ITP red CITE  (01-10-2020)/{}/".format(CARPETAS_DICT[i])))
            file_count = len(files)


            for j in range(1, 1 + 1 ):
                file_dir = "../../data/01. Información ITP red CITE  (01-10-2020)/{}/TABLA_0{}_N0{}.csv".format(CARPETAS_DICT[i],i,j)

                df = pd.read_csv(file_dir)
                k = k + 1
        
        df = pd.melt(df, id_vars=['cite','anio','modalidad'], value_vars=['directivo', 'tecnico', 'operativo', 'administrativo',
               'practicante'])
        df = df.rename(columns={'variable':'tipo_trabajador','anio':'year','value':'cantidad'})

 

        ## modalidad dim
        modalidad_list = list(df["modalidad"].unique())
        modalidad_map = {k:v for (k,v) in zip(sorted(modalidad_list), list(range(1, len(modalidad_list) +1)))}

   
        df = pd.DataFrame({"modalidad_id": list(range(len(modalidad_list))),"modalidad": sorted(modalidad_list)})


      
        return df

class ModalidadPipeline(EasyPipeline):
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
            'modalidad_id':                         'UInt8',
            'modalidad':                            'String',

         }

        transform_step = TransformStep()  
        load_step = LoadStep(
          'dim_shared_cite_modalidad', connector=db_connector, if_exists='drop',
          pk=['modalidad_id'], dtype=dtypes, nullable_list=[])

        if params.get("ingest")==True:
            steps = [transform_step, load_step]
        else:
            steps = [transform_step]

        return steps

if __name__ == "__main__":
    modalidad_pipeline = ModalidadPipeline()
    modalidad_pipeline.run(
        {
            "output-db": "clickhouse-local",
            "ingest": False
        }
    )