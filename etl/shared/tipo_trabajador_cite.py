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

 

        ## contratos
        df["tipo_trabajador"] = df["tipo_trabajador"].str.capitalize()
        tipo_trabajador_list = list(df["tipo_trabajador"].unique())
        tipo_trabajador_map = {k:v for (k,v) in zip(sorted(tipo_trabajador_list), list(range(1, len(tipo_trabajador_list) +1)))}

   
        df = pd.DataFrame({"tipo_trabajador_id": list(range(len(tipo_trabajador_list))),"tipo_trabajador": sorted(tipo_trabajador_list)})

   
        return df

class TipoTrabajadorPipeline(EasyPipeline):
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
            'tipo_trabajador_id':                         'UInt8',
            'tipo_trabajador':                            'String',

         }

        transform_step = TransformStep()  
        load_step = LoadStep(
          'dim_shared_cite_tipo_trabajador', connector=db_connector, if_exists='drop',
          pk=['tipo_trabajador_id'], dtype=dtypes, nullable_list=[])

        if params.get("ingest")==True:
            steps = [transform_step, load_step]
        else:
            steps = [transform_step]

        return steps

if __name__ == "__main__":
    tipo_trabajador_pipeline = TipoTrabajadorPipeline()
    tipo_trabajador_pipeline.run(
        {
            "output-db": "clickhouse-local",
            "ingest": False
        }
    )