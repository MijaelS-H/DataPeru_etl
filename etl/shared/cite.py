import numpy as np
import pandas as pd
import os
from functools import reduce
from unidecode import unidecode
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
        for i in range(1,1 +1):
            path, dirs, files = next(os.walk("../../../datasets/20201001/01. Información ITP red CITE  (01-10-2020)/{}/".format(CARPETAS_DICT[i])))
            file_count = len(files)
    
            for j in [1]:
                file_dir = "../../../datasets/20201001/01. Información ITP red CITE  (01-10-2020)/01 INFORMACIÓN INSTITUCIONAL/TABLA_01_N01.csv".format(CARPETAS_DICT[i],i,j)

                df = pd.read_csv(file_dir)
                k = k + 1

        df['cite_id'] = df['cite'].index + 1

        df['cite'] = df['cite'].str.replace("CITE","")
        df['cite'] = df['cite'].str.replace("UT","")
        df['cite'] = df['cite'].str.capitalize()
        df['cite'] = df['cite'].apply(unidecode)
        
 

        df = df[['cite_id','cite_name']]


        return df

class CiteDimPipeline(EasyPipeline):
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
            'cite_id':               'UInt8',
            'cite_name':              'String',

         }

        transform_step = TransformStep()  
        load_step = LoadStep(
          'dim_shared_cite', connector=db_connector, if_exists='drop',
          pk=['cite_id'], dtype=dtypes, nullable_list=[])

        if params.get("ingest")==True:
            steps = [transform_step, load_step]
        else:
            steps = [transform_step]

        return steps

if __name__ == "__main__":
    pp = CiteDimPipeline()
    pp.run(
        {
            "output-db": "clickhouse-local",
            "ingest": False
        }
    )