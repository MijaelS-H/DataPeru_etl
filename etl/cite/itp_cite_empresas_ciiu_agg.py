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
from static import CARPETAS_DICT

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        k = 1
        df = {}
        for i in range(2,2 +1):
            path, dirs, files = next(os.walk("../../../datasets/20201001/01. Información ITP red CITE  (01-10-2020)/{}/".format(CARPETAS_DICT[i])))
            file_count = len(files)

            for j in range(3, 3 + 1 ):
                file_dir = "../../../datasets/20201001/01. Información ITP red CITE  (01-10-2020)/{}/TABLA_0{}_N0{}.csv".format(CARPETAS_DICT[i],i,j)

                df = pd.read_csv(file_dir)
                k = k + 1

        df= df[['cod_ciiu', 'anio', 'empresas']]
        
        df = df.rename(columns={'cod_ciiu' : 'class_id'})

        df['class_id'] = df['class_id'].replace({"No determinados" : "0000"})
        
        df[['empresas']] = df[['empresas']].astype(float)

        return df

class CiteEmpresas2Pipeline(EasyPipeline):
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

            'class_id':              'String',
            'anio':                  'UInt16',
            'empresas':              'Float32',
         }

        transform_step = TransformStep()  
        load_step = LoadStep(
          'itp_cite_empresas_ciiu_agg', connector=db_connector, if_exists='drop',
          pk=['class_id'], dtype=dtypes, nullable_list=['empresas'])

        if params.get("ingest")==True:
            steps = [transform_step, load_step]
        else:
            steps = [transform_step]

        return steps

if __name__ == "__main__":
    cite_empresas2_pipeline = CiteEmpresas2Pipeline()
    cite_empresas2_pipeline.run(
        {
            "output-db": "clickhouse-local",
            "ingest": True
        }
    )