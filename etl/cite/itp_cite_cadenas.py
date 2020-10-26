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
from static import CARPETAS_DICT, TIPO_CITE_DICT


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        k = 1
        df = {}
        for i in range(1,1 +1):
            path, dirs, files = next(os.walk("../../../datasets/20201001/01. Información ITP red CITE  (01-10-2020)/{}/".format(CARPETAS_DICT[i])))
            file_count = len(files)
    
            for j in range(1, file_count + 1 ):
                file_dir = "../../../datasets/20201001/01. Información ITP red CITE  (01-10-2020)/{}/TABLA_0{}_N0{}.csv".format(CARPETAS_DICT[i],i,j)
                df[k] = pd.read_csv(file_dir)
                k = k + 1

        df_list = [df[i] for i in range(1, file_count + 1)]
        df = reduce(lambda df1,df2: pd.merge(df1,df2,on=['cite'],how='outer'), df_list)

        df = df[['cite', 'cadena_atencion', 'cadena_pip', 'cadena_resolucion']]

        df.rename(columns={'cite' : 'cite_id', 'cadena_atencion' : 'cadena_atencion_id', 'cadena_pip' : 'cadena_pip_id', 'cadena_resolucion' : 'cadena_resolucion_id'},
                           inplace = True)
        
        ## df has no measures
        df['cantidad_cite'] = 1

        return df

class ReplaceStep(PipelineStep):
    def run_step(self, prev, params):

        df = prev

        cite_dim = dict(zip(df['cite_id'].dropna().unique(), range(1, len(df['cite_id'].dropna().unique()) + 1 )))
        df['cite_id'].replace(cite_dim, inplace=True)

        cadena_atencion_dim = dict(zip(df['cadena_atencion_id'].dropna().unique(), range(1, len(df['cadena_atencion_id'].dropna().unique()) + 1 )))
        df['cadena_atencion_id'].replace(cadena_atencion_dim, inplace=True)

        cadena_pip_dim = dict(zip(df['cadena_pip_id'].dropna().unique(), range(1, len(df['cadena_pip_id'].dropna().unique()) + 1 )))
        df['cadena_pip_id'].replace(cadena_pip_dim, inplace=True)

        cadena_resolucion_dim = dict(zip(df['cadena_resolucion_id'].dropna().unique(), range(1, len(df['cadena_resolucion_id'].dropna().unique()) + 1 )))
        df['cadena_resolucion_id'].replace(cadena_resolucion_dim, inplace=True)

        return df

class FormatStep(PipelineStep):
    def run_step(self, prev, params):
       
        df = prev

        df[['cite_id', 'cadena_atencion_id', 'cadena_pip_id', 'cadena_resolucion_id']] = df[['cite_id', 'cadena_atencion_id', 'cadena_pip_id', 'cadena_resolucion_id']].astype(int)
        
        return df

class CiteInfoPipeline(EasyPipeline):
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
            'cadena_atencion_id':    'UInt8',
            'cadena_resolucion_id':  'UInt8',
            'cadena_pip_id':         'UInt8',
            'cantidad_cite':         'UInt8',  
         }

        transform_step = TransformStep()
        replace_step = ReplaceStep()
        format_step = FormatStep()

        load_step = LoadStep(
          'itp_cite_cadenas', connector=db_connector, if_exists='drop',
          pk=['cite_id'], dtype=dtypes, nullable_list=[])

        if params.get("ingest")==True:
            steps = [transform_step, replace_step, format_step,  load_step]
        else:
            steps = [transform_step, replace_step, format_step]

        return steps

if __name__ == "__main__":
    pp = CiteInfoPipeline()
    pp.run(
        {
            "output-db": "clickhouse-local",
            "ingest": True
        }
    )