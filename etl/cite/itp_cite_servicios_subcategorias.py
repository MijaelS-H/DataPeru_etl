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
from static import CARPETAS_DICT, MONTHS_DICT


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_csv("../../../datasets/20201001/01. Información ITP red CITE  (01-10-2020)/03 SERVICIOS BRINDADOS/TABLA_03_N01.csv")

        df = pd.melt(df, id_vars=['cite','anio','subcategoria'], value_vars=['mes_01','mes_02', 'mes_03', 'mes_04',
                'mes_05', 'mes_06', 'mes_07', 'mes_08', 'mes_09', 'mes_10', 'mes_11',
                'mes_12'])
                
        df['subcategoria'] = df['subcategoria'].str.strip()
        subcategory_list = list(df["subcategoria"].unique())
        subcategory_map = {k:v for (k,v) in zip(sorted(subcategory_list), list(range(1, len(subcategory_list) + 1)))}

        cite_list = list(df["cite"].unique())
        cite_map = {k:v for (k,v) in zip(sorted(cite_list), list(range(1, len(cite_list) +1)))}

        df = df.rename(columns={'variable':'month_id','value': "servicios"})
        df['month_id'] = df['month_id'].map(MONTHS_DICT)
        df['time'] = df['anio'].astype(str) + df['month_id'].str.zfill(2)
        
        df['cite_id'] = df['cite'].map(cite_map)
        df['subcategoria_id'] = df['subcategoria'].map(subcategory_map)

        df[['cite_id', 'subcategoria_id', 'time']] = df[['cite_id', 'subcategoria_id', 'time']].astype(int)
        df['servicios'] =  df['servicios'].astype(float)
        
        df = df[['cite_id', 'subcategoria_id', 'time', 'servicios']]

        
        return df

class CiteSubcategoryPipeline(EasyPipeline):
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
            'subcategoria_id':        'UInt8',
            'time':                   'UInt32',
            'servicios':               'Float32',
         }

        transform_step = TransformStep()  
        load_step = LoadStep(
          'itp_cite_servicios_subcategorias', connector=db_connector, if_exists='drop',
          pk=['cite_id'], dtype=dtypes, nullable_list=['servicios'])

        if params.get("ingest")==True:
            steps = [transform_step, load_step]
        else:
            steps = [transform_step]

        return steps

if __name__ == "__main__":
    cite_subcategory_pipeline = CiteSubcategoryPipeline()
    cite_subcategory_pipeline.run(
        {
            "output-db": "clickhouse-local",
            "ingest": True
        }
    )