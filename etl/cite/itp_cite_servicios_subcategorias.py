import numpy as np
import pandas as pd
import os
from os import path
from functools import reduce
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import grab_connector
from etl.consistency import AggregatorStep
from bamboo_lib.helpers import query_to_df

MONTHS_DICT = {'mes_01' :'1', 'mes_02' :'2', 'mes_03' :'3', 'mes_04' :'4','mes_05' :'5', 'mes_06' :'6', 'mes_07' :'7', 'mes_08' :'8', 'mes_09' :'9', 'mes_10' :'10', 'mes_11' :'11','mes_12':'12'}


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv(path.join(params["datasets"], "01_Informacion_ITP_red_CITE", "03_SERVICIOS_BRINDADOS", "TABLA_03_N01.csv"))

        df = pd.melt(df, id_vars=['cite','anio','subcategoria'], value_vars=['mes_01','mes_02', 'mes_03', 'mes_04',
                'mes_05', 'mes_06', 'mes_07', 'mes_08', 'mes_09', 'mes_10', 'mes_11',
                'mes_12'])

        df['subcategoria'] = df['subcategoria'].str.strip()
        subcategory_list = list(df["subcategoria"].unique())
        subcategory_map = {k:v for (k,v) in zip(sorted(subcategory_list), list(range(1, len(subcategory_list) + 1)))}
        
        dim_cite_query = 'SELECT cite, cite_id FROM dim_shared_cite'
        dim_cite = query_to_df(self.connector, raw_query=dim_cite_query)
        df = df.merge(dim_cite, on="cite")
        
        df = df.rename(columns={'variable':'month_id','value': "servicios"})
        df['month_id'] = df['month_id'].map(MONTHS_DICT)
        df['time'] = df['anio'].astype(str) + df['month_id'].str.zfill(2)
        
        df['subcategoria_id'] = df['subcategoria'].map(subcategory_map)

        df[['cite_id', 'subcategoria_id', 'time']] = df[['cite_id', 'subcategoria_id', 'time']].astype(int)
        df['servicios'] =  df['servicios'].astype(float)

        df = df[['cite_id', 'subcategoria_id', 'time', 'servicios']]

        return df

class CiteSubcategoryPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtypes = {
            'cite_id':                'UInt8',
            'subcategoria_id':        'UInt8',
            'time':                   'UInt32',
            'servicios':              'Float32'
         }

        transform_step = TransformStep(connector=db_connector)
        agg_step = AggregatorStep('itp_cite_servicios_subcategorias', measures=['servicios'])
        load_step = LoadStep('itp_cite_servicios_subcategorias', connector=db_connector, if_exists='drop', pk=['cite_id'], dtype=dtypes,
                            nullable_list=['servicios'])

        return [transform_step, load_step]

def run_pipeline(params: dict):
    pp = CiteSubcategoryPipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
