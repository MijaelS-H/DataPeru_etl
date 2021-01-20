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
from .static import HS_DICT

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_excel(path.join(params["datasets"],"20201001", "01. Información ITP red CITE  (01-10-2020)", "07 PARTIDAS ARANCELARIAS", "TABLA_08_N01 (18-10-2020).xlsx"))

        df = df[df['cite'].notna()]
        df['partida_arancelaria'] = df['partida_arancelaria'].fillna("0000000000")
        df['descripcion_partida '] = df['descripcion_partida '].fillna("No Definido")

        df = df.rename(columns={'descripcion_partida ' : 'descripcion_partida','partida_arancelaria' : 'partida_id','tipo_exportación' : 'tipo_exportacion'})
        df['descripcion_partida'] = df['descripcion_partida'].str.capitalize()
        df['tipo_exportacion'] = df['tipo_exportacion'].str.capitalize()
        df['sector'] = df['sector'].str.capitalize()
        
        df['hs10_id'] = df['partida_id'].astype(int).astype(str).str.zfill(10)
        df['hs6_id'] = df['partida_id'].astype(int).astype(str).str.zfill(10).str[:-4]
        df['hs6_id'] = df['hs6_id'].replace(HS_DICT)
        
        df['cantidad_cite'] = 1
        df['cadena_productiva'] = df['cadena_productiva'].str.strip()
        
        dim_cite_query = 'SELECT cite, cite_id FROM dim_shared_cite'
        dim_cite = query_to_df(self.connector, raw_query=dim_cite_query)
        df = df.merge(dim_cite, on="cite")
        
        sector_list = list(df["sector"].unique())
        sector_map = {k:v for (k,v) in zip(sorted(sector_list), list(range(1, len(sector_list) +1)))}
        df['sector_id'] = df['sector'].map(sector_map)

        tipo_exp_list = list(df["tipo_exportacion"].unique())
        tipo_exp_map = {k:v for (k,v) in zip(sorted(tipo_exp_list), list(range(1, len(tipo_exp_list) +1)))}
        df['tipo_exp_id'] = df['tipo_exportacion'].map(tipo_exp_map)

        dim_cadena_query = 'SELECT cadena_productiva, cad_prod_id FROM dim_shared_cite_cad_prod'
        dim_cadena = query_to_df(self.connector, raw_query=dim_cadena_query)
        df = df.merge(dim_cadena, on="cadena_productiva")

        df[['cite_id','sector_id','cad_prod_id','tipo_exp_id','cantidad_cite']] = df[['cite_id','sector_id','cad_prod_id','tipo_exp_id','cantidad_cite']].astype(int)
        df = df[['cite_id','sector_id','cad_prod_id','hs10_id', 'hs6_id', 'tipo_exp_id','cantidad_cite']]

    
        return df

class CitePartidasPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtypes = {
            'cite_id':                'UInt8',
            'sector_id':              'UInt8',
            'cad_prod_id':            'UInt8',
            'hs10_id':                'String',
            'hs6_id':                 'String',
            'tipo_exp_id':            'UInt8',
            'cantidad_cite':          'UInt8',
         }

        transform_step = TransformStep(connector=db_connector)
        agg_step = AggregatorStep('itp_cite_partidas', measures=['cantidad_cite'])
        load_step = LoadStep('itp_cite_partidas', connector=db_connector, if_exists='drop', pk=['cite_id'], dtype=dtypes)

        return [transform_step, agg_step, load_step]

def run_pipeline(params: dict):
    pp = CitePartidasPipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
