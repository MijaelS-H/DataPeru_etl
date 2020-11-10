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

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_excel(path.join(params["datasets"],"20201001", "01. Información ITP red CITE  (01-10-2020)", "07 PARTIDAS ARANCELARIAS", "TABLA_08_N01 (18-10-2020).xlsx"))

        df = df[df['cite'].notna()]
        df = df.rename(columns={'descripcion_partida ' : 'descripcion_partida','partida_arancelaria' : 'partida_id','tipo_exportación' : 'tipo_exportacion'})
        df['descripcion_partida'] = df['descripcion_partida'].str.capitalize()
        df['tipo_exportacion'] = df['tipo_exportacion'].str.capitalize()
        df['sector'] = df['sector'].str.capitalize()
        df['hs6_id'] = df['partida_id'].astype(str).str[:-6].str.zfill(6)
        df['cantidad_cite'] = 1
        df['cadena_productiva'] = df['cadena_productiva'].str.strip()
        
        cite_list = list(df["cite"].unique())
        cite_map = {k:v for (k,v) in zip(sorted(cite_list), list(range(1, len(cite_list) +1)))}
        df['cite_id'] = df['cite'].map(cite_map)


        sector_list = list(df["sector"].unique())
        sector_map = {k:v for (k,v) in zip(sorted(sector_list), list(range(1, len(sector_list) +1)))}
        df['sector_id'] = df['sector'].map(sector_map)

        tipo_exp_list = list(df["tipo_exportacion"].unique())
        tipo_exp_map = {k:v for (k,v) in zip(sorted(tipo_exp_list), list(range(1, len(tipo_exp_list) +1)))}
        df['tipo_exp_id'] = df['tipo_exportacion'].map(tipo_exp_map)

        cadena_productiva_list = list(df["cadena_productiva"].unique())
        cadena_productiva_map = {k:v for (k,v) in zip(sorted(cadena_productiva_list), list(range(1, len(cadena_productiva_list) +1)))}
        df['cad_prod_id'] = df['cadena_productiva'].map(cadena_productiva_map)

        df[['cite_id','sector_id','cad_prod_id','tipo_exp_id','cantidad_cite']] = df[['cite_id','sector_id','cad_prod_id','tipo_exp_id','cantidad_cite']].astype(int)
        df = df[['cite_id','sector_id','cad_prod_id','hs6_id','tipo_exp_id','cantidad_cite']]

    
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
            'hs6_id':                 'String',
            'tipo_exp_id':            'UInt8',
            'cantidad_cite':          'UInt8',
         }

        transform_step = TransformStep()
        agg_step = AggregatorStep('itp_cite_partidas', measures=['cantidad_cite'])
        load_step = LoadStep('itp_cite_partidas', connector=db_connector, if_exists='drop', pk=['cite_id'], dtype=dtypes)

        return [transform_step, agg_step, load_step]

def run_pipeline(params: dict):
    pp = CitePartidasPipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys

    run_pipeline({
        "connector": params["connector"],
        "datasets": sys.argv[1]
    })
