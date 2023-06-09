
import numpy as np
import pandas as pd
from os import path
from bamboo_lib.connectors.models import Connector
from bamboo_lib.helpers import query_to_df
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_excel(path.join(params["datasets"], "01_Informacion_ITP_red_CITE", "07_PARTIDAS_ARANCELARIAS", "TABLA_07_N01.xlsx"))
        df2 = pd.read_excel(path.join(params["datasets"], "01_Informacion_ITP_red_CITE", "07_PARTIDAS_ARANCELARIAS", "TABLA_08_N01.xlsx"))

        df = df[df['cadena_productiva'].notna()]
        df['hs6_id'] = df['partida_arancelaria'].astype(str).str[:-6].str.zfill(6)
        df.rename(columns ={'descripcion_partida ' : 'hs10_name', 'partida_arancelaria' : 'hs10_id'}, inplace=True)
        df['hs10_name'] = df['hs10_name'].str.title()
        hs_10_dim = df[['hs10_id','hs10_name']].copy()
        
        df2 = df2[df2['cadena_productiva'].notna()]
        df2.rename(columns ={'descripcion_partida ' : 'hs10_name', 'partida_arancelaria' : 'hs10_id'}, inplace=True)
        df2['hs10_name'] = df2['hs10_name'].str.title()

        hs_10_dim = hs_10_dim.append(df2[['hs10_id', 'hs10_name']])

        # hs6_query = 'SELECT * FROM dim_shared_hs'
        # db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))
        # hs6  = query_to_df(db_connector, raw_query=hs6_query)

        # hs_10_dim = pd.merge(hs6, hs10, on=['hs6_id'])
        hs_10_dim['hs10_id'] = hs_10_dim['hs10_id'].fillna(0000000000)
        hs_10_dim['hs10_name'] = hs_10_dim['hs10_name'].fillna('No definido')

        hs_10_dim['hs10_id'] = hs_10_dim['hs10_id'].astype(int).astype(str).str.zfill(10)
        # hs_10_dim = hs_10_dim.append({'chapter_id' : 0, 'chapter_name': 'No definido', 'hs2_id': '00', 'hs2_name': 'No definido', 'hs4_id': '0000',
        # 'hs4_name': 'No definido', 'hs6_id': '000000', 'hs6_name': 'No definido', 'hs10_id': '0000000000', 'hs10_name': 'No definido'}, ignore_index=True)

        hs_10_dim = hs_10_dim.append(
            {'hs10_id': '0000000000', 'hs10_name': 'No definido'}, ignore_index=True
        )

        hs_10_dim = hs_10_dim.drop_duplicates(subset=['hs10_id'])

        return hs_10_dim

class HS_10_Pipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        
        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtype = {
            # 'chapter_id':       'UInt8',
            # 'chapter_name':     'String',    
            # 'hs2_id':           'String',          
            # 'hs2_name':         'String',        
            # 'hs4_id':           'String',          
            # 'hs4_name':         'String',        
            # 'hs6_id':           'String',          
            # 'hs6_name':         'String',
            'hs10_id':          'String',          
            'hs10_name':        'String'            
        }

        transform_step = TransformStep()
        load_step = LoadStep('dim_shared_hs10', db_connector, if_exists='drop', pk=['hs10_id'], dtype=dtype)

        return [transform_step, load_step]

def run_pipeline(params: dict):
    pp = HS_10_Pipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
