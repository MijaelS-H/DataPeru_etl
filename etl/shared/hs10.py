
import numpy as np
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.helpers import query_to_df
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_excel('../../../datasets/20201001/01. Información ITP red CITE  (01-10-2020)/07 PARTIDAS ARANCELARIAS/TABLA_07_N01.xlsx')
        
        df = df[df['cadena_productiva'].notna()]
        df['hs6_id'] = df['partida_arancelaria'].astype(str).str[:-6].str.zfill(6)
        df.rename(columns ={'descripcion_partida ' : 'hs10_name', 'partida_arancelaria' : 'hs10_id'}, inplace=True)
        df['hs10_name'] = df['hs10_name'].str.title()
        hs10 = df[['hs6_id','hs10_id','hs10_name']]
        
        hs6_query = 'SELECT * FROM dim_shared_hs'
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
        hs6  = query_to_df(db_connector, raw_query=hs6_query)

        hs_10_dim = pd.merge(hs6, hs10, on=['hs6_id'])

        hs_10_dim['hs10_id'] = hs_10_dim['hs10_id'].astype(int).astype(str).str.zfill(10)
        
        return hs_10_dim

class HSPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'chapter_id':       'UInt8',
            'chapter_name':     'String',    
            'hs2_id':           'String',          
            'hs2_name':         'String',        
            'hs4_id':           'String',          
            'hs4_name':         'String',        
            'hs6_id':           'String',          
            'hs6_name':         'String',
            'hs10_id':          'String',          
            'hs10_name':        'String'            
        }

        transform_step = TransformStep()
        load_step = LoadStep('dim_shared_hs10', db_connector, if_exists='drop', pk=['hs10_id'], 
            dtype=dtype, 
            nullable_list=[]
        )

        return [transform_step, load_step]

if __name__ == '__main__':
    pp = HSPipeline()
    pp.run({})    