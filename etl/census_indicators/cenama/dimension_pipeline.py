import numpy as np
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep
from bamboo_lib.steps import LoadStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        #read dataframe
        df = pd.read_spss('../../../datasets/20201001/02. Información Censos (01-10-2020)/03 CENSO NACIONAL DE MERCADOS DE ABASTO/02 MÓDULO 1117_ Identificación del Mercado e Informante/Capítulo_II_NACIONAL.sav')

        df.columns = df.columns.str.lower()
        df = df[['id', 'nombre_mercado']]
        
        df['nombre_mercado'] = df['nombre_mercado'].str.capitalize()

        df.rename(columns = {
            'id': 'market_id',
            'nombre_mercado': 'market_name'
        }, inplace=True)
        
        return df

class MarketPipeline(EasyPipeline):
    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'market_id': 'String',
            'market_name': 'String'
        }

        transform_step = TransformStep()
    
        load_step = LoadStep('dim_market', db_connector, if_exists='drop', 
                             pk=['market_id'], dtype=dtype,
                             nullable_list=[])

        return [transform_step, load_step]

if __name__ == "__main__":
    pp = MarketPipeline()
    pp.run({})