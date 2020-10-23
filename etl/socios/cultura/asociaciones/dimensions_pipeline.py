import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep
from cultura_asociaciones import TransformStep
from shared import ReplaceStep


class ProcessingStep(PipelineStep):
    def run_step(self, prev, params):
        df, asociacion_dim, actividad_n_1_dim, actividad_n_2_dim, manifestacion_n_1_dim, manifestacion_n_2_dim, manifestacion_n_3_dim, inscrita_sunarp_dim = prev

        if params.get('pk') == 'codigo_asociacion':
            df = pd.DataFrame.from_dict(asociacion_dim, orient='index').reset_index()
            df.columns = ['asociacion_name', 'codigo_asociacion']
         
            return df
        elif (params.get('pk') == 'manifestacion_n_1_id'):
            df = pd.DataFrame.from_dict(manifestacion_n_1_dim, orient='index').reset_index()
            df.columns = ['manifestacion_n_1_name', 'manifestacion_n_1_id']
        
            return df
        
        elif (params.get('pk') == 'manifestacion_n_2_id'):
            df = pd.DataFrame.from_dict(manifestacion_n_2_dim, orient='index').reset_index()
            df.columns = ['manifestacion_n_2_name', 'manifestacion_n_2_id']
      
            return df
        
        elif (params.get('pk') == 'manifestacion_n_3_id'):
            df = pd.DataFrame.from_dict(manifestacion_n_3_dim, orient='index').reset_index()
            df.columns = ['manifestacion_n_3_name', 'manifestacion_n_3_id']
      
            return df

class DimAsociacionesPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return[
            Parameter(name='pk', dtype=str),
            Parameter(name='table_name', dtype=str)
        ]

    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open('../../../conns.yaml'))

        if (k == 'codigo_asociacion'):
            dtype = {
                params.get('pk'): 'String'
            }
        else: 
            dtype = {
                params.get('pk'): 'UInt8'
            }
       
        transform_step = TransformStep()
        replace_step = ReplaceStep()
        processing_step = ProcessingStep()
        load_step = LoadStep(params.get('table_name'), db_connector, if_exists='drop', 
                             pk=[params.get('pk')], dtype=dtype)

        return [transform_step, replace_step, processing_step]

if __name__ == "__main__":
    pp = DimAsociacionesPipeline()
    for k, v in {'codigo_asociacion':  'dim_asociaciones_culturales',
                'manifestacion_n_1_id':  'dim_asociaciones_culturales_manifestacion_n_1',
                'manifestacion_n_2_id':  'dim_asociaciones_culturales_manifestacion_n_2',
                'manifestacion_n_3_id':  'dim_asociaciones_culturales_manifestacion_n_3',
                 }.items():
        pp.run({'pk': k,
                'table_name': v})
