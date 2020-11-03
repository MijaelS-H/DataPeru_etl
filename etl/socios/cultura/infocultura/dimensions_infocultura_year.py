import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep
from cultura_infocultura_year import TransformStep
from shared_year import ReplaceStep

class ProcessingStep(PipelineStep):
    def run_step(self, prev, params):
        df, indicator_dim, category_dim, subcategory_dim = prev

        if params.get('pk') == 'indicator_id':
            df = pd.DataFrame.from_dict(indicator_dim, orient='index').reset_index()
            df.columns = ['indicator_name', 'indicator_id']
            
            return df
        
        elif params.get('pk') == 'category_id':
            df = pd.DataFrame.from_dict(category_dim, orient='index').reset_index()
            df.columns = ['category_name', 'category_id']
            
            return df        
        
        elif params.get('pk') == 'subcategory_id':
            df = pd.DataFrame.from_dict(subcategory_dim, orient='index').reset_index()
            df.columns = ['subcategory_name', 'subcategory_id']
            
            return df   


class DimAgentesPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return[
            Parameter(name='pk', dtype=str),
            Parameter(name='table_name', dtype=str)
        ]

    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open('../../conns.yaml'))

        if (k == 'subcategory_id'):
            dtype = {
                params.get('pk'): 'UInt16'
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

        return [transform_step, replace_step, processing_step, load_step]

if __name__ == "__main__":
    pp = DimAgentesPipeline()
    for k, v in {'indicator_id':    'dim_shared_infocultura_indicators_year',
                 'category_id':     'dim_shared_infocultura_categories_year',
                 'subcategory_id':  'dim_shared_infocultura_subcategories_year',
                 }.items():
        pp.run({'pk': k,
                'table_name': v})
