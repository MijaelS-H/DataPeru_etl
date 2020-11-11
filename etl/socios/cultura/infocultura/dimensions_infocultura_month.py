import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep
from etl.socios.cultura.infocultura.cultura_infocultura_month import TransformStep
from etl.socios.cultura.infocultura.shared_month import ReplaceStep

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


class DimInfoculturaMonthPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return[
            Parameter(name='pk', dtype=str),
            Parameter(name='table_name', dtype=str)
        ]

    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open(params['connector']))

        dtype = {
                params.get('pk'): 'UInt8'
        }

        transform_step = TransformStep()
        replace_step = ReplaceStep()
        processing_step = ProcessingStep()
        load_step = LoadStep(params.get('table_name'), db_connector, if_exists='drop', 
                             pk=[params.get('pk')], dtype=dtype)

        return [transform_step, replace_step, processing_step, load_step]

def run_pipeline(params: dict):

    pp = DimInfoculturaMonthPipeline()

    levels = {
        'indicator_id':    'dim_shared_infocultura_indicators_month',
        'category_id':     'dim_shared_infocultura_categories_month',
        'subcategory_id':  'dim_shared_infocultura_subcategories_month',
    }

    for k, v in levels.items():

        pp_params = {'pk': k, 'table_name': v}
        pp_params.update(params)

        pp.run(pp_params)

if __name__ == "__main__":
    import sys
    from os import path
    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline({
        "connector": path.join(__dirname, "..", "..", "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })