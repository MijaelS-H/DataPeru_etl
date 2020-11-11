
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep
from etl.survey_indicators.enave.enave_pipeline import TransformStep
from .shared import ReplaceStep


class ProcessingStep(PipelineStep):
    def run_step(self, prev, params):
        df, category_dim, indicator_dim = prev

        if params.get('pk') == 'category_id':
            df = pd.DataFrame.from_dict(category_dim, orient='index').reset_index()
            df.columns = ['category_name', 'category_id']
            return df
        else:
            df = pd.DataFrame.from_dict(indicator_dim, orient='index').reset_index()
            df.columns = ['indicator_name', 'indicator_id']
            return df

class DimENAVEPipeline(EasyPipeline):
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

    pp = DimENAVEPipeline()

    levels = {
        'category_id':  'dim_category_enave',
        'indicator_id': 'dim_indicator_enave'}

    for k, v in levels.items():

        pp_params = {'pk': k, 'table_name': v}
        pp_params.update(params)
        pp.run(pp_params)


if __name__ == "__main__":
    import sys
    from os import path
    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline({
        "connector": path.join(__dirname, "..", "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })

