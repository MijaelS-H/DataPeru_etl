import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep
from cultura_infocultura_month import TransformStep
from shared_month import ReplaceStep

class ProcessingStep(PipelineStep):
    def run_step(self, prev, params):
        df, indicator_dim, response_dim = prev

        if params.get('pk') == 'indicator_id':
            df = pd.DataFrame.from_dict(indicator_dim, orient='index').reset_index()
            df.columns = ['indicator_name', 'indicator_id']
            
            return df
        elif params.get('pk') == 'response_id':
            df = pd.DataFrame.from_dict(response_dim, orient='index').reset_index()
            df.columns = ['response_name', 'response_id']
            
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

        dtype = {
            params.get('pk'): 'UInt16'
        }

        transform_step = TransformStep()
        replace_step = ReplaceStep()
        processing_step = ProcessingStep()
        load_step = LoadStep(params.get('table_name'), db_connector, if_exists='drop', 
                             pk=[params.get('pk')], dtype=dtype)

        return [transform_step, replace_step, processing_step, load_step]

if __name__ == "__main__":
    pp = DimAgentesPipeline()
    for k, v in {'indicator_id':  'dim_shared_infocultura_indicators_month',
                 'response_id':  'dim_shared_infocultura_responses_month',
                 }.items():
        pp.run({'pk': k,
                'table_name': v})
