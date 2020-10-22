import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep
from cultura_agentes import TransformStep
from shared import ReplaceStep


class ProcessingStep(PipelineStep):
    def run_step(self, prev, params):
        df, razon_social_dim, actividad_1_dim, actividad_2_dim, actividad_3_dim, actividad_4_dim = prev

        if params.get('pk') == 'razon_social_id':
            df = pd.DataFrame.from_dict(razon_social_dim, orient='index').reset_index()
            df.columns = ['razon_social_name', 'razon_social_id']
            
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

        db_connector = Connector.fetch('clickhouse-database', open('../../../conns.yaml'))

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
    pp = DimAgentesPipeline()
    for k, v in {'razon_social_id':  'dim_razon_social_agentes',
                 }.items():
        pp.run({'pk': k,
                'table_name': v})
