
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep
from etl.survey_indicators.enaho.enaho_pipeline import TransformStep
from shared import ReplaceStep


class ProcessingStep(PipelineStep):
    def run_step(self, prev, params):
        df, category_dim, indicator_dim, geo_dim, region_dim = prev

        if params.get('pk') == 'category_id':
            df = pd.DataFrame.from_dict(category_dim, orient='index').reset_index()
            df.columns = ['category_name', 'category_id']
            df.dropna(inplace=True)
            return df
        elif params.get('pk') == 'indicator_id':
            df = pd.DataFrame.from_dict(indicator_dim, orient='index').reset_index()
            df.columns = ['indicator_name', 'indicator_id']
            return df
        elif params.get('pk') == 'region_id':
            df = pd.DataFrame.from_dict(region_dim, orient='index').reset_index()
            df.columns = ['region_name', 'region_id']
            df.dropna(inplace=True)
            return df
        else:
            df = pd.DataFrame.from_dict(geo_dim, orient='index').reset_index()
            df.columns = ['geo_name', 'geo_id']
            df.dropna(inplace=True)
            return df

class DimsPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return[
            Parameter(name='pk', dtype=str),
            Parameter(name='table_name', dtype=str)
        ]

    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            params.get('pk'): 'UInt8'
        }

        transform_step = TransformStep()
        replace_step = ReplaceStep(connector=db_connector)
        processing_step = ProcessingStep()
        load_step = LoadStep(params.get('table_name'), db_connector, if_exists='drop', 
                             pk=[params.get('pk')], dtype=dtype)

        return [transform_step, replace_step, processing_step, load_step]

if __name__ == "__main__":
    pp = DimIndustryPipeline()
    for k, v in {'category_id':  'dim_category_ene',
                 'indicator_id': 'dim_indicator_ene',
                 'region_id': 'dim_region_ene', 
                 'geo_id': 'dim_geo_ene'}.items():
        pp.run({'pk': k,
                'table_name': v})
