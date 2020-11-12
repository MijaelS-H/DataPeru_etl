import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import LoadStep

from .enaho_pipeline import TransformStep
from .shared import ReplaceStep


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

class DimENAHOPipeline(EasyPipeline):
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
        replace_step = ReplaceStep(connector=db_connector)
        processing_step = ProcessingStep()
        load_step = LoadStep(params.get('table_name'), db_connector, if_exists='drop',
                             pk=[params.get('pk')], dtype=dtype)

        return [transform_step, replace_step, processing_step, load_step]


def run_pipeline(params: dict):
    pp = DimENAHOPipeline()
    levels = {
        'category_id':  'dim_category_enaho',
        'indicator_id': 'dim_indicator_enaho',
        'region_id': 'dim_region_enaho',
        'geo_id': 'dim_geo_enaho',
    }

    for k, v in levels.items():
        pp.run({'pk': k, 'table_name': v, **params})


if __name__ == "__main__":
    import sys
    from os import path
    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline({
        "connector": path.join(__dirname, "..", "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
