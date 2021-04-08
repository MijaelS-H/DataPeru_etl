
from bamboo_lib.models import PipelineStep
from bamboo_lib.helpers import query_to_df
from bamboo_lib.connectors.models import Connector

class ReplaceStep(PipelineStep):
    def run_step(self, prev, params):
        
        db_connector = Connector.fetch('clickhouse-database', open(params['connector']))

        df = prev

        # replace dims
        dim_geo_query = 'SELECT department_id, department_name FROM dim_shared_ubigeo_department'
        #dim_geo = query_to_df(self.connector, raw_query=dim_geo_query)
        dim_geo = query_to_df(db_connector, raw_query=dim_geo_query)
        df['department_id'].replace(dict(zip(dim_geo['department_name'], dim_geo['department_id'])), inplace=True)

        category_dim = dict(zip(df['categoría'].unique(), range(1, len(df['categoría'].unique()) + 1 )))
        df['categoría'].replace(category_dim, inplace=True)

        indicator_dim = dict(zip(df['indicador'].unique(), range(1, len(df['indicador'].unique()) + 1 )))
        df['indicador'].replace(indicator_dim, inplace=True)

        geo_dim = dict(zip(df['geo_id'].unique(), range(1, len(df['geo_id'].unique()) + 1 )))
        df['geo_id'].replace(geo_dim, inplace=True)

        region_dim = dict(zip(df['region_id'].unique(), range(1, len(df['region_id'].unique()) + 1 )))
        df['region_id'].replace(region_dim, inplace=True)

        return df, category_dim, indicator_dim, geo_dim, region_dim