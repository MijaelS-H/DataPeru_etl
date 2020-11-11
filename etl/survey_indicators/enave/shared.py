
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import PipelineStep
from bamboo_lib.helpers import query_to_df
from .static import CATEGORY_REPLACE, GEO_REPLACE, INDUSTRY_REPLACE, SIZE_REPLACE

class ReplaceStep(PipelineStep):
    def run_step(self, prev, params):

        df = prev

        # replace dims
        df['categoría'].replace(CATEGORY_REPLACE, inplace=True)
        df['categoría'].fillna('Undefined', inplace=True)

        category_dim = dict(zip(df['categoría'].unique(), range(1, len(df['categoría'].unique()) + 1 )))
        df['categoría'].replace(category_dim, inplace=True)

        indicator_dim = dict(zip(df['indicador'].unique(), range(1, len(df['indicador'].unique()) + 1 )))
        df['indicador'].replace(indicator_dim, inplace=True)

        df['industry_id'].replace(INDUSTRY_REPLACE, inplace=True)

        df['size_id'].replace(SIZE_REPLACE, inplace=True)

        dim_geo_query = 'SELECT department_id, department_name FROM dim_shared_ubigeo_department'

        db_connector = Connector.fetch('clickhouse-database', open(params['connector']))

        dim_geo = query_to_df(db_connector, raw_query=dim_geo_query)

        df['department_id'] = df['department_id'].str.capitalize()

        df['department_id'].replace(GEO_REPLACE, inplace=True)

        df['department_id'].replace(dict(zip(dim_geo['department_name'], dim_geo['department_id'])), inplace=True)

        return df, category_dim, indicator_dim