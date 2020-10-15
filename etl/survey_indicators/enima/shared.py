from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import PipelineStep
from bamboo_lib.helpers import query_to_df
from static import INDUSTRY_EXCEPTIONS 

class ReplaceStep(PipelineStep):
    def run_step(self, prev, params):

        df = prev

        # replace dims
        category_dim = dict(zip(df['categoría'].unique(), range(1, len(df['categoría'].unique()) + 1 )))
        df['categoría'].replace(category_dim, inplace=True)

        indicator_dim = dict(zip(df['indicador'].unique(), range(1, len(df['indicador'].unique()) + 1 )))
        df['indicador'].replace(indicator_dim, inplace=True)

        dim_industry_query = 'SELECT division_id, division_name FROM dim_shared_ciiu'

        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dim_industry = query_to_df(db_connector, raw_query=dim_industry_query)
        dim_industry.drop_duplicates(subset=['division_id', 'division_name'], inplace=True)

        df['industry_id'].replace(dict(zip(dim_industry['division_name'], dim_industry['division_id'])), inplace=True)

        #df['industry_id'].replace(INDUSTRY_REPLACE, inplace=True)
        df['industry_id'].replace(INDUSTRY_EXCEPTIONS,  inplace=True)

        return df, category_dim, indicator_dim