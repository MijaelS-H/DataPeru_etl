
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import PipelineStep
from bamboo_lib.helpers import query_to_df

class ReplaceStep(PipelineStep):
    def run_step(self, prev, params):

        df = prev

        # replace dims
        category_dim = dict(zip(df['categoría'].dropna().unique(), range(1, len(df['categoría'].unique()) + 1 )))
        df['categoría'].replace(category_dim, inplace=True)
        
        indicator_dim = dict(zip(df['indicador'].dropna().unique(), range(1, len(df['indicador'].unique()) + 1 )))
        df['indicador'].replace(indicator_dim, inplace=True)
        
        size_dim = dict(zip(df['size_id'].dropna().unique(), range(1, len(df['size_id'].unique()) + 1 )))
        df['size_id'].replace(size_dim, inplace=True)
        
        workforce_dim = dict(zip(df['workforce_id'].dropna().unique(), range(1, len(df['workforce_id'].unique()) + 1 )))
        df['workforce_id'].replace(workforce_dim, inplace=True)
        
        dim_industry_query = 'SELECT section_id, section_name FROM dim_shared_ciiu'

        db_connector = Connector.fetch('clickhouse-database', open(params['connector']))

        dim_industry = query_to_df(db_connector, raw_query=dim_industry_query)
        dim_industry.drop_duplicates(subset=['section_id', 'section_name'], inplace=True)

        df['industry_id'].replace(dict(zip(dim_industry['section_name'], dim_industry['section_id'])), inplace=True)

        return df, category_dim, indicator_dim