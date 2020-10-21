from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import PipelineStep
from bamboo_lib.helpers import query_to_df
from static import SECTOR_DICT, LOCATION_DICT

class ReplaceStep(PipelineStep):
    def run_step(self, prev, params):

        df = prev

        # replace dims
        category_dim = dict(zip(df['categoría'].unique(), range(1, len(df['categoría'].unique()) + 1 )))
        df['categoría'].replace(category_dim, inplace=True)

        indicator_dim = dict(zip(df['indicador'].unique(), range(1, len(df['indicador'].unique()) + 1 )))
        df['indicador'].replace(indicator_dim, inplace=True)

        df['sector_type_id'] = df['sector_type_id'].replace(SECTOR_DICT)
        df['location_type_id'] = df['location_type_id'].replace(LOCATION_DICT)
        
        dim_department_query = 'SELECT department_id, department_name FROM dim_shared_ubigeo_department'

        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dim_department = query_to_df(db_connector, raw_query=dim_department_query)
        dim_department.drop_duplicates(subset=['department_id', 'department_name'], inplace=True)

        df['department_id'].replace(dict(zip(dim_department['department_name'], dim_department['department_id'])), inplace=True)

        return df, category_dim, indicator_dim