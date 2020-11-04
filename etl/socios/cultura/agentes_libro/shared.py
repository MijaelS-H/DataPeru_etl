from bamboo_lib.models import PipelineStep
from static import DISTRICT_REPLACE_4
from bamboo_lib.helpers import query_to_df

class ReplaceStep(PipelineStep):
    def run_step(self, prev, params):

        df = prev

       # replace dims
        df['district_id'].replace(DISTRICT_REPLACE_4, inplace=True)
        dim_geo_query = 'SELECT district_id, district_name FROM dim_shared_ubigeo_district'
        dim_geo = query_to_df(self.connector, raw_query=dim_geo_query)
        df['district_id'].replace(dict(zip(dim_geo['district_name'], dim_geo['district_id'])), inplace=True)

        # replace
        razon_social_dim = dict(zip(df['razon_social_id'].dropna().unique(), range(1, len(df['razon_social_id'].dropna().unique()) + 1 )))
        df['razon_social_id'].replace(razon_social_dim, inplace=True)

        actividad_1_dim = dict(zip(df['actividad_1_id'].dropna().unique(), range(1, len(df['actividad_1_id'].dropna().unique()) + 1 )))
        df['actividad_1_id'].replace(actividad_1_dim, inplace=True)

        actividad_2_dim = dict(zip(df['actividad_2_id'].dropna().unique(), range(1, len(df['actividad_2_id'].dropna().unique()) + 1 )))
        df['actividad_2_id'].replace(actividad_2_dim, inplace=True)

        actividad_3_dim = dict(zip(df['actividad_3_id'].dropna().unique(), range(1, len(df['actividad_3_id'].dropna().unique()) + 1 )))
        df['actividad_3_id'].replace(actividad_3_dim, inplace=True)

        actividad_4_dim = dict(zip(df['actividad_4_id'].dropna().unique(), range(1, len(df['actividad_4_id'].dropna().unique()) + 1 )))
        df['actividad_4_id'].replace(actividad_4_dim, inplace=True)

        return df, razon_social_dim, actividad_1_dim, actividad_2_dim, actividad_3_dim, actividad_4_dim