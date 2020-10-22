from bamboo_lib.models import PipelineStep
from static import DISTRICT_REPLACE_2

class ReplaceStep(PipelineStep):
    def run_step(self, prev, params):

        df = prev

       # replace dims
        df['district_name'].replace(DISTRICT_REPLACE_2, inplace=True)

        # replace
        tipo_constitucion_dim = dict(zip(df['tipo_constitucion_id'].dropna().unique(), range(1, len(df['tipo_constitucion_id'].dropna().unique()) + 1 )))
        df['tipo_constitucion_id'].replace(tipo_constitucion_dim, inplace=True)

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

        return df, tipo_constitucion_dim, razon_social_dim, actividad_1_dim, actividad_2_dim, actividad_3_dim,actividad_4_dim