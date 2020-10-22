from bamboo_lib.models import PipelineStep
from static import DISTRICT_REPLACE_1

class ReplaceStep(PipelineStep):
    def run_step(self, prev, params):

        df = prev

       # replace dims
        df['district_name'].replace(DISTRICT_REPLACE_1, inplace=True)

        # replace
        asociacion_dim = dict(zip(df['codigo_asociacion'], df['asociacion_name']))

        actividad_n_1_dim = dict(zip(df['actividad_n_1_id'].dropna().unique(), range(1, len(df['actividad_n_1_id'].dropna().unique()) + 1 )))
        df['actividad_n_1_id'].replace(actividad_n_1_dim, inplace=True)

        actividad_n_2_dim = dict(zip(df['actividad_n_2_id'].dropna().unique(), range(1, len(df['actividad_n_2_id'].dropna().unique()) + 1 )))
        df['actividad_n_2_id'].replace(actividad_n_2_dim, inplace=True)

        manifestacion_n_1_dim = dict(zip(df['manifestacion_n_1_id'].dropna().unique(), range(1, len(df['manifestacion_n_1_id'].dropna().unique()) + 1 )))
        df['manifestacion_n_1_id'].replace(manifestacion_n_1_dim, inplace=True)

        manifestacion_n_2_dim = dict(zip(df['manifestacion_n_2_id'].dropna().unique(), range(1, len(df['manifestacion_n_2_id'].dropna().unique()) + 1 )))
        df['manifestacion_n_2_id'].replace(manifestacion_n_2_dim, inplace=True)

        manifestacion_n_3_dim = dict(zip(df['manifestacion_n_3_id'].dropna().unique(), range(1, len(df['manifestacion_n_3_id'].dropna().unique()) + 1 )))
        df['manifestacion_n_3_id'].replace(manifestacion_n_3_dim, inplace=True)

        inscrita_sunarp_dim = dict(zip(df['inscrita_sunarp_id'].dropna().unique(), range(1, len(df['inscrita_sunarp_id'].dropna().unique()) + 1 )))
        df['inscrita_sunarp_id'].replace(inscrita_sunarp_dim, inplace=True)

        return df, asociacion_dim, actividad_n_1_dim, actividad_n_2_dim, manifestacion_n_1_dim, manifestacion_n_2_dim, manifestacion_n_3_dim, inscrita_sunarp_dim