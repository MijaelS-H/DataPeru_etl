from bamboo_lib.models import PipelineStep
from static import DISTRICT_REPLACE_3

class ReplaceStep(PipelineStep):
    def run_step(self, prev, params):

        df = prev

       # replace dims
        df['district_id'].replace(DISTRICT_REPLACE_3, inplace=True)

        # replace

        fase_cadena_valor_dim = dict(zip(df['fase_cadena_valor_id'].dropna().unique(), range(1, len(df['fase_cadena_valor_id'].dropna().unique()) + 1 )))
        df['fase_cadena_valor_id'].replace(fase_cadena_valor_dim, inplace=True)
        
        estimulo_economico_dim = dict(zip(df['estimulo_economico_id'].dropna().unique(), range(1, len(df['estimulo_economico_id'].dropna().unique()) + 1 )))
        df['estimulo_economico_id'].replace(estimulo_economico_dim, inplace=True)
        
        nombre_proyecto_dim = dict(zip(df['nombre_proyecto_id'].dropna().unique(), range(1, len(df['nombre_proyecto_id'].dropna().unique()) + 1 )))
        df['nombre_proyecto_id'].replace(nombre_proyecto_dim, inplace=True)

        tipo_postulante_dim = dict(zip(df['tipo_postulante_id'].dropna().unique(), range(1, len(df['tipo_postulante_id'].dropna().unique()) + 1 )))
        df['tipo_postulante_id'].replace(tipo_postulante_dim, inplace=True)

        postulante_dim = dict(zip(df['postulante_id'].dropna().unique(), range(1, len(df['postulante_id'].dropna().unique()) + 1 )))
        df['postulante_id'].replace(postulante_dim, inplace=True)

        estado_dim = dict(zip(df['estado_id'].dropna().unique(), range(1, len(df['estado_id'].dropna().unique()) + 1 )))
        df['estado_id'].replace(estado_dim, inplace=True)

        return df, estimulo_economico_dim, nombre_proyecto_dim, postulante_dim