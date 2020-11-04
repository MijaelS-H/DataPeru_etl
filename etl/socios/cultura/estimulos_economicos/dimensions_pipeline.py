import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep
from cultura_eec import TransformStep
from shared import ReplaceStep


class ProcessingStep(PipelineStep):
    def run_step(self, prev, params):
        df, estimulo_economico_dim, nombre_proyecto_dim, postulante_dim = prev

        if params.get('pk') == 'estimulo_economico_id':
            df = pd.DataFrame.from_dict(estimulo_economico_dim, orient='index').reset_index()
            df.columns = ['estimulo_economico_name', 'estimulo_economico_id']

            return df

        elif params.get('pk') == 'nombre_proyecto_id':
            df = pd.DataFrame.from_dict(nombre_proyecto_dim, orient='index').reset_index()
            df.columns = ['nombre_proyecto_name', 'nombre_proyecto_id']

            df['nombre_proyecto_name'] = df['nombre_proyecto_name'].astype(str)
        
            return df

        elif params.get('pk') == 'postulante_id':
            df = pd.DataFrame.from_dict(postulante_dim, orient='index').reset_index()
            df.columns = ['postulante_name', 'postulante_id']

            return df

class DimEECPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return[
            Parameter(name='pk', dtype=str),
            Parameter(name='table_name', dtype=str)
        ]

    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open('../../conns.yaml'))

        if (k == 'estimulo_economico_id'):
            dtype = {
                params.get('pk'): 'UInt16'
            }
        else: 
            dtype = {
                params.get('pk'): 'UInt16'
            }
       
        transform_step = TransformStep()
        replace_step = ReplaceStep(connector=db_connector)
        processing_step = ProcessingStep()
        load_step = LoadStep(params.get('table_name'), db_connector, if_exists='drop', 
                             pk=[params.get('pk')], dtype=dtype)

        return [transform_step, replace_step, processing_step, load_step]

if __name__ == "__main__":
    pp = DimEECPipeline()
    for k, v in {'estimulo_economico_id': 'dim_eec_estimulo_economico',
                'nombre_proyecto_id': 'dim_eec_nombre_proyecto',
                'postulante_id': 'dim_eec_postulante'
                 }.items():
        pp.run({'pk': k,
                'table_name': v})
