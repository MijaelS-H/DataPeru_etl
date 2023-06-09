import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep
from etl.socios.cultura.asociaciones.cultura_asociaciones import TransformStep
from etl.socios.cultura.asociaciones.shared import ReplaceStep


class ProcessingStep(PipelineStep):
    def run_step(self, prev, params):
        df, asociacion_dim, actividad_n_1_dim, actividad_n_2_dim, manifestacion_n_1_dim, manifestacion_n_2_dim, manifestacion_n_3_dim, inscrita_sunarp_dim = prev

        if params.get('pk') == 'codigo_asociacion':
            df = pd.DataFrame.from_dict(asociacion_dim, orient='index').reset_index()
            df.columns = ['codigo_asociacion', 'asociacion_name']

            return df

        elif (params.get('pk') == 'manifestacion_n_1_id'):
            df = pd.DataFrame.from_dict(manifestacion_n_1_dim, orient='index').reset_index()
            df.columns = ['manifestacion_n_1_name', 'manifestacion_n_1_id']

            return df

        elif (params.get('pk') == 'manifestacion_n_2_id'):
            df = pd.DataFrame.from_dict(manifestacion_n_2_dim, orient='index').reset_index()
            df.columns = ['manifestacion_n_2_name', 'manifestacion_n_2_id']

            return df

        elif (params.get('pk') == 'manifestacion_n_3_id'):
            df = pd.DataFrame.from_dict(manifestacion_n_3_dim, orient='index').reset_index()
            df.columns = ['manifestacion_n_3_name', 'manifestacion_n_3_id']

            return df

class DimAsociacionesPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return[
            Parameter(name='pk', dtype=str),
            Parameter(name='table_name', dtype=str)
        ]

    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open(params['connector']))

        if (params['pk'] == 'codigo_asociacion'):
            dtype = {
                params.get('pk'): 'String'
            }
        else: 
            dtype = {
                params.get('pk'): 'UInt8'
            }
       
        transform_step = TransformStep()
        replace_step = ReplaceStep(connector=db_connector)
        processing_step = ProcessingStep()
        load_step = LoadStep(params.get('table_name'), db_connector, if_exists='drop', 
                             pk=[params.get('pk')], dtype=dtype)

        return [transform_step, replace_step, processing_step, load_step]

def run_pipeline(params: dict):
    pp = DimAsociacionesPipeline()
    levels = {'codigo_asociacion':    'dim_asociaciones_culturales',
                'manifestacion_n_1_id':  'dim_asociaciones_culturales_manifestacion_n_1',
                'manifestacion_n_2_id':  'dim_asociaciones_culturales_manifestacion_n_2',
                'manifestacion_n_3_id':  'dim_asociaciones_culturales_manifestacion_n_3'}

    for k, v in levels.items():
        pp_params = {'pk': k, 'table_name': v}
        pp_params.update(params)
        pp.run(pp_params)


if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "..", "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
