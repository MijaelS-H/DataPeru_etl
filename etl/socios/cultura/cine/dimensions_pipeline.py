import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep
from etl.socios.cultura.cine.cultura_cine import TransformStep
from etl.socios.cultura.cine.shared import ReplaceStep

class ProcessingStep(PipelineStep):
    def run_step(self, prev, params):
        df, tipo_constitucion_dim, razon_social_dim, actividad_1_dim, actividad_2_dim, actividad_3_dim,actividad_4_dim,actividad_5_dim = prev

        if params.get('pk') == 'razon_social_id':
            df = pd.DataFrame.from_dict(razon_social_dim, orient='index').reset_index()
            df.columns = ['razon_social_name', 'razon_social_id']

            return df

class DimCulturaPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return[
            Parameter(name='pk', dtype=str),
            Parameter(name='table_name', dtype=str)
        ]

    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtype = {
            params.get('pk'): 'UInt16'
        }

        transform_step = TransformStep()
        replace_step = ReplaceStep(connector=db_connector)
        processing_step = ProcessingStep()
        load_step = LoadStep(params.get('table_name'), db_connector, if_exists='drop', pk=[params.get('pk')], dtype=dtype)

        return [transform_step, replace_step, processing_step, load_step]


def run_pipeline(params: dict):
    pp = DimCulturaPipeline()
    levels = {'razon_social_id': 'dim_cine_razon_social'}

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
