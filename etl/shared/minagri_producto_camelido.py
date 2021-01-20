import pandas as pd
from os import path
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import LoadStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_csv(
            path.join(params["datasets"], "20210119", "08 CADENAS PRODUCTIVAS Y MERCADO INTERNO", "TABLA_08_N04.csv"),
            dtype='str',
            sep=";",
            encoding="latin-1"
        )

        df.rename(columns={
            "descripcion_producto ": "descripcion_producto"
        }, inplace=True)

        df = df[["codigo_producto", "descripcion_producto"]].drop_duplicates().dropna()

        return df

class MinagriProductoCamelido(EasyPipeline):
    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtype = {
            'codigo_producto':          'String',
            'descripcion_producto':     'String',
        }

        transform_step = TransformStep()
        load_step = LoadStep('dim_shared_minagri_producto_camelido', db_connector, if_exists='drop', pk=['codigo_producto'], dtype=dtype)

        return [transform_step, load_step]

def run_pipeline(params: dict):
    pp = MinagriProductoCamelido()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })