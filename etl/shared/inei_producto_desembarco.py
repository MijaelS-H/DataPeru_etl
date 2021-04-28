import pandas as pd
from os import path
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import LoadStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        with open(path.join(params["datasets"], "01_Informacion_ITP_red_CITE", "08_CADENAS_PRODUCTIVAS_Y_MERCADO_INTERNO", "TABLA_08_N02.csv"), 'r', encoding='latin-1') as f:
            df = pd.read_csv(
                f,
                dtype='str',
                sep=";"
            )

        df.rename(columns={
            "codigo_producto ": "codigo_producto"
        }, inplace=True)

        df = df[["codigo_producto", "descripcion_producto"]].drop_duplicates().dropna()

        return df

class IneiProductoDesembarco(EasyPipeline):
    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtype = {
            'codigo_producto':          'String',
            'descripcion_producto':     'String',
        }

        transform_step = TransformStep()
        load_step = LoadStep('dim_shared_inei_producto_desembarco', db_connector, if_exists='drop', pk=['codigo_producto'], dtype=dtype)

        return [transform_step, load_step]

def run_pipeline(params: dict):
    pp = IneiProductoDesembarco()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
