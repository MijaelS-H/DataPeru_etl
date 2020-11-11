import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.DataFrame({
            'trade_flow_id': [1, 2],
            'trade_flow': ['Importaciones', 'Exportaciones']
        })

        return df

class TradeFlowPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))
        dtype = {
            'trade_flow_id':    'UInt8',
            'trade_flow':       'String'
        }

        transform_step = TransformStep()
        load_step = LoadStep("dim_shared_trade_flow", db_connector, if_exists="drop", pk=["trade_flow_id"], dtype=dtype)

        return [transform_step, load_step]

def run_pipeline(params: dict):
    pp = TradeFlowPipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
