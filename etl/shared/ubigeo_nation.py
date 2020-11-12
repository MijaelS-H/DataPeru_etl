import numpy as np
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.DataFrame({'nation_id': ['per'], 'nation_name': ['Perú']})

        return df

class Ubigeo_Nation_Pipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtype = {
            'nation_id': 'String',
            'nation_name': 'String'
        }

        transform_step = TransformStep()

        load_step = LoadStep(
            "dim_shared_ubigeo_nation", db_connector, if_exists="drop", pk=["nation_id"], dtype=dtype)

        return [transform_step, load_step]

def run_pipeline(params: dict):
    pp = Ubigeo_Nation_Pipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
