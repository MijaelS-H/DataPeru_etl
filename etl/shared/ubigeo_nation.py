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

class UbigeoPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'nation_id': 'String',
            'nation_name': 'String'
        }

        transform_step = TransformStep()

        load_step = LoadStep(
            "dim_shared_ubigeo_nation", db_connector, if_exists="drop", pk=["nation_id"], dtype=dtype)

        return [transform_step, load_step]

if __name__ == "__main__":

    pp = UbigeoPipeline()
    pp.run({})