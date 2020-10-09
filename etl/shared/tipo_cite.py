import numpy as np
import pandas as pd
from unidecode import unidecode
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep



class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        tipo_list = ['CITE','UT']
        tipo_id_list = [1,2]

        df = pd.DataFrame({ "tipo_id": tipo_id_list,"tipo": tipo_list})
    
        return df

class TipoPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter("output-db", dtype=str),
            Parameter("ingest", dtype=bool)
        ]
    
    
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'tipo_id':                              'UInt8',
            'tipo':                                'String',
        }

        transform_step = TransformStep()

        load_step = LoadStep(
            "dim_shared_tipo_cite", db_connector, if_exists="drop", pk=["tipo_id"], dtype=dtype)

        if params.get("ingest")==True:
            steps = [transform_step, load_step]
        else:
            steps = [transform_step]

        return steps

if __name__ == "__main__":

    tipo_pipeline = TipoPipeline()
    tipo_pipeline.run(
        {
            "output-db": "clickhouse-local",
            "ingest": False
        }
    )