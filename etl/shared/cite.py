import numpy as np
import pandas as pd
from unidecode import unidecode
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep

TYPE_DICT = {
    'Centro de Innovación Productiva y Transferencia Tecnológica (CITE)' : 'CITE',
    'Unidad Técnica (UT)' : 'UT'
}

TYPE_ID = {
     'CITE' : 1,
     'UT' : 2
}


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_csv('../../data/01. Información ITP red CITE  (01-10-2020)/01 INFORMACIÓN INSTITUCIONAL/TABLA_01_N01.csv')

        df['cite_slug'] = df['cite'].str.replace("CITE","")
        df['cite_slug'] = df['cite_slug'].str.replace("UT","")
        df['cite_slug'] = df['cite_slug'].str.lower()
        df['cite_slug'] = df['cite_slug'].apply(unidecode)
        df['cite_slug'] = df['cite_slug'].str.replace(" ", "_")


        df['cite_name'] = df['cite'].str.replace("CITE","")
        df['cite_name'] = df['cite_name'].str.replace("UT","")
        df['cite_name'] = df['cite_name'].str.capitalize()
        df['cite_name'] = df['cite_name'].apply(unidecode)

        df['cite_id'] = df['cite'].index + 1

        df = df[['cite_id','cite_slug','cite_name']]


        return df

class CitePipeline(EasyPipeline):
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
            'cite_id': 'String',
            'cite_slug': 'String',
            'cite_name': 'String',
        }

        transform_step = TransformStep()

        load_step = LoadStep(
            "dim_shared_cite", db_connector, if_exists="drop", pk=["cite_id"], dtype=dtype)

        if params.get("ingest")==True:
            steps = [transform_step, load_step]
        else:
            steps = [transform_step]

        return steps

if __name__ == "__main__":

    cite_pipeline = CitePipeline()
    cite_pipeline.run(
        {
            "output-db": "clickhouse-local",
            "ingest": False
        }
    )