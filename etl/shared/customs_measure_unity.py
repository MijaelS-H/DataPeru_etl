import pandas as pd
import nltk
from os import path
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from etl.helpers import format_text

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        # Read Customs unities file
        df = pd.read_csv(path.join(params["datasets"],"anexos", "UnidMedida.txt"),  sep='\t')

        # Rename file columns
        df = df.reset_index()
        df.rename(columns={
            'level_0': 'measure_unit_id',
            'level_1': 'measure_unit_name'
            }, 
            inplace=True
        )

        # Select usable columns
        df = df[['measure_unit_id', 'measure_unit_name']].copy()
        df['measure_unit_id'] = df['measure_unit_id'].str.strip()

        # Format text columns
        text_cols = ['measure_unit_name']

        nltk.download('stopwords')
        stopwords_es = nltk.corpus.stopwords.words('spanish')
        df = format_text(df, text_cols, stopwords=stopwords_es)

        df.replace({
            'Unidad Po 10**6': 'Unidad por 10**6'
            }, 
            inplace=True
        )

        return df

class CustomsMeasuresPipeline(EasyPipeline):
    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtype = {
            'measure_unit_id':       'String',
            'measure_unit_name':     'String',
        }

        transform_step = TransformStep()
        load_step = LoadStep('dim_shared_customs_unities', db_connector, if_exists='drop', pk=['measure_unit_id'], 
            dtype=dtype, engine='ReplacingMergeTree')

        return [transform_step, load_step]

def run_pipeline(params: dict):
    pp = CustomsMeasuresPipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
