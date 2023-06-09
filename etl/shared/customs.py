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

        # Read customs file
        df = pd.read_excel(path.join(params["datasets"], "anexos", "codigos_aduana.xls"), header=2)
        df.rename(columns={
            '    N°': 'aduana_id',
            '                      DESCRIPCIÓN': 'aduana'
        }, inplace=True)

        nltk.download('stopwords')
        stopwords_es = nltk.corpus.stopwords.words('spanish')
        df = format_text(df, ['aduana'], stopwords=stopwords_es)

        return df

class CustomsPipeline(EasyPipeline):
    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtype = {
            'aduana_id':        'UInt16',
            'aduana':           'String'
        }

        transform_step = TransformStep()
        load_step = LoadStep('dim_shared_aduanas', db_connector, if_exists='drop', pk=['aduana_id'], 
                            dtype=dtype, engine='ReplacingMergeTree')

        return [transform_step, load_step]

def run_pipeline(params: dict):
    pp = CustomsPipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
