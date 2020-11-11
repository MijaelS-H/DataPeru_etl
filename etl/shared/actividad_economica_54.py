import pandas as pd
import nltk
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from etl.helpers import format_text

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        # Read econimic activity excel file
        df = pd.read_excel("https://docs.google.com/spreadsheets/d/e/2PACX-1vThTmtmBix54JYBgx7p4dnd9iZwlAtp-gMndb2ZAGcpbrxSnjv8lFTo7XS09P-l9AphgGHsREaPGUst/pub?output=xlsx")

        # Adding 0's to ids
        df["actividad_economica_id"] = df["actividad_economica_id"].astype(str).str.zfill(2)
        df["sub_actividad_economica_id"] = df["sub_actividad_economica_id"].astype(str).str.zfill(4)

        return df

class ActividadEconomica54Pipeline(EasyPipeline):
    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtype = {
            "actividad_economica":                      "String",
            "actividad_economica_id":                   "String",
            "sub_actividad_economica":                  "String",
            "sub_actividad_economica_id":               "String",
        }

        transform_step = TransformStep()
        load_step = LoadStep('dim_shared_actividad_economica_54', db_connector, if_exists='drop', pk=['actividad_economica_id'],
                            dtype=dtype)

        return [transform_step, load_step]

def run_pipeline(params: dict):
    pp = ActividadEconomica54Pipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
