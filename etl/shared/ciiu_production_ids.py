import pandas as pd
from os import path
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        # Read economic activity tsv file
        df = pd.read_csv(path.join(params["datasets"], "anexos", "CIIU_yearly_production.tsv"), sep="\t")

        for item in df.columns:
            df[item] = df[item].astype(str).str.strip()

        df = df.drop_duplicates()

        return df

class CIIU_Production_Pipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        
        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))

        dtype = {
            "division":                       "String",
            "division_id":                    "String",
            "group":                          "String",
            "group_id":                       "String",
            "product_name":                   "String",
            "product_id":                     "String"
        }

        transform_step = TransformStep()
        load_step = LoadStep("dim_shared_ciiu_production", db_connector, if_exists="drop", pk=["product_id"], dtype=dtype)

        return [transform_step, load_step]

def run_pipeline(params: dict):
    pp = CIIU_Production_Pipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
