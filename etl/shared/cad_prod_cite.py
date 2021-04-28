import numpy as np
import pandas as pd
import os
from os import path
from unidecode import unidecode
from functools import reduce
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_excel(path.join(params["datasets"], "01_Informacion_ITP_red_CITE", "07_PARTIDAS_ARANCELARIAS", "TABLA_08_N01.xlsx"))

        df['cadena_productiva'] = df['cadena_productiva'].str.strip()

        cadena_productiva_list = list(df["cadena_productiva"].unique())
        cadena_productiva_map = {k:v for (k,v) in zip(sorted(cadena_productiva_list), list(range(1, len(cadena_productiva_list) +1)))}
        df['cad_prod_id'] = df['cadena_productiva'].map(cadena_productiva_map)

        df = df[['cad_prod_id','cadena_productiva']]
        df = df.drop_duplicates()

        return df

class CiteCadenaProductivaPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtype = {
            'cad_prod_id':                  'UInt8',
            'cadena_productiva':            'String',
        }

        transform_step = TransformStep()
        load_step = LoadStep("dim_shared_cite_cad_prod", db_connector, if_exists="drop", pk=["cad_prod_id"], dtype=dtype)

        return [transform_step, load_step]

def run_pipeline(params: dict):
    pp = CiteCadenaProductivaPipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
