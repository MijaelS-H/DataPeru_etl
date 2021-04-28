import numpy as np
import pandas as pd
import os
from os import path
from unidecode import unidecode
from functools import reduce
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep

CARPETAS_DICT = {
    1: "01_INFORMACIÓN_INSTITUCIONAL",
    2: "02_CLIENTES_ATENDIDOS",
    3: "03_SERVICIOS_BRINDADOS",
    4: "04_PROYECTOS_DE_INVERSION_PUBLICA",
    5: "05_EJECUCION_PRESUPUESTAL",
    6: "06_RECURSOS_HUMANOS",
    7: "07_PARTIDAS_ARANCELARIAS",
}

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        file_dir = path.join(params["datasets"], "01_Informacion_ITP_red_CITE", "02_CLIENTES_ATENDIDOS", "TABLA_02_N05.csv")
        df = pd.read_csv(file_dir)

        df["tipo_contribuyente"] = df["tipo_contribuyente"].str.capitalize() 
        contribuyente_list = list(df["tipo_contribuyente"].unique())

        # contribuyente_map = {k:v for (k,v) in zip(sorted(contribuyente_list), list(range(1, len(contribuyente_list) +1)))}
        df = pd.DataFrame({ "contribuyente_id": list(range(1, len(contribuyente_list) + 1)),"contribuyente": sorted(contribuyente_list)})

        return df

class ContribuyenteCitePipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtype = {
            'contribuyente_id':                 'UInt8',
            'contribuyente':                    'String'
        }

        transform_step = TransformStep()
        load_step = LoadStep("dim_shared_cite_contribuyente", db_connector, if_exists="drop", pk=["contribuyente_id"], dtype=dtype)

        return [transform_step, load_step]

def run_pipeline(params: dict):
    pp = ContribuyenteCitePipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
