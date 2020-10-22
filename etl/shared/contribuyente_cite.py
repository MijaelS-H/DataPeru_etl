import numpy as np
import pandas as pd
import os
from unidecode import unidecode
from functools import reduce
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep

CARPETAS_DICT = {
    1: "01 INFORMACIÓN INSTITUCIONAL",
    2: "02 CLIENTES ATENDIDOS",
    3: "03 SERVICIOS BRINDADOS",
    4: "04 PROYECTOS DE INVERSIÓN PÚBLICA",
    5: "05 EJECUCIÓN PRESUPUESTAL",
    6: "06 RECURSOS HUMANOS",
    7: "07 PARTIDAS ARANCELARIAS",
}

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        k = 1
        df = {}
        for i in range(2,2 +1):
            path, dirs, files = next(os.walk("../../../datasets/20201001/01. Información ITP red CITE  (01-10-2020)/{}/".format(CARPETAS_DICT[i])))
            file_count = len(files)


            for j in range(5, 5 + 1 ):
                file_dir = "../../../datasets/20201001/01. Información ITP red CITE  (01-10-2020)/{}/TABLA_0{}_N0{}.csv".format(CARPETAS_DICT[i],i,j)

                df = pd.read_csv(file_dir)
        
        df["tipo_contribuyente"] = df["tipo_contribuyente"].str.capitalize() 
        contribuyente_list = list(df["tipo_contribuyente"].unique())
        contribuyente_map = {k:v for (k,v) in zip(sorted(contribuyente_list), list(range(1, len(contribuyente_list) +1)))}
        
        df = pd.DataFrame({ "contribuyente_id": list(range(len(contribuyente_list))),"contribuyente": sorted(contribuyente_list)})

    
        return df

class ContribuyenteCitePipeline(EasyPipeline):
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
            'contribuyente_id':                 'UInt8',
            'contribuyente':                    'String',
        }

        transform_step = TransformStep()

        load_step = LoadStep(
            "dim_shared_cite_contribuyente", db_connector, if_exists="drop", pk=["contribuyente_id"], dtype=dtype)

        if params.get("ingest")==True:
            steps = [transform_step, load_step]
        else:
            steps = [transform_step]

        return steps

if __name__ == "__main__":

    contribuyente_pipeline = ContribuyenteCitePipeline()
    contribuyente_pipeline.run(
        {
            "output-db": "clickhouse-local",
            "ingest": True
        }
    )