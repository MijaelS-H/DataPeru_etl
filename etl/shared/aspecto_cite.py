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
        for i in range(4,4 +1):
            path, dirs, files = next(os.walk("../../data/01. Información ITP red CITE  (01-10-2020)/{}/".format(CARPETAS_DICT[i])))
            file_count = len(files)


            for j in range(1, file_count):
                file_dir = "../../data/01. Información ITP red CITE  (01-10-2020)/{}/TABLA_0{}_N0{}.csv".format(CARPETAS_DICT[i],i,j)

                df[k] = pd.read_csv(file_dir)
                k = k + 1

        df_list = [df[i] for i in range(1,3)]
        df = reduce(lambda df1,df2: pd.merge(df1,df2,on=['cite'],how='outer'), df_list)

        aspecto_list = list(df["aspecto"].unique())
        aspecto_map = {k:v for (k,v) in zip(sorted(aspecto_list), list(range(len(aspecto_list))))}

        df = pd.DataFrame({"aspecto_id": list(range(len(aspecto_list))),"aspecto": sorted(aspecto_list)})

    
 
        return df

class AspectoPipeline(EasyPipeline):
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
            'aspecto_id':       'UInt8',
            'aspecto':          'String',
        }

        transform_step = TransformStep()

        load_step = LoadStep(
            "dim_shared_cite_aspecto", db_connector, if_exists="drop", pk=["aspecto_id"], dtype=dtype)

        if params.get("ingest")==True:
            steps = [transform_step, load_step]
        else:
            steps = [transform_step]

        return steps

if __name__ == "__main__":

    aspecto_pipeline = AspectoPipeline()
    aspecto_pipeline.run(
        {
            "output-db": "clickhouse-local",
            "ingest": False
        }
    )