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
            path, dirs, files = next(os.walk("../../data/01. Información ITP red CITE  (01-10-2020)/{}/".format(CARPETAS_DICT[i])))
            file_count = len(files)


            for j in range(1, 1 + 1 ):
                file_dir = "../../data/01. Información ITP red CITE  (01-10-2020)/{}/TABLA_0{}_N0{}.csv".format(CARPETAS_DICT[i],i,j)

                df = pd.read_csv(file_dir)
                k = k + 1
        
        empresas_list = list(df["tipo"].unique())
        empresas_map = {k:v for (k,v) in zip(sorted(empresas_list), list(range(len(empresas_list))))}

        df = pd.DataFrame({ "tipo_empresa_id": list(range(len(empresas_list))),"tipo_empresa": sorted(empresas_list)})

    
        return df

class EmpresasCitePipeline(EasyPipeline):
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
            'tipo_empresa_id':              'UInt8',
            'tipo_empresa':                 'String',
        }

        transform_step = TransformStep()

        load_step = LoadStep(
            "dim_shared_empresas_cite", db_connector, if_exists="drop", pk=["tipo_empresa_id"], dtype=dtype)

        if params.get("ingest")==True:
            steps = [transform_step, load_step]
        else:
            steps = [transform_step]

        return steps

if __name__ == "__main__":

    empresas_cite_pipeline = EmpresasCitePipeline()
    empresas_cite_pipeline.run(
        {
            "output-db": "clickhouse-local",
            "ingest": False
        }
    )