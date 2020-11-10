import numpy as np
import pandas as pd
import os
from os import path
from functools import reduce
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import grab_connector
from etl.consistency import AggregatorStep

CARPETAS_DICT = {1: "01 INFORMACIÓN INSTITUCIONAL", 2: "02 CLIENTES ATENDIDOS", 3: "03 SERVICIOS BRINDADOS", 4: "04 PROYECTOS DE INVERSIÓN PÚBLICA", 5: "05 EJECUCIÓN PRESUPUESTAL", 6: "06 RECURSOS HUMANOS", 7: "07 PARTIDAS ARANCELARIAS"}


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        k = 1
        df = {}
        for i in range(2,2 +1):
            for j in range(3, 3 + 1 ):
                file_dir = path.join(params["datasets"], "20201001", "01. Información ITP red CITE  (01-10-2020)", "{}".format(CARPETAS_DICT[i]),"TABLA_0{}_N0{}.csv".format(i,j))
                df = pd.read_csv(file_dir)
                print(file_dir)
                k = k + 1

        df= df[['cod_ciiu', 'anio', 'empresas']]

        df = df.rename(columns={'cod_ciiu' : 'class_id'})

        df['class_id'] = df['class_id'].str[:-1].replace({"No determinado" : "0000"}).astype(str)

        df['empresas'] = df['empresas'].astype(float)

        return df

class CiteEmpresas2Pipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtypes = {
            'class_id':              'String',
            'anio':                  'UInt16',
            'empresas':              'Float32',
         }

        transform_step = TransformStep()
        agg_step = AggregatorStep('itp_cite_empresas_ciiu_agg', measures=['empresas'])
        load_step = LoadStep('itp_cite_empresas_ciiu_agg', connector=db_connector, if_exists='drop', pk=['class_id'], dtype=dtypes, nullable_list=['empresas'])

        return [transform_step, agg_step, load_step]


def run_pipeline(params: dict):
    pp = CiteEmpresas2Pipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
