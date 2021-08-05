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

CARPETAS_DICT = {1: "01_INFORMACION_INSTITUCIONAL", 2: "02_CLIENTES_ATENDIDOS", 3: "03_SERVICIOS_BRINDADOS", 4: "04_PROYECTOS_DE_INVERSION_PUBLICA", 5: "05_EJECUCION_PRESUPUESTAL", 6: "06_RECURSOS_HUMANOS", 7: "07_PARTIDAS_ARANCELARIAS"}

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        k = 1
        df = {}
        for i in range(2,2 +1):
            for j in range(4, 4 + 1 ):
                file_dir = path.join(params["datasets"], "01_Informacion_ITP_red_CITE", "{}".format(CARPETAS_DICT[i]),"TABLA_0{}_N0{}.csv".format(i,j))
                df = pd.read_csv(file_dir, encoding="latin1")
                k = k + 1

        df= df[['cod_ciiu', 'anio', 'empresas', 'fecha']]

        df = df.rename(columns={'cod_ciiu' : 'class_id'})

        df['class_id'] = df['class_id'].replace({"NO DETERMINADO" : "00000"})

        df['class_id'] = df['class_id'].str[:-1].astype(str)

        df['empresas'] = df['empresas'].astype(float)

        df['fecha_actualizacion'] = df['fecha'].str[-4:] + df['fecha'].str[3:5]
        df['fecha_actualizacion'] = df['fecha_actualizacion'].astype(int)

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
            'fecha_actualizacion':   'UInt32'
         }

        transform_step = TransformStep()
        agg_step = AggregatorStep('itp_cite_empresas_ciiu_agg', measures=['empresas'])
        load_step = LoadStep('itp_cite_empresas_ciiu_agg', connector=db_connector, if_exists='drop', pk=['class_id'], dtype=dtypes, nullable_list=['empresas'])

        return [transform_step, load_step]


def run_pipeline(params: dict):
    pp = CiteEmpresas2Pipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
