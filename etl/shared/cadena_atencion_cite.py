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
        for i in range(1,1 +1):
            path, dirs, files = next(os.walk("../../data/01. Información ITP red CITE  (01-10-2020)/{}/".format(CARPETAS_DICT[i])))
            file_count = len(files)


            for j in [1,5,6,7]:
                file_dir = "../../data/01. Información ITP red CITE  (01-10-2020)/{}/TABLA_0{}_N0{}.csv".format(CARPETAS_DICT[i],i,j)

                df[k] = pd.read_csv(file_dir)
                k = k + 1
        
        df_list = [df[i] for i in range(1,4)]
        df = reduce(lambda df1,df2: pd.merge(df1,df2,on=['cite'],how='outer'), df_list)

        cadenas = df[['cadena_resolucion','cadena_pip','cadena_atencion']]

        cadena_resolucion_list = list(cadenas["cadena_resolucion"].unique())
        cadena_resolución_map = {k:v for (k,v) in zip(sorted(cadena_resolucion_list), list(range(1, len(cadena_resolucion_list) +1)))}
        
        cadena_pip_list = list(cadenas["cadena_pip"].unique())
        cadena_pip_map = {k:v for (k,v) in zip(sorted(cadena_pip_list), list(range(1, len(cadena_pip_list) +1)))}

        cadena_atencion_list = list(cadenas["cadena_atencion"].unique())
        cadena_atencion_map = {k:v for (k,v) in zip(sorted(cadena_atencion_list), list(range(1, len(cadena_atencion_list) +1)))}
        
        cadenas['cadena_resolucion_id'] = cadenas['cadena_resolucion'].map(cadena_resolución_map).astype(str)
        cadenas['cadena_pip_id'] = cadenas['cadena_pip'].map(cadena_pip_map).astype(str)
        cadenas['cadena_atencion_id'] = cadenas['cadena_atencion'].map(cadena_atencion_map).astype(str)
        
        cadenas['cadena_resolucion_id_temp'] = cadenas['cadena_resolucion_id'].str.zfill(2)
        cadenas['cadena_pip_id_temp'] = cadenas['cadena_pip_id'].str.zfill(2)
        cadenas['cadena_atencion_id_temp'] = cadenas['cadena_atencion_id'].str.zfill(2)

        cadenas['cadena_resolucion_id'] = (cadenas['cadena_resolucion_id_temp']).astype(int) 
        cadenas['cadena_pip_id'] = (cadenas['cadena_resolucion_id_temp'] + cadenas['cadena_pip_id_temp']).astype(int)  
        cadenas['cadena_atencion_id']  = (cadenas['cadena_pip_id'].astype(str)  + cadenas['cadena_atencion_id_temp']).astype(int)  

        df = cadenas[['cadena_resolucion','cadena_resolucion_id','cadena_pip','cadena_pip_id','cadena_atencion','cadena_atencion_id']]


    
        return df

class TipoPipeline(EasyPipeline):
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
            'cadena_atencion_id':   'UInt8',
            'cadena_atencion':      'String',
        }

        transform_step = TransformStep()

        load_step = LoadStep(
            "dim_shared_cadena_atencion", db_connector, if_exists="drop", pk=["cadena_atencion_id"], dtype=dtype)

        if params.get("ingest")==True:
            steps = [transform_step, load_step]
        else:
            steps = [transform_step]

        return steps

if __name__ == "__main__":

    tipo_pipeline = TipoPipeline()
    tipo_pipeline.run(
        {
            "output-db": "clickhouse-local",
            "ingest": False
        }
    )