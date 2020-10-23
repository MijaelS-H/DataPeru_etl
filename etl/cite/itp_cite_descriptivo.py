import numpy as np
import pandas as pd
import os
from functools import reduce
from unidecode import unidecode
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import grab_connector

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
            path, dirs, files = next(os.walk("../../../datasets/20201001/01. Información ITP red CITE  (01-10-2020)/{}/".format(CARPETAS_DICT[i])))
            file_count = len(files)
    
            for j in [1,5,6,7]:
                file_dir = "../../../datasets/20201001/01. Información ITP red CITE  (01-10-2020)/{}/TABLA_0{}_N0{}.csv".format(CARPETAS_DICT[i],i,j)

                df[k] = pd.read_csv(file_dir)
                k = k + 1

        i = 4
        j = 1
        df[5] = pd.read_csv("../../../datasets/20201001/01. Información ITP red CITE  (01-10-2020)/{}/TABLA_0{}_N0{}.csv".format(CARPETAS_DICT[i],i,j))
        
        df[1]['tipo'] = df[1]['tipo'].replace({'Centro de Innovación Productiva y Transferencia Tecnológica (CITE)' : 'CITE',
       'Unidad Técnica (UT)': 'UT'})

        df[3] = df[3][['cite','ubigeo','latitud','longitud']]

        df_list = [df[i] for i in range(1,5)]
        df = reduce(lambda df1,df2: pd.merge(df1,df2,on=['cite'],how='outer'), df_list)

        df['descriptivo'] = df['descriptivo'].str.replace("UT",'')
        df['descriptivo'] = df['descriptivo'].str.replace('El ','')
        df['descriptivo'] = df['descriptivo'].str.replace('CITE','')
        df['descriptivo'] = df['descriptivo'].str.replace('La ','')
        df['descriptivo'] = df['descriptivo'].str.lstrip()
        df['descriptivo'] = df['descriptivo'].str.capitalize()
    
        df['district_id'] = df['ubigeo'].astype(str).str.zfill(6)
        df['cite_id'] = df['cite'].index + 1
        
        cadena_atencion_list = list(df["cadena_atencion"].unique())
        cadena_atencion_map = {k:v for (k,v) in zip(sorted(cadena_atencion_list), list(range(1, len(cadena_atencion_list) +1)))}
        df['cadena_atencion_id'] = df["cadena_atencion"].map(cadena_atencion_map).astype(int)
        
        df['cite'] = df['cite'].str.replace("CITE","")
        df['cite_slug'] = df['cite'].str.replace("CITE","")

        df['cite'] = df['cite'].str.replace("UT","")
        df['cite'] = df['cite'].str.capitalize()
        df['cite'] = df['cite'].apply(unidecode)
        
        df['cite_slug'] = df['cite_slug'].str.replace("UT","")
        df['cite_slug'] = df['cite_slug'].str.lower()
        df['cite_slug'] = df['cite_slug'].apply(unidecode)
        df['cite_slug'] = df['cite_slug'].str.replace(" ", "_")

        df['tipo_id'] = df['tipo'].map({"CITE": 1, "UT" : 2})

        df = df[['cite_id','cite_slug','tipo_id',
       'district_id','cadena_atencion_id','latitud', 'longitud', 'descriptivo']].copy()
        
        df['cantidad_cite'] = 1
        df[['cite_id','tipo_id','cadena_atencion_id']] =  df[['cite_id','tipo_id','cadena_atencion_id']].astype(int) 
        df[['district_id', 'latitud', 'longitud', 'descriptivo']] = df[['district_id', 'latitud', 'longitud', 'descriptivo']] .astype(str)

      
        return df

class CiteInfoPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter("output-db", dtype=str),
            Parameter("ingest", dtype=bool)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtypes = {
            'cite_id':               'UInt8',  
            'tipo_id':               'UInt8',
            'cadena_atencion_id':    'UInt8',
            'cite_slug' :            'String',
            'district_id':           'String',  
            'latitud':               'String',  
            'longitud':              'String',  
            'descriptivo':           'String',
            'cantidad_cite':         'UInt8',  
         }

        transform_step = TransformStep()  
        load_step = LoadStep(
          'itp_cite_descriptivo', connector=db_connector, if_exists='drop',
          pk=['cite_id'], dtype=dtypes, nullable_list=[])

        if params.get("ingest")==True:
            steps = [transform_step, load_step]
        else:
            steps = [transform_step]

        return steps

if __name__ == "__main__":
    cite_info_pipeline = CiteInfoPipeline()
    cite_info_pipeline.run(
        {
            "output-db": "clickhouse-local",
            "ingest": True
        }
    )