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

TIPO_CITE_DICT = {'Centro de Innovación Productiva y Transferencia Tecnológica (CITE)' : 'CITE',
                  'Unidad Técnica (UT)': 'UT'}

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        k = 1
        df = {}
        for i in range(1,1 +1):
            path, dirs, files = next(os.walk("../../../datasets/20201001/01. Información ITP red CITE  (01-10-2020)/01 INFORMACIÓN INSTITUCIONAL/"))
            file_count = len(files)
    
            for j in range(1, file_count + 1 ):
                file_dir = "../../../datasets/20201001/01. Información ITP red CITE  (01-10-2020)/01 INFORMACIÓN INSTITUCIONAL/TABLA_0{}_N0{}.csv".format(i,j)
                df[k] = pd.read_csv(file_dir)
                k = k + 1

        df_list = [df[i] for i in range(1, file_count + 1)]
        df = reduce(lambda df1,df2: pd.merge(df1,df2,on=['cite'],how='outer'), df_list)

        df = df[['cite', 'tipo', 'director', 'coordinador_ut', 'lista_miembros','ambito',
                  'ubigeo', 'direccion', 'latitud', 'longitud', 'cadena_atencion','cadena_pip','cadena_resolucion','descriptivo']]

        df['tipo'] = df['tipo'].replace(TIPO_CITE_DICT)
        df['coordinador_ut'] = df['coordinador_ut'].fillna("No disponible")
        df['descriptivo'] = df['descriptivo'].str.replace("UT",'')
        df['descriptivo'] = df['descriptivo'].str.replace('El ','')
        df['descriptivo'] = df['descriptivo'].str.replace('CITE','')
        df['descriptivo'] = df['descriptivo'].str.replace('La ','')
        df['descriptivo'] = df['descriptivo'].str.lstrip()
        df['descriptivo'] = df['descriptivo'].str.capitalize()

        cite_list = list(df['cite'].unique())
        cite_map = {k:v for (k,v) in zip(sorted(cite_list), list(range(1, len(cite_list) +1)))}
        df['cite_id'] = df['cite'].map(cite_map)
        df['cite'] = df['cite'].str.replace("CITE","")
        df['cite_slug'] = df['cite'].str.replace("CITE","")

        df['cite'] = df['cite'].str.replace("UT","")
        df['cite'] = df['cite'].str.capitalize()
        df['cite'] = df['cite'].apply(unidecode)
        df['cite_slug'] = df['cite_slug'].str.replace("UT","")
        df['cite_slug'] = df['cite_slug'].str.lower()
        df['cite_slug'] = df['cite_slug'].apply(unidecode)
        df['cite_slug'] = df['cite_slug'].str.replace(" ", "_")

        df['director'] = df['director'].str.title()
        df['lista_miembros'] = df['lista_miembros'].str.replace('\n•',',')
        df['lista_miembros'] = df['lista_miembros'].str.replace('• ','')


        
        df.rename(columns={'ubigeo' : 'district_id'}, inplace = True)

        return df


class FormatStep(PipelineStep):
    def run_step(self, prev, params):
       
        df = prev

        df['cite_id'] = df['cite_id'].astype(int)
        df['district_id'] = df['district_id'].astype(str).str.zfill(6) 
        df['latitud'] = df['latitud'].astype(str)
        df['longitud'] = df['longitud'].astype(str)
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
            'tipo':                  'String',
            'director':              'String',
            'coordinador_ut':        'String',
            'lista_miembros':        'String',
            'ambito':                'String',          
            'district_id' :          'String',
            'direccion':             'String',  
            'latitud':               'String',  
            'longitud':              'String',  
            'descriptivo':           'String',
            'cadena_atencion':       'String',
            'cadena_pip':            'String',
            'cadena_resolucion':     'String',
            'cite_slug':             'String',
         }

        transform_step = TransformStep()
        format_step = FormatStep()

        load_step = LoadStep(
          'dim_shared_cite', connector=db_connector, if_exists='drop',
          pk=['cite_id'], dtype=dtypes, nullable_list=[])

        if params.get("ingest")==True:
            steps = [transform_step, format_step,  load_step]
        else:
            steps = [transform_step, format_step]

        return steps

if __name__ == "__main__":
    cite_info_pipeline = CiteInfoPipeline()
    cite_info_pipeline.run(
        {
            "output-db": "clickhouse-local",
            "ingest": True
        }
    )