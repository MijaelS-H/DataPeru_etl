import numpy as np
import pandas as pd
import os
from os import path
from functools import reduce
from unidecode import unidecode
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import grab_connector

TIPO_CITE_DICT = {'Centro de Innovación Productiva y Transferencia Tecnológica (CITE)' : 'CITE', 'Unidad Técnica (UT)': 'UT'}

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

#         df = pd.read_csv(path.join(params["datasets"],"anexos", "ISIC_Rev_4_spanish_structure.txt"), encoding='latin-1')
        k = 1
        df = {}
        for i in range(1,1 +1):
            _a,_b,files = next(os.walk(path.join(params["datasets"],"20201001", "01. Información ITP red CITE  (01-10-2020)", "01 INFORMACIÓN INSTITUCIONAL")))
            file_count = len(files)

            for j in range(1, file_count + 1 ):
                file_dir = path.join(params["datasets"], "20201001", "01. Información ITP red CITE  (01-10-2020)", "01 INFORMACIÓN INSTITUCIONAL","TABLA_01_N0{}.csv".format(j))
                df[k] = pd.read_csv(file_dir)
                k = k + 1

        df_list = [df[i] for i in range(1, file_count + 1)]
        df = reduce(lambda df1,df2: pd.merge(df1,df2,on=['cite'],how='outer'), df_list)
        
        df = df[['cite', 'categoria', 'tipo', 'estado', 'patrocinador', 'director', 'coordinador_ut', 'resolucion_x', 
            'fecha', 'lista_miembros', 'resolucion_mod','fecha_mod', 'nota', 'ambito', 'resolucion_y', 'resolucion_calificacion', 
            'resolucion_adecuacion', 'resolucion_cambio_nombre', 'cadena_atencion', 'cadena_pip','cadena_resolucion','cadena_privados', 
            'ubigeo', 'direccion', 'latitud', 'longitud', 'descriptivo']]

        df['tipo'] = df['tipo'].replace(TIPO_CITE_DICT)
        df['coordinador_ut'] = df['coordinador_ut'].fillna("No disponible")
        df['descriptivo'] = df['descriptivo'].str.lstrip()

        cite_list = list(df['cite'].unique())
        cite_map = {k:v for (k,v) in zip(sorted(cite_list), list(range(1, len(cite_list) +1)))}
        df['cite_id'] = df['cite'].map(cite_map)

        df['cite'] = df['cite'].apply(unidecode)
 
        df['cite_slug'] = df['cite'].str.lower()
        df['cite_slug'] = df['cite_slug'].apply(unidecode)
        df['cite_slug'] = df['cite_slug'].str.replace(" ", "_")

        df['director'] = df['director'].str.title()
        df['lista_miembros'] = df['lista_miembros'].str.replace('\n•',',')
        df['lista_miembros'] = df['lista_miembros'].str.replace('• ','')

        df.rename(columns={'ubigeo' : 'district_id', 'resolucion_x' : 'resolucion_director', 'fecha' : 'fecha_director', 'resolucion_y' : 'resolucion_ambito'}, inplace = True)

        return df

class FormatStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev

        df['cite_id'] = df['cite_id'].astype(int)
        df['district_id'] = df['district_id'].astype(str).str.zfill(6)
        df[['cite', 'categoria', 'tipo', 'estado', 'patrocinador', 'director', 'coordinador_ut', 'resolucion_director', 
            'fecha_director', 'lista_miembros', 'resolucion_mod','fecha_mod', 'nota', 'ambito', 'resolucion_ambito', 'resolucion_calificacion', 
            'resolucion_adecuacion', 'resolucion_cambio_nombre', 'cadena_atencion', 'cadena_pip','cadena_resolucion','cadena_privados', 
            'district_id', 'direccion', 'latitud', 'longitud', 'descriptivo']] = df[['cite', 'categoria', 'tipo', 'estado', 'patrocinador', 'director', 'coordinador_ut', 'resolucion_director', 
            'fecha_director', 'lista_miembros', 'resolucion_mod','fecha_mod', 'nota', 'ambito', 'resolucion_ambito', 'resolucion_calificacion', 
            'resolucion_adecuacion', 'resolucion_cambio_nombre', 'cadena_atencion', 'cadena_pip','cadena_resolucion','cadena_privados', 
            'district_id', 'direccion', 'latitud', 'longitud', 'descriptivo']].astype(str)
            
        return df

class CiteInfoPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtypes = {
            'cite_id':                      'UInt8',
            'cite':                         'String',
            'categoria':                    'String',
            'tipo':                         'String',
            'estado':                       'String',
            'patrocinador':                 'String',
            'director':                     'String',
            'coordinador_ut':               'String',
            'resolucion_director':          'String',
            'fecha_director':               'String',
            'lista_miembros':               'String',
            'resolucion_mod':               'String',
            'fecha_mod':                    'String',
            'nota':                         'String',
            'ambito':                       'String',
            'resolucion_ambito':            'String',
            'resolucion_calificacion':      'String',
            'resolucion_adecuacion':        'String',
            'resolucion_cambio_nombre':     'String',
            'cadena_atencion':              'String',
            'cadena_pip':                   'String',
            'cadena_resolucion':            'String',
            'cadena_privados':              'String',
            'district_id':                  'String',
            'direccion':                    'String',
            'latitud':                      'String',
            'longitud':                     'String',
            'descriptivo':                  'String',
            'cite_slug':                    'String'
        }

        transform_step = TransformStep()
        format_step = FormatStep()

        load_step = LoadStep('dim_shared_cite', connector=db_connector, if_exists='drop', pk=['cite_id'], dtype=dtypes, nullable_list=['estado', 'patrocinador',
        'resolucion_director', 'fecha_director', 'lista_miembros', 'resolucion_mod', 'fecha_mod', 'nota', 'resolucion_ambito', 'resolucion_calificacion',
        'resolucion_adecuacion', 'resolucion_cambio_nombre', 'cadena_atencion', 'cadena_pip', 'cadena_resolucion', 'cadena_privados', 'direccion', 'latitud',
        'longitud'])

        return [transform_step, format_step, load_step]

def run_pipeline(params: dict):
    pp = CiteInfoPipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
