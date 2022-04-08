import csv
import numpy as np
import pandas as pd
import os
import glob
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

        k = 1
        df = {}
        for i in range(1,1 +1):

            file_list = glob.glob(path.join(params["datasets"], "01_Informacion_ITP_red_CITE", "01_INFORMACION_INSTITUCIONAL", '*'))

            for item in file_list:

                if item == "../datasets/01_Informacion_ITP_red_CITE/01_INFORMACION_INSTITUCIONAL/TABLA_01_N01.csv":
                    df[k] = pd.read_csv(item, encoding='latin1')
                else:
                    df[k] = pd.read_csv(item, encoding='latin1')

                k += 1

        df_list = [df[i] for i in range(1, len(file_list) + 1)]

        df = reduce(lambda df1, df2: pd.merge(df1, df2, on=['cite'], how='outer'), df_list)
        
        df = df[['cite', 'categoria', 'tipo', 'estado', 'patrocinador', 'director', 'coordinador_ut', 'resolucion_x', 
            'fecha', 'lista_miembros', 'resolucion_mod', 'fecha_mod', 'nota', 'ambito', 'resolucion_y', 'resolucion_calificacion', 
            'resolucion_adecuacion', 'resolucion_cambio_nombre', 'cadena_atencion', 'cadena_pip', 'cadena_resolucion', 'cadena_privados', 
            'ubigeo', 'direccion', 'latitud', 'longitud', 'descriptivo']]

        df['tipo'] = df['tipo'].replace(TIPO_CITE_DICT)
        df['coordinador_ut'] = df['coordinador_ut'].fillna("No disponible")
        df['descriptivo'] = df['descriptivo'].str.lstrip()

        #cite_list = list(df['cite'].unique())

        #cite_map = {k:v for (k,v) in zip(sorted(cite_list), list(range(1, len(cite_list) +1)))}
        #cite_map = {k:v for (k,v) in zip(cite_list, list(range(1, len(cite_list) +1)))}

        #df['cite_id'] = df['cite'].map(cite_map)
        df['cite_id'] = df['CITEID'].astype(int)
 
        df['cite_slug'] = df['cite'].str.lower()
        df['cite_slug'] = df['cite_slug'].apply(unidecode)
        df['cite_slug'] = df['cite_slug'].str.replace(" ", "_")

        df['director'] = df['director'].str.title()

        df['lista_miembros'] = df['lista_miembros'].str.replace('\n',',')
        df['lista_miembros'] = df['lista_miembros'].str.strip().replace('\n•',',')
        df['lista_miembros'] = df['lista_miembros'].str.strip().replace('• ','')
        
        df['cadena_atencion'] = df['cadena_atencion'].str.replace('\x95','•')
        df['cadena_atencion'] = df['cadena_atencion'].str.replace('\n','')

        df['cadena_pip'] = df['cadena_pip'].str.replace('\x95','•').str.strip()
        df['cadena_pip'] = df['cadena_pip'].str.replace('\n','').str.strip()

        #df['cadena_resolucion'] = df['cadena_resolucion'].str.replace('\n',',')
        #df['cadena_resolucion'] = df['cadena_resolucion'].str.strip().replace('\n•',',')
        #df['cadena_resolucion'] = df['cadena_resolucion'].str.strip().replace('• ','')

        df.rename(columns={'ubigeo' : 'district_id', 'resolucion_x' : 'resolucion_director', 'fecha' : 'fecha_director', 'resolucion_y' : 'resolucion_ambito'}, inplace = True)

        return df

class FormatStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev

        df['cite_id'] = df['cite_id'].astype(int)
        df['district_id'] = df['district_id'].fillna(0).astype(int).astype(str).str.zfill(6)
        df[['cite', 'categoria', 'tipo', 'estado', 'patrocinador', 'director', 'coordinador_ut', 'resolucion_director', 
            'fecha_director', 'lista_miembros', 'resolucion_mod','fecha_mod', 'nota', 'ambito', 'resolucion_ambito', 'resolucion_calificacion', 
            'resolucion_adecuacion', 'resolucion_cambio_nombre', 'cadena_atencion', 'cadena_pip','cadena_resolucion','cadena_privados', 
            'district_id', 'direccion', 'latitud', 'longitud', 'descriptivo']] = df[['cite', 'categoria', 'tipo', 'estado', 'patrocinador', 'director', 'coordinador_ut', 'resolucion_director', 
            'fecha_director', 'lista_miembros', 'resolucion_mod','fecha_mod', 'nota', 'ambito', 'resolucion_ambito', 'resolucion_calificacion', 
            'resolucion_adecuacion', 'resolucion_cambio_nombre', 'cadena_atencion', 'cadena_pip','cadena_resolucion','cadena_privados', 
            'district_id', 'direccion', 'latitud', 'longitud', 'descriptivo']].fillna(np.nan)
        df[['latitud', 'longitud']] = df[['latitud', 'longitud']].astype(str)
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
        'longitud', 'director', 'ambito', 'descriptivo'])

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
