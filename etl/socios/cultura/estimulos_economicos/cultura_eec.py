import re
from os import path

import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.helpers import query_to_df
from bamboo_lib.models import EasyPipeline, PipelineStep
from bamboo_lib.steps import LoadStep
from etl.consistency import AggregatorStep

from .shared import ReplaceStep

COUNTRY_DICT = {
    "Eeuu": "Estados Unidos"
}

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        #data = pd.ExcelFile(path.join(params["datasets"], "05_Socios_Estrategicos_Ministerio_de_Cultura", "01_Informacion_Direccion_de_Industrias_Culturales", "03_EEC_2019_DATAPERU.xlsx"))
        data = pd.ExcelFile(path.join(params["datasets"], "05_Socios_Estrategicos_Ministerio_de_Cultura", "01_Informacion_Direccion_de_Industrias_Culturales", "03_EEC_2020_DATAPERU.xlsx"))
        sheet = data.sheet_names[1]
        df = pd.read_excel(data, data.sheet_names[1])

        df = df[['EEC', 'Fase de cadena de valor_E',
       'Nombre del proyecto',
       'Tipo de Postulante', 'Postulante',
       'País',
       'Distrito', 'Estado revisado']]

        df = df.rename(columns={'EEC' : 'estimulo_economico_id', 'Fase de cadena de valor_E' : "fase_cadena_valor_id", "Nombre del proyecto" : "nombre_proyecto_id", 
        "Tipo de Postulante" :  "tipo_postulante_id","Postulante" : "postulante_id","Distrito" : "district_id", 'Estado revisado' :  "estado_id", "País": "pais_procedencia"})

        df['pais_procedencia'] = df['pais_procedencia'].str.title()

        df['pais_procedencia'] = df['pais_procedencia'].replace(COUNTRY_DICT)

        dim_country_query = 'SELECT country_name_es, iso3 FROM dim_shared_country'
        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))
        countries = query_to_df(db_connector, raw_query=dim_country_query)

        SHARED_DIM_COUNTRY = dict(zip(countries['country_name_es'], countries['iso3']))

        df['pais_procedencia'] = df['pais_procedencia'].replace(SHARED_DIM_COUNTRY)

        df['district_id'] = df['district_id'].fillna('Otro')
        df['district_id'] = df['district_id'].astype(str)
       
        df['estimulo_economico_id'] = df['estimulo_economico_id'].str[4:].str.strip()
        df['fase_cadena_valor_id'] = df['fase_cadena_valor_id'].str[2:].str.strip()
        df["tipo_postulante_id"] = df["tipo_postulante_id"].replace({'Persona natural': 'Persona Natural'})

        df["tipo_postulante_id"] = df["tipo_postulante_id"].fillna("Otro")

        for column in ['district_id']:
            df[column] = df[column].str.strip()
            df[column] = df[column].str.title()

        df['cantidad_postulante'] = 1
        
        return df

class FormatStep(PipelineStep):
    def run_step(self, prev, params):
        # df subset
        df = prev[0]
        
        df = df[['estimulo_economico_id',  'fase_cadena_valor_id',  'nombre_proyecto_id',  
        'tipo_postulante_id',  'postulante_id','district_id',  'estado_id', 'cantidad_postulante', 'pais_procedencia']].copy()

        ## column types

        df[['estimulo_economico_id',  'fase_cadena_valor_id',  'nombre_proyecto_id',  
        'tipo_postulante_id',  'postulante_id', 'estado_id']] = df[['estimulo_economico_id',  'fase_cadena_valor_id',  'nombre_proyecto_id',  
        'tipo_postulante_id',  'postulante_id', 'estado_id']].fillna(0)

        df[['estimulo_economico_id',  'fase_cadena_valor_id',  'nombre_proyecto_id',  
        'tipo_postulante_id',  'postulante_id', 'estado_id']] = df[['estimulo_economico_id',  'fase_cadena_valor_id',  'nombre_proyecto_id',  
        'tipo_postulante_id',  'postulante_id', 'estado_id']].astype(int).astype(str)

        return df

class EECPipeline(EasyPipeline):
    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtype = {
            'estimulo_economico_id':                'UInt16',
            'district_id':                          'String',
            'nombre_proyecto_id':                   'UInt16',
            'tipo_postulante_id':                   'UInt8',
            'postulante_id':                        'UInt16',
            'fase_cadena_valor_id':                 'UInt8',
            'estado_id':                            'UInt8',
            'cantidad_postulante':                  'UInt8',
            'pais_procedencia':                     'String'
        }

        transform_step = TransformStep()
        replace_step = ReplaceStep(connector=db_connector)
        format_step = FormatStep()
        agg_step = AggregatorStep('cultura_eec', measures=["cantidad_postulante"])
        load_step = LoadStep('cultura_eec', db_connector, if_exists='drop', 
                             pk=['estimulo_economico_id', 'fase_cadena_valor_id', 'nombre_proyecto_id'], dtype=dtype)

        return [transform_step, replace_step, format_step, load_step]

def run_pipeline(params: dict):
    pp = EECPipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "..", "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
