import re
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep
from bamboo_lib.steps import LoadStep
from shared import ReplaceStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        
        data = pd.ExcelFile("../../../../../datasets/20201018/05. Socios Estratégicos - Ministerio de Cultura (18-10-2020)/03.  EEC_2019_DATAPERU.xlsx")
        sheet = data.sheet_names[1]
        df = pd.read_excel(data, data.sheet_names[1])

        df = df[['EEC', 'Fase de cadena de valor_E',
       'Nombre del proyecto',
       'Tipo de Postulante', 'Postulante',
       'Distrito', 'Estado revisado']]
     
        df = df.rename(columns={'EEC' : 'estimulo_economico_id', 'Fase de cadena de valor_E' : "fase_cadena_valor_id", "Nombre del proyecto" : "nombre_proyecto_id", 
        "Tipo de Postulante" :  "tipo_postulante_id","Postulante" : "postulante_id","Distrito" : "district_id", 'Estado revisado' :  "estado_id"})
        
        df = df[df['district_id'].notna()]
       
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
        'tipo_postulante_id',  'postulante_id','district_id',  'estado_id', 'cantidad_postulante']].copy()

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

        db_connector = Connector.fetch('clickhouse-database', open('../../../conns.yaml'))

        dtype = {
            'estimulo_economico_id':                'UInt16',
            'district_id':                          'String',
            'nombre_proyecto_id':                   'UInt8',
            'tipo_postulante_id':                   'UInt8',
            'postulante_id':                        'UInt8',
            'fase_cadena_valor_id':                 'UInt8',
            'estado_id':                            'UInt8',
            'cantidad_postulante':                  'UInt8',
        }

        transform_step = TransformStep()
        replace_step = ReplaceStep()
        format_step = FormatStep()
        load_step = LoadStep('cultura_eec', db_connector, if_exists='drop', 
                             pk=['estimulo_economico_id', 'fase_cadena_valor_id', 'nombre_proyecto_id'], dtype=dtype)

        return [transform_step, replace_step, format_step]


if __name__ == "__main__":
    pp = EECPipeline()
    pp.run({})