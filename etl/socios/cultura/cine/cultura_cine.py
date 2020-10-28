import re
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep
from bamboo_lib.steps import LoadStep
from shared import ReplaceStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        
        data = pd.ExcelFile("../../../../../datasets/20201018/05. Socios Estratégicos - Ministerio de Cultura (18-10-2020)/02. RCN_2020_PJ_DATAPERU.xlsx")
        sheet = data.sheet_names[1]
        df = pd.read_excel(data, data.sheet_names[1])
        df = df[0:1267]
        df = df[['AÑO INSCRIPCIÓN',
       'TIPO DE CONSTITUCIÓN', 'RAZON SOCIAL ',
       'Actividad_1', 'Actividad_2', 'Actividad_3', 'Actividad_4',
       'DISTRITO']]
        
        for column in ['DISTRITO','TIPO DE CONSTITUCIÓN']:

            df[column] = df[column].str.strip()
    
        df = df.rename(columns={'AÑO INSCRIPCIÓN' : "anio_inscripcion","TIPO DE CONSTITUCIÓN" : "tipo_constitucion_id","RAZON SOCIAL " : "razon_social_id",
            "Actividad_1" : "actividad_1_id","Actividad_2":"actividad_2_id", "Actividad_3":"actividad_3_id",
            "Actividad_4":"actividad_4_id","DISTRITO":"district_id"})


        df['cantidad_org'] = 1
        df = df[df['district_id'].notna()]
        df = df[df['anio_inscripcion'].notna()]
        df = df[df['anio_inscripcion'] != "ND"]
     
        return df

class FormatStep(PipelineStep):
    def run_step(self, prev, params):
        # df subset
        df = prev[0]
        
        df = df[['anio_inscripcion','tipo_constitucion_id','razon_social_id','actividad_1_id',	
        'actividad_2_id','actividad_3_id','actividad_4_id',	'district_id', 'cantidad_org']].copy()

        # column types

        df[['actividad_1_id',	
        'actividad_2_id','actividad_3_id','actividad_4_id']] = df[['actividad_1_id',	
        'actividad_2_id','actividad_3_id','actividad_4_id']].fillna(0)

        df[['actividad_1_id',	
        'actividad_2_id','actividad_3_id','actividad_4_id']] = df[['actividad_1_id',	
        'actividad_2_id','actividad_3_id','actividad_4_id']].astype(int).astype(str)

        
        return df

class CinePipeline(EasyPipeline):
    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open('../../../conns.yaml'))

        dtype = {
            'anio_inscripcion':               'UInt16',
            'tipo_constitucion_id':           'UInt8',
            'razon_social_id':                'UInt16',
            'actividad_1_id':                 'UInt8',
            'actividad_2_id':                 'UInt8',
            'actividad_3_id':                 'UInt8',
            'actividad_4_id':                 'UInt8',
            'district_id':                    'String',
            'cantidad_org':                   'UInt8',
        }

        transform_step = TransformStep()
        replace_step = ReplaceStep()
        format_step = FormatStep()
        load_step = LoadStep('cultura_cine', db_connector, if_exists='drop', 
                             pk=['district_id'], dtype=dtype)

        return [transform_step, replace_step, format_step, load_step]


if __name__ == "__main__":
    pp = CinePipeline()
    pp.run({})