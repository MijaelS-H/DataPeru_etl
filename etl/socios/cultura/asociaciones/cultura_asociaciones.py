import re
import pandas as pd
from os import path
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep
from bamboo_lib.steps import LoadStep
from etl.socios.cultura.asociaciones.shared import ReplaceStep
from etl.consistency import AggregatorStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        data = pd.ExcelFile(path.join(params["datasets"], "05_Socios_Estrategicos_Ministerio_de_Cultura", "01_Informacion_Direccion_de_Industrias_Culturales", "01_Puntos_de_Cultura_2020_DataPeru.xlsx"))
        sheet = data.sheet_names[1]
        df = pd.read_excel(data, data.sheet_names[1])

        df = df[['Código','Nombre','Distrito', 'Año de fundación', '¿La organización está inscrita en SUNARP?',
            'Cantidad de miembros', 'Actividad realizada N° 1',
            'Actividad realizada N° 2', 'Manifestación artística cultural N° 1',
            'Manifestación artística cultural N° 2',
            'Manifestación artística cultural N° 3'
            ]].copy()

        for column in [
            'Código','Nombre','Distrito', '¿La organización está inscrita en SUNARP?',
            'Actividad realizada N° 1',
            'Actividad realizada N° 2', 'Manifestación artística cultural N° 1',
            'Manifestación artística cultural N° 2',
            'Manifestación artística cultural N° 3'
            ]:

            df[column] = df[column].str.strip()

        df  = df.rename(columns = {"Código" : "codigo_asociacion", "¿La organización está inscrita en SUNARP?" : "inscrita_sunarp_id", 
        "Actividad realizada N° 1" : "actividad_n_1_id",  "Actividad realizada N° 2" : "actividad_n_2_id", "Manifestación artística cultural N° 1": "manifestacion_n_1_id", 
        "Manifestación artística cultural N° 2": "manifestacion_n_2_id", "Manifestación artística cultural N° 3": "manifestacion_n_3_id","Distrito" : 
        "district_id","Nombre" : "asociacion_name", "Año de fundación": "anio_fundacion", "Cantidad de miembros": "cantidad_miembros"})

        for column in ['manifestacion_n_1_id','manifestacion_n_2_id', 'manifestacion_n_3_id']:
            df[column] = df[column].str.replace('Otro:', '' )
            df[column] = df[column].str.replace('Otros:', '').str.strip()
        
        df['inscrita_sunarp_id'] = df['inscrita_sunarp_id'].str.title()
        df['cantidad_asociacion'] = 1
        
        return df

class FormatStep(PipelineStep):
    def run_step(self, prev, params):
        # df subset
        df = prev[0]

        df = df[['codigo_asociacion', 'district_id',
       'inscrita_sunarp_id', 'actividad_n_1_id', 'actividad_n_2_id',
       'manifestacion_n_1_id', 'manifestacion_n_2_id', 'manifestacion_n_3_id',
       'cantidad_asociacion', 'anio_fundacion', 'cantidad_miembros']].copy()

        # column types
        df[['inscrita_sunarp_id', 'actividad_n_1_id', 'actividad_n_2_id',
       'manifestacion_n_1_id', 'manifestacion_n_2_id', 'manifestacion_n_3_id', 'cantidad_miembros']] = df[['inscrita_sunarp_id', 'actividad_n_1_id', 'actividad_n_2_id',
       'manifestacion_n_1_id', 'manifestacion_n_2_id', 'manifestacion_n_3_id', 'cantidad_miembros']].astype(float).fillna(0)

        df['inscrita_sunarp_id'] = df['inscrita_sunarp_id'].astype(int).replace({0: 2})
        df['anio_fundacion'] = df['anio_fundacion'].astype(float).fillna(0)

        df[[
        'inscrita_sunarp_id', 'actividad_n_1_id', 'actividad_n_2_id',
        'manifestacion_n_1_id', 'manifestacion_n_2_id', 'manifestacion_n_3_id',
        'cantidad_asociacion', 'cantidad_miembros']] = df[[
        'inscrita_sunarp_id', 'actividad_n_1_id', 'actividad_n_2_id',
        'manifestacion_n_1_id', 'manifestacion_n_2_id', 'manifestacion_n_3_id',
        'cantidad_asociacion', 'cantidad_miembros']].astype(int)

        return df

class AsociacionPipeline(EasyPipeline):
    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtype = {
            'codigo_asociacion':                    'String',
            'district_id':                          'String',
            'inscrita_sunarp_id':                   'UInt8',
            'actividad_n_1_id':                     'UInt8',
            'actividad_n_2_id':                     'UInt8',
            'manifestacion_n_1_id':                 'UInt8',
            'manifestacion_n_2_id':                 'UInt8',
            'manifestacion_n_3_id':                 'UInt8',
            'cantidad_asociacion':                  'UInt8',
            'cantidad_miembros':                    'UInt16',
            'anio_fundacion':                       'UInt16'
        }

        transform_step = TransformStep()
        replace_step = ReplaceStep(connector=db_connector)
        agg_step = AggregatorStep('cultura_asociaciones', measures=['cantidad_asociacion', 'cantidad_miembros'])
        format_step = FormatStep()
        load_step = LoadStep('cultura_asociaciones', db_connector, if_exists='drop', pk=['district_id'], dtype=dtype,
                            nullable_list=['anio_fundacion'])

        return [transform_step, replace_step, format_step, load_step]

def run_pipeline(params: dict):
    pp = AsociacionPipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "..", "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
