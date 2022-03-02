import re
import numpy as np
import pandas as pd
from os import path
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep
from bamboo_lib.steps import LoadStep
from etl.socios.cultura.cine.shared import ReplaceStep
from etl.consistency import AggregatorStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        #data = pd.ExcelFile(path.join(params["datasets"], "05_Socios_Estrategicos_Ministerio_de_Cultura", "01_Informacion_Direccion_de_Industrias_Culturales", "02_RCN_2020_PJ_DATAPERU.xlsx"))
        data = pd.ExcelFile(path.join(params["datasets"], "05_Socios_Estrategicos_Ministerio_de_Cultura", "01_Informacion_Direccion_de_Industrias_Culturales", "02_RCN_2021_PJ_DATAPERU.xlsx"))

        sheet = data.sheet_names[1]
        df = pd.read_excel(data, data.sheet_names[1])
        df = df[0:1267]
        df = df[['AÑO INSCRIPCIÓN',
       'TIPO DE CONSTITUCIÓN', 'RAZON SOCIAL ',
       'Actividad_1', 'Actividad_2', 'Actividad_3', 'Actividad_4',
       'DISTRITO']].copy()

        for column in ['DISTRITO','TIPO DE CONSTITUCIÓN']:

            df[column] = df[column].str.strip()

        df = df.rename(columns={'AÑO INSCRIPCIÓN' : "year","TIPO DE CONSTITUCIÓN" : "tipo_constitucion_id","RAZON SOCIAL " : "razon_social_id",
            "Actividad_1" : "actividad_1_id","Actividad_2":"actividad_2_id", "Actividad_3":"actividad_3_id",
            "Actividad_4":"actividad_4_id","DISTRITO":"district_id"})

        df['cantidad_org'] = 1

        df['district_id'] = df['district_id'].fillna('999999')
        df['year'] = df['year'].replace({"ND": np.nan})

        return df

class FormatStep(PipelineStep):
    def run_step(self, prev, params):
        # df subset
        df = prev[0]

        df = df[['year','tipo_constitucion_id','razon_social_id','actividad_1_id',	
        'actividad_2_id','actividad_3_id','actividad_4_id',	'district_id', 'cantidad_org']].copy()

        # column types
        df['year'] = df['year'].astype(float).fillna(0)

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

        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtype = {
            'year':                           'UInt16',
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
        replace_step = ReplaceStep(connector=db_connector)
        format_step = FormatStep()
        agg_step = AggregatorStep('cultura_cine', measures=["cantidad_org"])
        load_step = LoadStep('cultura_cine', db_connector, if_exists='drop', pk=['district_id'], dtype=dtype, nullable_list=['year'])

        return [transform_step, replace_step, format_step, load_step]


def run_pipeline(params: dict):
    pp = CinePipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "..", "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
