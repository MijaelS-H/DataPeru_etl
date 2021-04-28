import re
from os import path
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep
from bamboo_lib.steps import LoadStep
from etl.socios.cultura.agentes_libro.shared import ReplaceStep
from etl.consistency import AggregatorStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        data = pd.ExcelFile(path.join(params["datasets"], "05_Socios_Estrategicos_Ministerio_de_Cultura", "01_Informacion_Direccion_de_Industrias_Culturales", "04_Agentes_del_libro_2020_PNYPJ_DATAPERU.xlsx"))

        sheet = data.sheet_names[1]
        df = pd.read_excel(data, data.sheet_names[1])

        df.columns =  df.iloc[3]
        df = df[4:]
        df = df[['Nombre y/o razón social de la organización','Actividad_1', 'Actividad_2', 'Actividad_3', 'Actividad_4','Distrito de la organización']]

        df.rename(columns={'Nombre y/o razón social de la organización': "razon_social_id", "Actividad_1" : "actividad_1_id",
        "Actividad_2" : "actividad_2_id", "Actividad_3" : "actividad_3_id", "Actividad_4" : "actividad_4_id", "Distrito de la organización": "district_id"}, inplace=True)
        df['cantidad_agentes'] = 1
        df['actividad_1_id'] = df['actividad_1_id'].replace('cartonera', 'Cartonera')

        return df

class FormatStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev[0]

        df = df[['razon_social_id', 'actividad_1_id',
       'actividad_2_id', 'actividad_3_id', 'actividad_4_id',
       'district_id', 'cantidad_agentes']].copy()

        # column types
        df[['razon_social_id', 'actividad_1_id',
       'actividad_2_id', 'actividad_3_id', 'actividad_4_id']] = df[['razon_social_id', 'actividad_1_id',
       'actividad_2_id', 'actividad_3_id', 'actividad_4_id']].fillna(0)

        df[['razon_social_id', 'actividad_1_id',
       'actividad_2_id', 'actividad_3_id', 'actividad_4_id']] = df[['razon_social_id', 'actividad_1_id',
       'actividad_2_id', 'actividad_3_id', 'actividad_4_id']].astype(int)

        df = df.drop_duplicates()

        return df

class AgentesPipeline(EasyPipeline):
    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtype = {
            'razon_social_id':                  'UInt16',
            'actividad_1_id':                   'UInt8',
            'actividad_2_id':                   'UInt8',
            'actividad_3_id':                   'UInt8',
            'actividad_4_id':                   'UInt8',
            'district_id':                      'String',
            'cantidad_agentes':                 'UInt8',
        }

        transform_step = TransformStep()
        replace_step = ReplaceStep(connector=db_connector)
        agg_step = AggregatorStep('cultura_agentes_libro', measures=['cantidad_agentes'])
        format_step = FormatStep()
        load_step = LoadStep('cultura_agentes_libro', db_connector, if_exists='drop', pk=['district_id'], dtype=dtype)

        return [transform_step, replace_step, format_step, load_step]

def run_pipeline(params: dict):
    pp = AgentesPipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "..", "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
