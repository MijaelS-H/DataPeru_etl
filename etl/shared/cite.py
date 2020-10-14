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


CADENAS_DICT = {
    'Tilapia, paiche, paco, gamitana y doncella': 10217,
    'Frutas y hortalizas, derivados lácteos': 21010,
    'Café, cacao y frutas de la zona': 50503,
    'Uva, espárrago, cebolla,  manzana, higo, plátano, cacao': 21319,
    'Uva, hongos comestibles, derivados lácteos, maíz morado, olivo, granos andinos y hierbas aromáticas': 21120,
    'Uva, palta y orégano': 21821,
    'Café, cacao, aguaymanto, granadilla, quito quito, piña, plátano, berries, naranja y carambola': 21404,
    'Café y cacao': 40402,
    'Cuero y calzado': 90808,
    'Madera y forestal': 121513,
    'Madera e industrias conexas': 110112,
    'Tilapia, paiche, trucha': 131618,
    'Paiche, paco, gamitana, doncella, tilapia': 131614,
    'Productos hidrobiológicos': 140115,
    'Lorna, pota, jurel, bonito, anchoveta, macroalgas, choro, caballa, lenguado, corvina, lapa, macha, pulpo, chanque': 131711,
    '• Agroindustrial: copoazú, cacao, ají charapita, castaña, naranja, plátano, sacha inchi, aguaymanto, aguaje\n• Pesquero: paco, gamitana, paiche, trucha, camarones\n• Madera: 1era y 2da transformación ': 31922,
    'Camu camu, aguaje, gamitana y paiche de la zona': 61205,
    'Cadena productiva de la alpaca': 70601,
    'Textiles': 160616,
    'Camélidos domésticos': 70606,
    'Chirimoya, granada, guanábana, mandarina, guinda, miel de abeja y uva': 20707,
    'Durazno, granadilla, rocoto, aguaymanto, yacón, carambola, papa, tocosh, yuca y plátano': 20309}
    
    

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        k = 1
        df = {}
        for i in range(1,1 +1):
            path, dirs, files = next(os.walk("../../data/01. Información ITP red CITE  (01-10-2020)/{}/".format(CARPETAS_DICT[i])))
            file_count = len(files)
    
            for j in [1,5,6,7]:
                file_dir = "../../data/01. Información ITP red CITE  (01-10-2020)/{}/TABLA_0{}_N0{}.csv".format(CARPETAS_DICT[i],i,j)

                df[k] = pd.read_csv(file_dir)
                k = k + 1

        i = 4
        j = 1
        df[5] = pd.read_csv("../../data/01. Información ITP red CITE  (01-10-2020)/{}/TABLA_0{}_N0{}.csv".format(CARPETAS_DICT[i],i,j))
        
        df[1]['tipo'] = df[1]['tipo'].replace({'Centro de Innovación Productiva y Transferencia Tecnológica (CITE)' : 'CITE',
       'Unidad Técnica (UT)': 'UT'})

        df[3] = df[3][['cite','ubigeo','latitud','longitud']]

        df_list = [df[i] for i in range(1,5)]
        df = reduce(lambda df1,df2: pd.merge(df1,df2,on=['cite'],how='outer'), df_list)

        
        for cite in df['cite'].unique():
            df['descriptivo'] = df['descriptivo'].str.replace("UT",'')
            df['descriptivo'] = df['descriptivo'].str.replace('El ','')
            df['descriptivo'] = df['descriptivo'].str.replace('CITE','')
            df['descriptivo'] = df['descriptivo'].str.replace('La ','')
            df['descriptivo'] = df['descriptivo'].str.lstrip()
            df['descriptivo'] = df['descriptivo'].str.capitalize()
        
        df['ubigeo'] = df['ubigeo'].astype(str).str.zfill(6)
        df['cite_id'] = df['cite'].index + 1
        
        df['cadena_atencion_id'] = df['cadena_atencion'].map(CADENAS_DICT)  
        
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

        df = df[['cite_id','cite','cite_slug', 'tipo','tipo_id', 'cadena_atencion','cadena_atencion_id',
       'ubigeo', 'latitud', 'longitud', 'descriptivo']]

        print(df)
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
            'cite':                  'String',
            'cite_slug':             'String',   
            'tipo':                  'String',
            'tipo_id':               'UInt8',
            'cadena_atencion':       'String',   
            'cadena_atencion_id':    'UInt8',  
            'ubigeo':                'String',  
            'latitud':               'Float32',  
            'longitud':              'Float32',  
            'descriptivo':           'String', 
         }

        transform_step = TransformStep()  
        load_step = LoadStep(
          'dim_shared_cite', connector=db_connector, if_exists='drop',
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
            "ingest": False
        }
    )