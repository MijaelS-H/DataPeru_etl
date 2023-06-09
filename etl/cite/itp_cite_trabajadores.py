import numpy as np
import pandas as pd
import os
from os import path
from functools import reduce
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import grab_connector
from etl.consistency import AggregatorStep
from bamboo_lib.helpers import query_to_df

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv(path.join(params["datasets"], "01_Informacion_ITP_red_CITE", "06_RECURSOS_HUMANOS", "TABLA_06_N01.csv"), encoding="latin1")

        df = pd.melt(df, id_vars=['cite','anio','modalidad','fecha'], value_vars=['directivo', 'tecnico', 'operativo', 'administrativo', 'practicante'])
        df = df.rename(columns={'variable':'tipo_trabajador','value':'cantidad'})
        df["tipo_trabajador"] = df["tipo_trabajador"].str.capitalize()
        
        ## cite dim
        dim_cite_query = 'SELECT cite, cite_id FROM dim_shared_cite'
        dim_cite = query_to_df(self.connector, raw_query=dim_cite_query)
        df = df.merge(dim_cite, on="cite")
        
        ## modalidad dim
        modalidad_list = list(df["modalidad"].unique())
        modalidad_map = {k:v for (k,v) in zip(sorted(modalidad_list), list(range(1, len(modalidad_list) +1)))}

        ## contratos
        tipo_trabajador_list = list(df["tipo_trabajador"].unique())
        tipo_trabajador_map = {k:v for (k,v) in zip(sorted(tipo_trabajador_list), list(range(1, len(tipo_trabajador_list) +1)))}

        df['modalidad_id'] = df['modalidad'].map(modalidad_map)
        df['tipo_trabajador_id'] = df['tipo_trabajador'].map(tipo_trabajador_map)

        df[['cite_id', 'anio', 'modalidad_id','tipo_trabajador_id']] = df[['cite_id', 'anio', 'modalidad_id','tipo_trabajador_id']].astype(int)
        df['cantidad'] = df['cantidad'].astype(float)

        df['fecha_actualizacion'] = df['fecha'].str[-4:] + df['fecha'].str[3:5]
        df['fecha_actualizacion'] = df['fecha_actualizacion'].astype(int)

        df = df[['cite_id', 'anio', 'modalidad_id','tipo_trabajador_id','cantidad','fecha_actualizacion']]

        return df

class CiteContratosPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtypes = {
            'cite_id':                                 'UInt8',
            'anio':                                    'UInt16',
            'modalidad_id':                            'UInt8',
            'tipo_trabajador_id':                      'UInt8',
            'cantidad':                                'Float32',
            'fecha_actualizacion':                     'UInt32'
         }

        transform_step = TransformStep(connector=db_connector)
        agg_step = AggregatorStep('itp_cite_trabajadores', measures=['cantidad'])
        load_step = LoadStep('itp_cite_trabajadores', connector=db_connector, if_exists='drop', pk=['cite_id'], dtype=dtypes, nullable_list=['cantidad'])

        return [transform_step, load_step]

def run_pipeline(params: dict):
    pp = CiteContratosPipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
