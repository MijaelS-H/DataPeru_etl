import numpy as np
import pandas as pd
import os
from functools import reduce
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import grab_connector



class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_csv("../../../datasets/20201001/01. Información ITP red CITE  (01-10-2020)/06 RECURSOS HUMANOS/TABLA_06_N01.csv")
        
        df = pd.melt(df, id_vars=['cite','anio','modalidad'], value_vars=['directivo', 'tecnico', 'operativo', 'administrativo',
               'practicante'])
        df = df.rename(columns={'variable':'tipo_trabajador','value':'cantidad'})
        df["tipo_trabajador"] = df["tipo_trabajador"].str.capitalize()
        
        ## cite dim
        cite_list = list(df["cite"].unique())
        cite_map = {k:v for (k,v) in zip(sorted(cite_list), list(range(1, len(cite_list) +1)))}

        ## modalidad dim
        modalidad_list = list(df["modalidad"].unique())
        modalidad_map = {k:v for (k,v) in zip(sorted(modalidad_list), list(range(1, len(modalidad_list) +1)))}

        ## contratos
        tipo_trabajador_list = list(df["tipo_trabajador"].unique())
        tipo_trabajador_map = {k:v for (k,v) in zip(sorted(tipo_trabajador_list), list(range(1, len(tipo_trabajador_list) +1)))}

        df['cite_id'] = df['cite'].map(cite_map)
        df['modalidad_id'] = df['modalidad'].map(modalidad_map)
        df['tipo_trabajador_id'] = df['tipo_trabajador'].map(tipo_trabajador_map)
        
        df[['cite_id', 'anio', 'modalidad_id','tipo_trabajador_id']] = df[['cite_id', 'anio', 'modalidad_id','tipo_trabajador_id']].astype(int)
        df['cantidad'] = df['cantidad'].astype(float)
        
        df = df[['cite_id', 'anio', 'modalidad_id','tipo_trabajador_id','cantidad']]
        
        return df

class CiteContratosPipeline(EasyPipeline):
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
            'cite_id':                                 'UInt8',
            'anio':                                    'UInt16',
            'modalidad_id':                            'UInt8',
            'tipo_trabajador_id':                      'UInt8',
            'cantidad':                                'Float32',

         }

        transform_step = TransformStep()  
        load_step = LoadStep(
          'itp_cite_trabajadores', connector=db_connector, if_exists='drop',
          pk=['cite_id'], dtype=dtypes, nullable_list=['cantidad'])

        if params.get("ingest")==True:
            steps = [transform_step, load_step]
        else:
            steps = [transform_step]

        return steps

if __name__ == "__main__":
    cite_contratos_pipeline = CiteContratosPipeline()
    cite_contratos_pipeline.run(
        {
            "output-db": "clickhouse-local",
            "ingest": True
        }
    )