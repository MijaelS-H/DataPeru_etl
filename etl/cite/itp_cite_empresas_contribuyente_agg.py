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
        df = pd.read_csv(path.join(params["datasets"], "01_Informacion_ITP_red_CITE", "02_CLIENTES_ATENDIDOS", "TABLA_02_N06.csv"), encoding="latin1")
        
        df["contribuyente"] = df["tipo_contribuyente"].str.capitalize() 
        dim_contribuyente_query = 'SELECT contribuyente, contribuyente_id FROM dim_shared_cite_contribuyente'
        dim_contribuyente = query_to_df(self.connector, raw_query=dim_contribuyente_query)
        df = df.merge(dim_contribuyente, on="contribuyente")
        
        df = df[['contribuyente_id', 'anio', 'empresas', 'fecha']].copy()

        df['anio'] = df['anio'].astype(int)
        df['empresas'] = df['empresas'].astype(float)

        df['empresas'].fillna(0, inplace=True)

        df['fecha_actualizacion'] = df['fecha'].str[-4:] + df['fecha'].str[3:5]
        df['fecha_actualizacion'] = df['fecha_actualizacion'].astype(int)

        return df

class CiteContribuyentePipelineAgg(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtypes = {
            'contribuyente_id':       'UInt8',
            'anio':                   'UInt16',
            'empresas':               'UInt32',
            'fecha_actualizacion':    'UInt32'
        }

        transform_step = TransformStep(connector=db_connector)
        agg_step = AggregatorStep('itp_cite_empresas_contribuyente_agg', measures=['empresas'])
        load_step = LoadStep('itp_cite_empresas_contribuyente_agg', connector=db_connector, if_exists='drop', pk=['contribuyente_id'], dtype=dtypes, nullable_list=['empresas'])

        return [transform_step, load_step]

def run_pipeline(params: dict):
    pp = CiteContribuyentePipelineAgg()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
