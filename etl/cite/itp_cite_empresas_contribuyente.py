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

CARPETAS_DICT = {
    1: "01 INFORMACIÓN INSTITUCIONAL",
    2: "02 CLIENTES ATENDIDOS",
    3: "03 SERVICIOS BRINDADOS",
    4: "04 PROYECTOS DE INVERSIÓN PÚBLICA",
    5: "05 EJECUCIÓN PRESUPUESTAL",
    6: "06 RECURSOS HUMANOS",
    7: "07 PARTIDAS ARANCELARIAS",
}

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv(path.join(params["datasets"],"20201001", "01. Información ITP red CITE  (01-10-2020)", "02 CLIENTES ATENDIDOS", "TABLA_02_N05.csv"))
        
        df["contribuyente"] = df["tipo_contribuyente"].str.capitalize() 
        dim_contribuyente_query = 'SELECT contribuyente, contribuyente_id FROM dim_shared_cite_contribuyente'
        dim_contribuyente = query_to_df(self.connector, raw_query=dim_contribuyente_query)
        df = df.merge(dim_contribuyente, on="contribuyente")
        
        dim_cite_query = 'SELECT cite, cite_id FROM dim_shared_cite'
        dim_cite = query_to_df(self.connector, raw_query=dim_cite_query)
        df = df.merge(dim_cite, on="cite")
        
        df['anio'] = df['anio'].astype(int)
        df['empresas'] = df['empresas'].astype(float)
        df = df[['cite_id', 'contribuyente_id', 'anio', 'empresas']]

        return df

class CiteContribuyentePipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtypes = {
            'cite_id':                'UInt8',
            'contribuyente_id':       'UInt8',
            'anio':                   'UInt16',
            'empresas':               'Float32',
         }

        transform_step = TransformStep(connector=db_connector)
        agg_step = AggregatorStep('itp_cite_empresas_contribuyente', measures=['empresas'])
        load_step = LoadStep('itp_cite_empresas_contribuyente', connector=db_connector, if_exists='drop', pk=['cite_id'], dtype=dtypes, nullable_list=['empresas'])

        return [transform_step, agg_step, load_step]

def run_pipeline(params: dict):
    pp = CiteContribuyentePipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
