from os import path

import numpy as np
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep
from bamboo_lib.steps import LoadStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        #read dataframe
        df = pd.read_spss(
            path.join(params["datasets"], "02_Informacion_Censos", "03_CENSO_NACIONAL_DE_MERCADOS_DE_ABASTO", "02_MODULO_1117_Identificacion_del_Mercado_e_Informante", "Capitulo_II_NACIONAL.sav")
        )

        df.columns = df.columns.str.lower()
        df = df[['id', 'nombre_mercado']].copy()
        
        df['nombre_mercado'] = df['nombre_mercado'].str.capitalize()

        df.rename(columns = {
            'id': 'market_id',
            'nombre_mercado': 'market_name'
        }, inplace=True)
        
        return df


class MarketPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        table_name = "dim_market"
        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))

        dtype = {"market_id": "String", "market_name": "String"}

        transform_step = TransformStep()

        load_step = LoadStep(table_name, db_connector, if_exists='drop', 
                             pk=['market_id'], dtype=dtype,
                             nullable_list=[])

        return [transform_step, load_step]


def run_pipeline(params: dict):
    pp = MarketPipeline()
    pp.run(params)


if __name__ == "__main__":
    import sys
    from os import path
    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline({
        "connector": path.join(__dirname, "..", "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
