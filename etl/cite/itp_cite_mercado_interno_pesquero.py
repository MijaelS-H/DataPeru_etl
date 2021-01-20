from os import path
import numpy as np
import pandas as pd

from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import query_to_df
from bamboo_lib.helpers import grab_connector

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_csv(
            path.join(params["datasets"], "20210119", "08 CADENAS PRODUCTIVAS Y MERCADO INTERNO", "TABLA_08_N02.csv"),
            dtype='str',
            sep=";",
            encoding="latin-1"
        )

        df = df.dropna(how="any").copy()

        df.rename(columns={
            "codigo_producto ": "codigo_producto",
            "desagregación": "desagregacion"
        }, inplace=True)

        df["cadena_productiva"] = df["cadena_productiva"].str.capitalize().str.strip()

        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        # Get CITE list
        cite_id_list_query = 'SELECT cite_id, cite FROM dim_shared_cite'
        cite_id_list  = query_to_df(db_connector, raw_query=cite_id_list_query)

        # Get Cadenas list
        cadenas_id_list_query = 'SELECT * from dim_shared_cite_cad_prod'
        cadenas_id_list = query_to_df(db_connector, raw_query=cadenas_id_list_query)

        df = df.merge(cite_id_list, on="cite")

        df = df.merge(cadenas_id_list, on="cadena_productiva")

        df = df[["cite_id", "cad_prod_id", "codigo_producto", "anio", "desagregacion", "desembarque", "unidad"]].copy()

        df["desembarque"] = df.desembarque.str.replace(",", "")

        df.desembarque.replace({
            "N.D.": np.nan
        }, inplace=True)

        df["desembarque"] = df.desembarque.astype("float")
        df["anio"] = df.anio.astype("int")
        df["desagregacion"] = "per"

        return df

class CiteMeracadoInternoPesquero(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtypes = {
            "cite_id":              "UInt8", 
            "cad_prod_id":          "UInt8", 
            "codigo_producto":      "String", 
            "anio":                 "UInt16", 
            "desagregacion":        "String", 
            "desembarque":          "Float32", 
            "unidad":               "String"
        }

        transform_step = TransformStep()

        load_step = LoadStep(
            "itp_cite_mercado_interno_pesquero", db_connector, if_exists='drop',
            pk=["cite_id", "cad_prod_id", "codigo_producto", "anio", "desagregacion"], dtype=dtypes,
            nullable_list=["desembarque"])

        return [transform_step, load_step]

def run_pipeline(params: dict):
    pp = CiteMeracadoInternoPesquero()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
