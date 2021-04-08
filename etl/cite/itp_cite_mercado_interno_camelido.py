from os import path
import pandas as pd
import xlrd
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

        wb = xlrd.open_workbook(path.join(params["datasets"], "20210119", "08 CADENAS PRODUCTIVAS Y MERCADO INTERNO", "TABLA_08_N04.xlsx"), encoding_override='latin1')
        df = pd.read_excel(
            wb,
            dtype='str'
        )

        df.dropna(how="any", inplace=True)

        df.rename(columns={
            "descripcion_producto ": "descripcion_producto"
        }, inplace=True)

        df["cadena_productiva"] = df["cadena_productiva"].str.capitalize().str.strip()
        #df["cite"] = df["cite"].str.replace("camélidos", "camelidos")

        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        # Get CITE list
        cite_id_list_query = 'SELECT cite_id, cite FROM dim_shared_cite'
        cite_id_list  = query_to_df(db_connector, raw_query=cite_id_list_query)

        # Get Cadenas list
        cadenas_id_list_query = 'SELECT * from dim_shared_cite_cad_prod'
        cadenas_id_list = query_to_df(db_connector, raw_query=cadenas_id_list_query)

        df = df.merge(cite_id_list, on="cite")

        df = df.merge(cadenas_id_list, on="cadena_productiva")

        df["departamento_id"] = df["ubigeo"].str[0:2]
        df["month_id"] = df["anio"] + df["mes"]

        df = df[["cite_id", "cad_prod_id", "codigo_producto", "month_id", "departamento_id", "produccion", "unidad"]].copy()

        df["month_id"] = df.month_id.astype("int")
        df["produccion"] = df.produccion.astype("float")

        return df

class CiteMeracadoInternoCamelido(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtypes = {
            "cite_id":              "UInt8", 
            "cad_prod_id":          "UInt8", 
            "codigo_producto":      "String", 
            "month_id":             "UInt32", 
            "departamento_id":      "String", 
            "produccion":           "Float32", 
            "unidad":               "String"
        }

        transform_step = TransformStep()

        load_step = LoadStep(
            "itp_cite_mercado_interno_camelido", db_connector, if_exists='drop',
            pk=["cite_id", "cad_prod_id", "codigo_producto", "month_id", "departamento_id"], dtype=dtypes,
            nullable_list=[])

        return [transform_step, load_step]

def run_pipeline(params: dict):
    pp = CiteMeracadoInternoCamelido()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
