
import os
import csv
import pandas as pd
from io import StringIO
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, UnzipStep, LoadStep
from bamboo_lib.helpers import query_to_df
from etl.helpers import clean_tables
from .static import URL_GASTO

class ReadStep(PipelineStep):
    def run_step(self, prev, params):

        column = params.get("dimension")
        query = """SELECT tipo_gobierno, sector, programa_ppto, producto_proyecto, 
                funcion, division_funcional, pliego, ejecutora, departamento_meta, monto_pia, monto_pim, monto_devengado, 
                district_id, month_id FROM {} where version = '{}'""".format(params.get("temp-table"), params.get("url"))
        df = query_to_df(self.connector, raw_query=query)

        df['year'] =  df['month_id'].map(lambda x: str(x)[:4] if str(x)[4:] == '00' else '0')
        df['month_id'] = df['month_id'].map(lambda x: '0' if str(x)[4:] == '00' else x)

        return df

class IngresosPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(name="url", dtype=str),
            Parameter(name="temp-table", dtype=str),
            Parameter(name="data-table", dtype=str)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open(params.get("connector")))

        dtypes = {
            "tipo_gobierno":                 "String",
            "sector":                        "UInt8",
            "programa_ppto":                 "UInt16",
            "producto_proyecto":             "UInt32",
            "funcion":                       "UInt8",
            "division_funcional":            "UInt8",
            "pliego":                        "UInt16",
            "ejecutora":                     "UInt16",
            "departamento_meta":             "UInt8",
            "monto_pia":                     "Int64",
            "monto_pim":                     "Int64",
            "monto_devengado":               "Int64",
            "district_id":                   "String",
            "month_id":                      "UInt32",
            "year":                          "UInt16"
        }

        read_step = ReadStep(connector=db_connector)

        load_step = LoadStep(params.get("data-table"), db_connector, if_exists="append", pk=["district_id", "month_id"], dtype=dtypes)

        return [read_step, load_step]


def run_pipeline(params: dict):
    PARAMS = {
        "temp-table": "temp_mef_gastos",
        "data-table": "mef_gastos"
    }
    # drop table before run all
    clean_tables(PARAMS["data-table"], params.get("connector"))
    pp = IngresosPipeline()
    for url in URL_GASTO:
        pp_params = {"url": url, "temp-table": PARAMS["temp-table"], "data-table": PARAMS["data-table"]}
        pp_params.update(params)
        pp.run(pp_params)


if __name__ == "__main__":
    import sys
    from os import path
    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml")
    })