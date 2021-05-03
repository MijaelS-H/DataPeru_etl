
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

        query = """SELECT tipo_gobierno, sector, programa_ppto, producto_proyecto, 
                funcion, division_funcional, pliego_nombre, sec_ejec AS ejecutora, departamento_meta, monto_pia, monto_pim, monto_devengado, 
                district_id, month_id FROM {} where version = '{}'""".format(params.get("temp-table"), params.get("url"))
        
        df = query_to_df(self.connector, raw_query=query)

        dim_query = """SELECT * FROM temp_dim_mef_gastos_pliego"""

        dim_df = query_to_df(self.connector, raw_query=dim_query)

        dim_df['tipo_gobierno'] = dim_df.pliego.str.slice(0, 1)
        dim_df['pliego_nombre_unique'] = dim_df.pliego.str.slice(0, 3)

        df_nac = df[df.tipo_gobierno == 'E'].copy()
        df_reg = df[df.tipo_gobierno == 'R'].copy()
        df_mun = df[df.tipo_gobierno == 'M'].copy()

        df_nac['pliego_nombre_unique'] = df_nac['tipo_gobierno'] + df_nac['sector'].astype(str).str.zfill(2)
        df_reg['pliego_nombre_unique'] = df_reg['tipo_gobierno'] + df_reg['sector'].astype(str).str.zfill(2)
        df_mun['pliego_nombre_unique'] = df_mun['tipo_gobierno'] + df_mun['sector'].astype(str).str.zfill(2)

        dim_nac = dim_df[dim_df.tipo_gobierno == 'E'][['pliego', 'pliego_nombre', 'pliego_nombre_unique']]
        dim_reg = dim_df[dim_df.tipo_gobierno == 'R'][['pliego', 'pliego_nombre', 'pliego_nombre_unique']]
        dim_mun = dim_df[dim_df.tipo_gobierno == 'M'][['pliego', 'pliego_nombre', 'pliego_nombre_unique']]

        df_nac = df_nac.merge(dim_nac, on=["pliego_nombre_unique", "pliego_nombre"], how="left")
        df_reg = df_reg.merge(dim_reg, on=["pliego_nombre_unique", "pliego_nombre"], how="left")
        df_mun = df_mun.merge(dim_mun, on=["pliego_nombre_unique", "pliego_nombre"], how="left")

        df = df_nac.append([df_reg, df_mun])

        query_last_period = "SELECT max(month_id) FROM {} where version = '{}'".format(params.get("temp-table"), params.get("url"))
        last_period = query_to_df(self.connector, raw_query=query_last_period).iloc[0].to_list()[0]
        print("Max current period:", last_period)

        df['departamento_meta'] = pd.to_numeric(df['departamento_meta']).astype(int).astype(str).str.zfill(2)
        df['year'] =  df['month_id'].map(lambda x: str(x)[:4])
        df['month_id'] = df['month_id'].map(lambda x: 0 if str(x)[4:] == '00' else x)
        df['latest'] = 0
        df.loc[df['month_id'] == last_period, 'latest'] = 1

        df = df[[
            'tipo_gobierno', 'sector', 'programa_ppto', 'producto_proyecto', 'funcion', 'division_funcional', 'pliego', 'ejecutora', 'departamento_meta',
            'monto_pia', 'monto_pim', 'monto_devengado', 'district_id', 'month_id', 'year', 'latest'
        ]]

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
            "pliego":                        "String",
            "ejecutora":                     "UInt32",
            "departamento_meta":             "String",
            "monto_pia":                     "Float64",
            "monto_pim":                     "Float64",
            "monto_devengado":               "Float64",
            "district_id":                   "String",
            "month_id":                      "UInt32",
            "year":                          "UInt16",
            "latest":                        "UInt8"
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