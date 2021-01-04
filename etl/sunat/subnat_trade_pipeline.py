import os

import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.helpers import query_to_df
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from etl.consistency import AggregatorStep
from etl.helpers import clean_tables

from .static import (COLUMNS_RENAME, COUNTRIES_DICT, HS_DICT, REGIMEN_DICT,
                     UBIGEO_DICT, UNIT_DICT)

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        # Read file
        df = pd.read_csv(
            params["filename"],
            sep="|",
            encoding="latin-1",
            low_memory=False,
            dtype={"ubigeo": "str"},
        )

        # Creates missing ubigeo if ubigeo column not in dataframe
        if "ubigeo" not in df.columns:
            df["ubigeo"] = "999999"

        # Renames columns to standard names
        df.rename(columns=COLUMNS_RENAME, inplace=True)

        # Select columns to work on next steps
        df = df[['trade_flow_id', 'aduana_id', 'country_id', 'shipment_date_id', 'report_date_id', 'hs6_id', 'trade_value', 'net_weight_value', 'unit', 'quantity', 'ubigeo']].copy()

        # Transforms NANDINA code to HS code (6-digit level)
        df['hs6_id'] = df['hs6_id'].fillna(0)
        df['hs6_id'] = df['hs6_id'].astype(str).str.replace('.','').str[:-4]
        df['hs6_id'] = df['hs6_id'].str.zfill(6)

        # Replaces 2-digit country codes to 3-digit level ones
        dim_country_query = "SELECT iso2, iso3 FROM dim_shared_country"
        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))
        countries = query_to_df(db_connector, raw_query=dim_country_query)

        DICT_COUNTRIES = dict(zip(countries["iso2"], countries["iso3"]))

        df["country_id"] = df["country_id"].str.lower()
        df["country_id"].replace(COUNTRIES_DICT, inplace=True)
        df["country_id"] = df["country_id"].replace(DICT_COUNTRIES)

        # Transform exports and imports names to numeric codes (Imports: 1, Exports: 2)
        df['trade_flow_id'].replace(REGIMEN_DICT, inplace=True)

        # Transforms defined unities to standard SUNAT unities
        dim_unit_query = "SELECT * FROM dim_shared_customs_unities"
        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))
        unities = query_to_df(db_connector, raw_query=dim_unit_query)

        DICT_UNITIES = dict(zip(unities["measure_unit_id"], unities["measure_unit_name"]))

        df['unit'] = df['unit'].str.strip()
        missing_value = [x for x in list(df.unit.unique()) if x not in list(DICT_UNITIES.keys())]

        if len(missing_value) > 1:
            df["unit"].replace({missing_value[0]: "XX"}, inplace=True)

        df['unit'] = df['unit'].fillna("XX")
        
        # Special replaces to fix bad formatting or missing elements
        df['unit'].replace(UNIT_DICT, inplace=True)
        df['hs6_id'].replace(HS_DICT, inplace=True)
        df['ubigeo'].replace(UBIGEO_DICT, inplace=True)

        # Date formatting to integers values
        df["shipment_date_id"] = df["shipment_date_id"].astype(float).fillna(0)
        df["report_date_id"] = df["report_date_id"].astype(float).fillna(0)
        
        df["shipment_date_id"] = df.apply(
            lambda x: x["report_date_id"]
            if x["shipment_date_id"] == 0
            else x["shipment_date_id"],
            axis=1,
        )
        df["report_date_id"] = df.apply(
            lambda x: x["shipment_date_id"]
            if x["report_date_id"] == 0
            else x["report_date_id"],
            axis=1,
        )

        df["shipment_date_id"] = df["shipment_date_id"].astype(int)
        df["report_date_id"] = df["report_date_id"].astype(int)

        df["aduana_id"] = df["aduana_id"].astype(int)

        df["trade_flow_id"] = df["trade_flow_id"].astype(int)

        # Group values to reduce DataFrame size before load step
        df = df.groupby(by=[
            "trade_flow_id",
            "aduana_id",
            "country_id",
            "shipment_date_id",
            "report_date_id",
            "hs6_id",
            "unit",
            "ubigeo"
        ]).sum()

        df = df.reset_index()

        return df

class SUNATPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))

        dtypes = {
            "trade_flow_id":     "UInt8",
            "aduana_id":         "UInt16",
            "country_id":        "String",
            "shipment_date_id":  "UInt32",
            "report_date_id":    "UInt32",
            "hs6_id":            "String",
            "unit":              "String",
            "ubigeo":            "String",
            "trade_value":       "Float64",
            "net_weight_value":  "Float64",
            "quantity":          "Float64",
        }

        transform_step = TransformStep()

        agg_step = AggregatorStep("sunat_subnational_trade",
                                  measures=["trade_value", "net_weight_value", "quantity"])

        load_step = LoadStep(
            "sunat_subnational_trade",
            connector=db_connector,
            if_exists="append",
            pk=["trade_flow_id", "aduana_id", "country_id", "shipment_date_id", "report_date_id", "hs6_id", "unit", "ubigeo"],
            dtype=dtypes,
            nullable_list=[],
        )

        return [transform_step, agg_step, load_step]

def run_pipeline(params: dict):
    clean_tables("sunat_subnational_trade", params["connector"])
    pp = SUNATPipeline()

    folders = [
        os.path.join(params["datasets"], "20200318", "180320 Inf. Administrativa SUNAT"),
        os.path.join(params["datasets"], "20201214", "Importaciones SUNAT")
    ]

    for filelist in folders:
        for filename in os.listdir(filelist):

            print('Ingesting: {}'.format(filename))

            pp_params = {"filename": os.path.join(filelist, filename)}
            pp_params.update(params)
            pp.run(pp_params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline(
        {"connector": path.join(__dirname, "..", "conns.yaml"), "datasets": sys.argv[1]}
    )
