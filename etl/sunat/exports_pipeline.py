import os

import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.helpers import query_to_df
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from etl.consistency import AggregatorStep

from .static import (COLUMNS_RENAME, COUNTRIES_DICT, HS_DICT, REGIMEN_DICT,
                     UBIGEO_DICT, UNIT_DICT)


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.DataFrame()

        filelist = os.path.join(params["datasets"], "20200318", "180320 Inf. Administrativa SUNAT")
        
        # Open and read exports files in defined path
        for filename in os.listdir(filelist):
            filepath = os.path.join(filelist, filename)
            _df = pd.read_csv(
                filepath,
                sep="|",
                encoding="latin-1",
                low_memory=False,
                dtype={"ubigeo": "str"},
            )

            df = df.append(_df)

        # Select columns to use
        df = df[["regimen", "cadu", "cpaides", "femb", "freg", "cnan", "vfobserdol", "vpesnet", "tunifis", "qunifis", "ubigeo"]].copy()

        # Rename columns to comprensive name
        df.rename(columns=COLUMNS_RENAME, inplace=True)

        # Replace NANDINA code by HS code at 6-digit level
        df["hs6_id"] = df["hs6_id"].str.replace(".", "").str[:-4]

        # Replace countries iso2 to iso3
        dim_country_query = "SELECT iso2, iso3 FROM dim_shared_country"
        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))
        countries = query_to_df(db_connector, raw_query=dim_country_query)

        DICT_COUNTRIES = dict(zip(countries["iso2"], countries["iso3"]))

        df["country_id"] = df["country_id"].str.lower()
        df["country_id"].replace(COUNTRIES_DICT, inplace=True)
        df["country_id"] = df["country_id"].replace(DICT_COUNTRIES)

        df["trade_flow_id"].replace(REGIMEN_DICT, inplace=True)
        df["unit"].replace(UNIT_DICT, inplace=True)
        df["hs6_id"].replace(HS_DICT, inplace=True)
        df["ubigeo"].replace(UBIGEO_DICT, inplace=True)

        df["shipment_date_id"] = df.apply(
            lambda x: x["registry_date_id"]
            if x["shipment_date_id"] == 0
            else x["shipment_date_id"],
            axis=1,
        )
        df["registry_date_id"] = df.apply(
            lambda x: x["shipment_date_id"]
            if x["registry_date_id"] == 0
            else x["registry_date_id"],
            axis=1,
        )

        df = df.groupby(
            by=[
                "trade_flow_id",
                "aduana_id",
                "country_id",
                "shipment_date_id",
                "registry_date_id",
                "hs6_id",
                "unit",
                "ubigeo",
            ]
        ).sum()

        df = df.reset_index()

        return df


class ExportsPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))

        dtypes = {
            "trade_flow_id":     "UInt8",
            "aduana_id":         "UInt16",
            "country_id":        "String",
            "shipment_date_id":  "UInt32",
            "registry_date_id":  "UInt32",
            "hs6_id":            "String",
            "unit":              "String",
            "ubigeo":            "String",
            "trade_value":       "Float64",
            "net_weight_value":  "Float64",
            "quantity":          "Float64",
        }

        transform_step = TransformStep()

        agg_step = AggregatorStep("sunat_exports",
                                  measures=["trade_value", "net_weight_value", "quantity"])

        load_step = LoadStep(
            "sunat_exports",
            connector=db_connector,
            if_exists="drop",
            pk=["trade_flow_id", "aduana_id", "country_id", "shipment_date_id", "registry_date_id", "hs6_id", "unit", "ubigeo"],
            dtype=dtypes,
            nullable_list=[],
        )

        return [transform_step, agg_step, load_step]


def run_pipeline(params):
    pp = ExportsPipeline()
    pp.run(params)


if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline(
        {"connector": path.join(__dirname, "..", "conns.yaml"), "datasets": sys.argv[1]}
    )
