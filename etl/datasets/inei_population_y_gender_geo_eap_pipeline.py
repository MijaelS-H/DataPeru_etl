from os import path
import numpy as np
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from etl.consistency import AggregatorStep

ids_dict = {"area_residencia": 1, "region_natural": 2, "dominio_region": 3, "urbana": 1, "rural": 2, "costa": 3, "sierra": 4, "selva": 5, "costa_urbana": 6, "costa_rural": 7, "sierra_urbana": 8, "sierra_rural": 9, "selva_urbana": 10, "selva_rural": 11}


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        # Creating inicial empty dataframe
        df = pd.DataFrame(columns = ["Unnamed: 0", "Hombre", "Mujer", "year"])

        # Reading step
        df1 = pd.read_excel(path.join(params["datasets"], "C_Empleo", "C.8.xlsx"), skiprows = (0,1,2,3))[3:11]
        df2 = pd.read_excel(path.join(params["datasets"], "C_Empleo", "C.9.xlsx"), skiprows = (0,1,2,3))[3:11]

        # For each dataset, apply common steps
        for item in [df1,df2]:
            item["ambito_geografico"] = np.nan
            item.rename(columns={"Ámbito geográfico": "sub_ambito_geografico"}, inplace = True)
            item["sub_ambito_geografico"].replace({"Área de residencia":"area_residencia",
                                            "Región natural": "region_natural",
                                            "Dominio geográfico": "dominio_region",
                                            "Urbana": "urbana",
                                            "Rural": "rural",
                                            "Costa ": "costa",
                                            "Sierra": "sierra",
                                            "Selva": "selva"}, inplace = True)
            item["ambito_geografico"] = np.nan
            item.dropna(axis = 0, how = "all", inplace = True)
            item.loc[item["sub_ambito_geografico"].str.contains("area_residencia|region_natural"), "ambito_geografico"] = item["sub_ambito_geografico"]
            item.loc[item["sub_ambito_geografico"].str.contains("area_residencia|region_natural"), "sub_ambito_geografico"] = np.nan
            item["ambito_geografico"].fillna(method="ffill", inplace = True)
            item.dropna(axis = 0, thresh = 2, inplace = True)
            item["nation_id"] = "per"

        # Steps to merge dataframes
        df1 = pd.melt(df1, id_vars = ["nation_id", "ambito_geografico", "sub_ambito_geografico"], value_vars = [2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020], var_name = "year", value_name = "peac_masculina")
        df2 = pd.melt(df2, id_vars = ["nation_id", "ambito_geografico", "sub_ambito_geografico"], value_vars = [2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020], var_name = "year", value_name = "peac_femenina")

        df1["peac_masculina"] = df1["peac_masculina"] * 1000
        df2["peac_femenina"]  = df2["peac_femenina"] * 1000

        df1["code"] = df1["ambito_geografico"] + df1["sub_ambito_geografico"] + df1["year"].astype(str)
        df2["code"] = df2["ambito_geografico"] + df2["sub_ambito_geografico"] + df2["year"].astype(str)

        df = pd.merge(df1, df2[["code", "peac_femenina"]], on = "code", how = "left")

        df["ambito_geografico"].replace(ids_dict, inplace = True)
        df["sub_ambito_geografico"].replace(ids_dict, inplace = True)

        df.drop(["code"], axis = 1, inplace = True)

        # setting corrections to types
        for i in ["peac_masculina", "peac_femenina"]:
            df[i] = df[i].astype(float)

        return df

class inei_population_y_gender_geo_eap_Pipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))

        dtype = {
            "nation_id":                        "String",
            "year":                             "UInt16",
            "ambito_geografico":                "UInt8",
            "sub_ambito_geografico":            "UInt8",
            "peac_masculina":                   "Float32",
            "peac_femenina":                    "Float32",
 
        }

        transform_step = TransformStep()
        agg_step = AggregatorStep("inei_population_y_gender_geo_eap", measures=["peac_masculina", "peac_femenina"])
        load_step = LoadStep("inei_population_y_gender_geo_eap", db_connector, if_exists="drop", pk=["nation_id"], dtype=dtype)

        return [transform_step, load_step]

def run_pipeline(params: dict):
    pp = inei_population_y_gender_geo_eap_Pipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
