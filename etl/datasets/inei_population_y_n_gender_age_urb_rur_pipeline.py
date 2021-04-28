from os import path
import numpy as np
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from etl.consistency import AggregatorStep

age_dict = {
    ' 0 - 4' : 1,
    ' 5 - 9' : 2,
    ' 10 - 14 ' : 3,
    ' 15 - 19 ' : 4,
    ' 20 - 24 ' : 5,
    ' 25 - 29 ' : 6,
    ' 30 - 34 ' : 7,
    ' 35 - 39 ' : 8,
    ' 40 - 44 ' : 9,
    ' 45 - 49 ' : 10,
    ' 50 - 54 ' : 11,
    ' 55 - 59 ' : 12,
    ' 60 - 64 ' : 13, 
    ' 65 - 69 ' : 14, 
    ' 70 - 74 ' : 15,
    ' 75 - 79 ' : 16,
    ' 80 y más ' : 17
}

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        # Loading data
        df1 = pd.read_excel(path.join(params["datasets"], "B_Poblacion_y_Vivienda", "B.10.xlsx"), skiprows = (0,1,2,3))[20:57]
        df2 = pd.read_excel(path.join(params["datasets"], "B_Poblacion_y_Vivienda", "B.11.xlsx"), skiprows = (0,1,2,3))[20:57]

        # Creating datasets by selecting specific columns related to gender, for both datasets, adding migration flow
        for item in [df1, df2]:
            item.drop(38, axis=0, inplace = True)
            item["edad"] = np.nan
            item.loc[item["Unnamed: 0"].str.contains("-|y"), "edad"] = item["Unnamed: 0"]
            item.loc[item["Unnamed: 0"].str.contains("-|y"), "Unnamed: 0"] = np.nan
            item["Unnamed: 0"].fillna(method="ffill", inplace = True)
            item.dropna(axis=0, how="any", inplace = True)
            item["Unnamed: 0"].replace({"Hombres": 1, "Mujeres": 2}, inplace = True)
            item["edad"].replace(age_dict, inplace = True)
            item.rename(columns = {"Unnamed: 0": "sexo"}, inplace = True)

        # Shorting dataframes to common years
        df1 = pd.melt(df1, id_vars = ["sexo", "edad"], value_vars = [2000, 2005, 2010, 2013, 2015, 2016, 2017], var_name = "year", value_name = "poblacion")
        df2 = pd.melt(df2, id_vars = ["sexo", "edad"], value_vars = [2000, 2005, 2010, 2013, 2015, 2016, 2017], var_name = "year", value_name = "poblacion")

        # Adding population type
        df1["tipo_poblacion"] = "1"
        df2["tipo_poblacion"] = "2"

        # Append datasets
        df = df1.append(df2, sort=True)

        # Changes from float to int
        df["poblacion"] = df["poblacion"].astype(int)

        # String type ubigeo
        df["nation_id"] = "per"

        return df

class inei_population_y_n_gender_age_urb_rur_pipeline(EasyPipeline):
  
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))
        dtype = {
            "nation_id":                      "String",
            "edad":                           "UInt8",
            "tipo_poblacion":                 "UInt8",
            "sexo":                           "UInt8",
            "year":                           "UInt16",
            "poblacion":                      "UInt32"
        }

        transform_step = TransformStep()
        agg_step = AggregatorStep("inei_population_y_n_gender_age_urb_rur", measures=["poblacion"])
        load_step = LoadStep("inei_population_y_n_gender_age_urb_rur", db_connector, if_exists="drop", pk=["nation_id"], dtype=dtype)

        return [transform_step, load_step]

def run_pipeline(params: dict):
    pp = inei_population_y_n_gender_age_urb_rur_pipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
