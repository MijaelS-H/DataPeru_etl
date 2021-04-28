from os import path

import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from etl.consistency import AggregatorStep

continents_ = ["América del Norte", "América del Centro", "América del Sur", "Europa", "Asia", "África", "Oceanía", "Otros"]
pivotes_ = [[1,2], [5,6], [9,10], [13,14], [17,18], [21,22], [25,26], [29,30]]

CONTINENT_DICT = {
    "América del Norte": "na",
    "América del Centro": "ca",
    "América del Sur": "sa",
    "Europa": "eu",
    "Asia": "as",
    "África": "af",
    "Oceanía": "oc",
    "Otros": "xx"
}

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        # Creating inicial empty dataframe
        df = pd.DataFrame(columns = ["year", "continente", "inmigration_flow", "hombre", "mujer"])

        # Loading data
        df1 = pd.read_excel(path.join(params["datasets"], "B_Poblacion_y_Vivienda","B.25.xls"), skiprows = (0,1,3,4,5))[0:31]
        df2 = pd.read_excel(path.join(params["datasets"], "B_Poblacion_y_Vivienda","B.26.xls"), skiprows = (0,1,3,4,5))[0:31]

        # Transpose dataframes and deleting NaN columns
        df_1 = df1.T
        df_2 = df2.T
        df_1.dropna(axis=1, how="all", inplace = True)
        df_2.dropna(axis=1, how="all", inplace = True)

        # Creating datasets by selecting specific columns related to gender, for both datasets, adding migration flow
        for i in range(0,8):
            pivote = df_1[pivotes_[i]].copy()
            pivote.drop("Continente / Sexo", axis = 0, inplace = True)
            pivote["continente"] = continents_[i]
            pivote.rename(columns = {pivotes_[i][0]: "hombre", pivotes_[i][1]: "mujer"}, inplace = True)
            pivote["year"] = pivote.index
            pivote["inmigration_flow"] = 1
            df = df.append(pivote, sort=True)

        for i in range(0,8):
            pivote = df_2[pivotes_[i]].copy()
            pivote.drop("Continente / Sexo", axis = 0, inplace = True)
            pivote["continente"] = continents_[i]
            pivote.rename(columns = {pivotes_[i][0]: "hombre", pivotes_[i][1]: "mujer"}, inplace = True)
            pivote["year"] = pivote.index
            pivote["inmigration_flow"] = 2
            df = df.append(pivote, sort=True)

        # Replacing typos in years
        df["year"].replace({"2010 R/": 2010, "2011 P/" : 2011}, inplace = True)

        # Melting dataframe
        df = pd.melt(df, id_vars = ["year", "continente", "inmigration_flow"], value_vars = ["hombre", "mujer"], var_name = "sexo", value_name = "poblacion")

        df["sexo"].replace({"hombre": 1, "mujer": 2}, inplace = True)

        df.replace(CONTINENT_DICT, inplace=True)

        df["poblacion"] = df["poblacion"].astype(int)

        df["nation_id"] = "per"
        return df

class inei_population_y_gender_nat_travel_Pipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))

        dtype = {
            "nation_id":                     "String",
            "year":                          "UInt16",
            "continente":                    "String",
            "inmigration_flow":              "UInt8",
            "sexo":                          "UInt8",
            "poblacion":                     "UInt32"
        }

        transform_step = TransformStep()
        agg_step = AggregatorStep("inei_population_y_gender_nat_travel", measures=["poblacion"])
        load_step = LoadStep("inei_population_y_gender_nat_travel", db_connector, if_exists="drop", pk=["nation_id"], dtype=dtype)

        return [transform_step, load_step]

def run_pipeline(params: dict):
    pp = inei_population_y_gender_nat_travel_Pipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
