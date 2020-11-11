from os import path
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline , Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from etl.consistency import AggregatorStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        # Loading data
        df1 = pd.read_excel(io = path.join(params["datasets"],"20200318", "B. Población y Vivienda", "B.27.xls"), skiprows = (0,1,2))[2:171]
        df2 = pd.read_excel(io = path.join(params["datasets"],"20200318", "B. Población y Vivienda", "B.28.xls"), skiprows = (0,1,2))[2:171]

        # For each dataframe
        for item in [df1, df2]:
            item.dropna(thresh = 3, axis=0,  inplace = True)
            item.drop("Unnamed: 1", axis=1, inplace = True)
            item["Unnamed: 0"] = item["Unnamed: 0"].astype(str)
            item.loc[item["Unnamed: 0"].str.contains("20"), "Unnamed: 2"] = item["Unnamed: 0"]
            item["Unnamed: 2"].fillna(method="ffill", inplace = True)
            item.drop(item.loc[item["Unnamed: 0"] == item["Unnamed: 2"]].index, inplace = True)
            item.rename(columns = {"Unnamed: 0": "continente", "Unnamed: 2": "year"}, inplace = True)
            item["continente"].replace({"Otros 1/": "Otros"}, inplace = True)

        # Renaming column to same format for append later step
        df1.rename(columns = {"0 -  9": "0 - 9", "10 - 1 9": "10 - 19"}, inplace = True)

        # Assign migration flow value
        df1["inmigration_flow"] = 1
        df2["inmigration_flow"] = 2

        # Melt of the dataframes to append later
        df1 = pd.melt(df1, id_vars = ["year", "continente", "inmigration_flow"], value_vars = ["0 - 9", "10 - 19", "20 - 29", "30 - 39", "40 - 49", "50 - 59", "60 - 69", "70 - 79", "80 y más"], var_name = "age_group", value_name = "poblacion")
        df2 = pd.melt(df2, id_vars = ["year", "continente", "inmigration_flow"], value_vars = ["0 - 9", "10 - 19", "20 - 29", "30 - 39", "40 - 49", "50 - 59", "60 - 69", "70 - 79", "80 y más"], var_name = "age_group", value_name = "poblacion")

        # Replacing non number values
        df1["poblacion"].replace({"-": pd.np.nan}, inplace = True)
        df2["poblacion"].replace({"-": pd.np.nan}, inplace = True)

        # Append the 2 datasets
        df = df1.append(df2)
        df["year"].replace({"2010 R/": 2010, "2011 P/": 2011}, inplace = True)

        df["year"] = df["year"].astype(int)
        df.drop(df.loc[df["continente"] == "América"].index, inplace = True)

        df["ubigeo"] = "per"
        return df

class inei_population_y_age_nat_travel_Pipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))

        dtype = {
            "ubigeo":                        "String",
            "year":                          "UInt16",
            "continente":                    "String",
            "inmigration_flow":              "UInt8",
            "age_group":                     "String",
            "poblacion":                     "UInt32"
        }

        transform_step = TransformStep()
        agg_step = AggregatorStep("inei_population_y_age_nat_travel", measures=["poblacion"])
        load_step = LoadStep("inei_population_y_age_nat_travel", db_connector, if_exists="drop", pk=["ubigeo"], dtype=dtype, nullable_list=["poblacion"])

        return [transform_step, agg_step, load_step]

def run_pipeline(params: dict):
    pp = inei_population_y_age_nat_travel_Pipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
