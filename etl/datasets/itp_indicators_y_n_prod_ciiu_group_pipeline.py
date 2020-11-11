from os import path
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from etl.consistency import AggregatorStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        # Loading data
        df1 = pd.read_excel(io = path.join(params["datasets"], "20200318", "A. Economía", "A.95.xlsx"), skiprows = (0,1,2))[2:74]
        df2 = pd.read_excel(io = path.join(params["datasets"], "20200318", "A. Economía", "A.96.xlsx"), skiprows = (0,1,2,3))[2:83]
        df3 = pd.read_excel(io = path.join(params["datasets"], "20200318", "A. Economía", "A.99.xlsx"), skiprows = (0,1,2,3))[2:40]

        # Common steps
        for item in [df1,df2, df3]:
            item.rename(columns = {"División": "group_id", "Producto": "product_name", "Unnamed: 2" : "unit", "de" : "unit", "   2015": 2015, "2018 P/": 2018}, inplace = True)
            item["group_id"].fillna(method = "ffill", inplace = True)
            item.drop(item.loc[item["unit"].isna()].index, axis = 0, inplace = True)

        # Renaming columns to understandable names
        df_1 = pd.melt(df1, id_vars = ["group_id", "product_name", "unit"], value_vars = [2012, 2013, 2014, 2015, 2016, 2017, 2018], var_name = "year", value_name = "produccion_industrial_anual")
        df_2 = pd.melt(df2, id_vars = ["group_id", "product_name", "unit"], value_vars = [2012, 2013, 2014, 2015, 2016, 2017, 2018], var_name = "year", value_name = "produccion_industrial_anual")
        df_3 = pd.melt(df3, id_vars = ["group_id", "product_name", "unit"], value_vars = [2012, 2013, 2014, 2015, 2016, 2017, 2018], var_name = "year", value_name = "produccion_industrial_anual")

        # Formatting data
        df = df_1.append(df_2, sort=False)
        df = df.append(df_3, sort=False)

        df['product_name'] = df['product_name'].str.strip()
        df['product_name'] = df['product_name'].str.replace("  ", " ")

        df["group_id"] = df["group_id"].astype(str)
        df["year"] = df["year"].astype(int)
        df["produccion_industrial_anual"] = df["produccion_industrial_anual"].astype(float)

        df["ubigeo"] = "per"

        return df

class itp_indicators_y_n_prod_ciiu_group_pipeline(EasyPipeline):

    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))
        dtype = {
            "ubigeo":                                   "String",
            "group_id":                                 "String",
            "product_name":                             "String",
            "unit":                                     "String",
            "year":                                     "UInt16",
            "produccion_industrial_anual":              "Float64"
            }

        transform_step = TransformStep()
        agg_step = AggregatorStep("itp_indicators_y_n_prod_ciiu_group", measures=["produccion_industrial_anual"])
        load_step = LoadStep("itp_indicators_y_n_prod_ciiu_group", db_connector, if_exists="drop", pk=["ubigeo"], dtype=dtype, nullable_list=["produccion_industrial_anual"])

        return [transform_step, agg_step, load_step]

def run_pipeline(params: dict):
    pp = itp_indicators_y_n_prod_ciiu_group_pipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
