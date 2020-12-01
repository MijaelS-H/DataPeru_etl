from os import path
import pandas as pd
from bamboo_lib.helpers import query_to_df
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from etl.consistency import AggregatorStep

sector_dict = {
    'Minería': 1,
    'Comunicaciones': 2,
    'Finanzas': 3,
    'Energía': 4,
    'Industria': 5,
    'Comercio': 6, 
    'Petróleo': 7, 
    'Servicios': 8, 
    'Transporte': 9, 
    'Construcción': 10,
    'Pesca': 11,
    'Turismo': 12, 
    'Agricultura': 13,
    'Vivienda': 14,
    'Silvicultura': 15
}

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        # Loading data
        df = pd.read_excel(io = path.join(params["datasets"],"20200318", "A. Economía", "A.187.xlsx"), skiprows = (0,1,2,4))[0:15]

        df.dropna(axis=1, how="all", inplace = True)

        df.rename(columns = {"Sector": "sector", "2018 P/": 2018}, inplace = True)

        df["sector"].replace(sector_dict, inplace = True)

        # Changing types to certain columns
        for item in df.columns:
            df[item] = df[item].astype(float)

        # Melt step
        df = pd.melt(df, id_vars = "sector", value_vars = [2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018] ,var_name = "year", value_name = "ied_millones_USD")

        df["nation_id"] = "per"

        return df

class proinversion_fdi_y_origin_sector_pipeline(EasyPipeline):
  
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))

        dtype = {
            "nation_id":                          "String",
            "year":                               "UInt16",
            "ied_millones_USD":                   "Float32",
            "sector":                             "UInt8"
            }

        transform_step = TransformStep()
        agg_step = AggregatorStep("proinversion_fdi_y_origin_sector", measures=["ied_millones_USD"])
        load_step = LoadStep("proinversion_fdi_y_origin_sector", db_connector, if_exists="drop", pk=["nation_id"], dtype=dtype)

        return [transform_step, agg_step, load_step]

def run_pipeline(params: dict):
    pp = proinversion_fdi_y_origin_sector_pipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
