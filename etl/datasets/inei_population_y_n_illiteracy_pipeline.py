from os import path
import numpy as np
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline , Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from etl.consistency import AggregatorStep

geography_dict = {"Área de residencia":"area_residencia", "Región natural": "region_natural", "Dominio geográfico": "dominio_region", "Urbana": "urbana", "Rural": "rural", "Costa": "costa", "Sierra": "sierra", "Selva": "selva", "Departamento": "departamento"}
ids_dict = {"area_residencia": 1, "region_natural": 2, "dominio_region": 3, "urbana": 1, "rural": 2, "costa": 3, "sierra": 4, "selva": 5}
depto_dict = {"Amazonas": 1, "Áncash": 2, "Apurímac": 3, "Arequipa": 4, "Ayacucho": 5, "Cajamarca": 6, "Callao ": 7, "Cusco": 8, "Huancavelica": 9, "Huánuco": 10, "Ica": 11, "Junín": 12, "La Libertad": 13, "Lambayeque": 14, "Región Lima 2/": 15, "Loreto": 16, "Madre de Dios": 17, "Moquegua": 18, "Pasco": 19, "Piura": 20, "Puno": 21, "San Martín": 22, "Tacna": 23, "Tumbes": 24, "Ucayali": 25}

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        # Loading data
        df = pd.read_excel(path.join(params["datasets"], "D_Sociales", "D.56.xlsx"), skiprows = range(0,6), usecols = "A:N")[3:37]

        df.drop(df.loc[df["Ámbito geográfico"].str.contains("Provincia ")].index, inplace=True)

        df.rename(columns={"Ámbito geográfico": "sub_ambito_geografico"}, inplace = True )

        df["sub_ambito_geografico"].replace(geography_dict, inplace = True)

        df["sub_ambito_geografico"].replace(depto_dict, inplace = True)

        df["ambito_geografico"] = np.nan
        df["ubigeo"] = np.nan

        df["sub_ambito_geografico"] = df["sub_ambito_geografico"].astype("str")

        df.loc[df["sub_ambito_geografico"].str.contains("area_residencia|region_natural"), "ambito_geografico"] = df["sub_ambito_geografico"]
        df.loc[df["sub_ambito_geografico"].str.contains("1|2|3|4|5|6|7|8|9"), "ubigeo"] = df["sub_ambito_geografico"].astype("str").str.zfill(2)

        df["ambito_geografico"].fillna(method="ffill", inplace = True)

        df.loc[df["sub_ambito_geografico"].str.contains("departamento|1|2|3|4|5|6|7|8|9"), "ambito_geografico"] = np.nan

        df.loc[df["sub_ambito_geografico"].str.contains("area_residencia|region_natural|departamento|1|2|3|4|5|6|7|8|9"), "sub_ambito_geografico"] = np.nan

        df.dropna(axis = 0, thresh = 2, inplace = True)

        df.replace("-", np.nan, inplace=True)

        df["id"] = range(1, 1+len(df))

        df = pd.melt(df, id_vars = ["id","ubigeo", "ambito_geografico", "sub_ambito_geografico"], value_vars = [2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018], var_name = "year", value_name = "illiteracy_15yrs_more_perc")

        df["ambito_geografico"].replace(ids_dict, inplace = True)
        df["sub_ambito_geografico"].replace(ids_dict, inplace = True)

        return df

class inei_population_y_n_illiteracy(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))

        dtype = {
            "id":                             "UInt8",
            "ubigeo":                         "String",
            "year":                           "UInt16",
            "ambito_geografico":              "UInt8",
            "sub_ambito_geografico":          "UInt8",
            "illiteracy_15yrs_more_perc":     "Float32"
        }

        transform_step = TransformStep()
        agg_step = AggregatorStep("inei_population_y_n_illiteracy", measures=["illiteracy_15yrs_more_perc"])
        load_step = LoadStep("inei_population_y_n_illiteracy", db_connector, if_exists="drop", pk=["id"], dtype=dtype,
                            nullable_list = ["ubigeo", "ambito_geografico", "sub_ambito_geografico"])

        return [transform_step, load_step]

def run_pipeline(params: dict):
    pp = inei_population_y_n_illiteracy()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
