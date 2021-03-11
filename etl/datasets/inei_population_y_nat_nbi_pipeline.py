from os import path
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline , Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from etl.consistency import AggregatorStep

geography_dict = {"Área de residencia":"area_residencia", "Región natural": "region_natural", "Dominio geográfico": "dominio_region", "Urbana": "urbana", "Rural": "rural", "Costa": "costa", "Sierra": "sierra", "Selva": "selva", "Costa urbana": "costa_urbana", "Costa rural": "costa_rural", "Sierra urbana": "sierra_urbana", "Sierra rural": "sierra_rural", "Selva urbana": "selva_urbana", "Selva rural": "selva_rural"}
ids_dict = {"area_residencia": 1, "region_natural": 2, "dominio_region": 3, "urbana": 1, "rural": 2, "costa": 3, "sierra": 4, "selva": 5, "costa_urbana": 6, "costa_rural": 7, "sierra_urbana": 8, "sierra_rural": 9, "selva_urbana": 10, "selva_rural": 11}

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        # Loading data
        df = pd.read_excel(io = path.join(params["datasets"],"20200318", "D. Sociales", "D.3.xlsx"), skiprows = (0,1,2), usecols = "A:K")[6:20]

        df.rename(columns={"Ámbito geográfico": "sub_ambito_geografico"}, inplace = True )

        df["sub_ambito_geografico"].replace(geography_dict, inplace = True)

        df["ambito_geografico"] = pd.np.nan

        df["nation_id"] = "per"

        df.loc[df["sub_ambito_geografico"].str.contains("area_residencia|region_natural|dominio_region"), "ambito_geografico"] = df["sub_ambito_geografico"]
        df.loc[df["sub_ambito_geografico"].str.contains("area_residencia|region_natural|dominio_region"), "sub_ambito_geografico"] = pd.np.nan

        df["ambito_geografico"].fillna(method="ffill", inplace = True)

        df.dropna(axis=0, how='any', inplace = True)

        df = pd.melt(df, id_vars = ["nation_id", "ambito_geografico", "sub_ambito_geografico"], value_vars = [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018], var_name = "year", value_name = "at_less_1_nbi_perc")

        df["ambito_geografico"].replace(ids_dict, inplace = True)
        df["sub_ambito_geografico"].replace(ids_dict, inplace = True)

        return df

class inei_population_y_nat_nbi(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))

        dtype = {
            "nation_id":                  "String",
            "year":                       "UInt16",
            "ambito_geografico":          "UInt8",
            "sub_ambito_geografico":      "UInt8",
            "at_less_1_nbi_perc":         "Float32"
        }

        transform_step = TransformStep()
        agg_step = AggregatorStep("inei_population_y_nat_nbi", measures=["at_less_1_nbi_perc"])
        load_step = LoadStep("inei_population_y_nat_nbi", db_connector, if_exists="drop", pk=["nation_id"], dtype=dtype)

        return [transform_step, agg_step, load_step]

def run_pipeline(params: dict):
    pp = inei_population_y_nat_nbi()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
