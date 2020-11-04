import pandas as pd
from bamboo_lib.helpers import grab_parent_dir
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
path = grab_parent_dir("../../") + "/datasets/20200318"

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        # Loading data
        df1 = pd.read_excel(io = "{}/{}/{}".format(path, "D. Sociales", "D.25.xlsx"), skiprows = range(0,5))[2:131]
        df2 = pd.read_excel(io = "{}/{}/{}".format(path, "D. Sociales", "D.26.xlsx"), skiprows = (0,1,2))[2:124]

        # For each dataframe, 
        for item in [df1, df2]:
            item.rename(columns = {"Causas / Grupos de edad": "causas", "Causas /Grupos de edad": "causas"}, inplace = True)
            item.dropna(thresh = 3, axis=0, inplace = True)
            item["age_group"] = pd.np.nan
            item["causas"].replace({"   Menores de 15 años": "Menores de 15 años", "   De 15 a 24 años": "De 15 a 24 años", "   De 25 a 49": "De 25 a 49 años", "   50 y más años": "50 y más años", "   De 25 a 49 años": "De 25 a 49 años", "  50 y más años": "50 y más años", "   No identificada": "No identificada", "   Ignorado": "Ignorado"}, inplace = True)
            item.loc[item["causas"].str.contains("años|Ignorado|identifi"), "age_group"] = item["causas"]
            item.loc[item["causas"].str.contains("años|Ignorado|identifi"), "causas"] = pd.np.nan
            item["causas"].fillna(method="ffill", inplace = True)

        # Assign migration flow value
        df1 = df1[df1["age_group"].notna()]
        df2 = df2[df2["age_group"].notna()]

        # Melt of the dataframes to append later
        df1 = pd.melt(df1, id_vars = ["causas", "age_group"], value_vars = [2007, 2008, 2009, 2010, 2011, 2012,
                        2013, 2014, 2015, 2016, 2017], var_name = "year", value_name = "poblacion")
        df2 = pd.melt(df2, id_vars = ["causas", "age_group"], value_vars = [2007, 2008, 2009, 2010, 2011, 2012,
                        2013, 2014, 2015, 2016, 2017], var_name = "year", value_name = "poblacion")

        df1["sexo"] = "mujer"
        df2["sexo"] = "hombre"

        # Replacing non number values
        df1["poblacion"].replace({"-": pd.np.nan}, inplace = True)
        df2["poblacion"].replace({"-": pd.np.nan}, inplace = True)

        # Append the 2 datasets
        df = df1.append(df2)

        df["causas"].replace({"Causas externas de morbilidad y mortalidad 1/": "Causas externas de morbilidad y mortalidad"}, inplace = True)
        df["poblacion"].replace({"-": pd.np.nan, " -": pd.np.nan}, inplace = True)

        df["year"] = df["year"].astype(int)
        df["poblacion"] = df["poblacion"].astype(float)

        df["ubigeo"] = "per"

        return df

class minsa_health_y_gender_age_nat_Pipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "ubigeo":                        "String",
            "year":                          "UInt16",
            "causas":                        "String",
            "sexo":                          "String",
            "age_group":                     "String",
            "poblacion":                     "Float"
        }

        transform_step = TransformStep()
        load_step = LoadStep(
            "minsa_health_y_gender_age_nat", db_connector, if_exists="drop", pk=["ubigeo"], dtype=dtype, nullable_list=["poblacion"]
        )

        return [transform_step, load_step]


if __name__ == "__main__":
    pp = minsa_health_y_gender_age_nat_Pipeline()
    pp.run({})
