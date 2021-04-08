import pandas as pd
from bamboo_lib.helpers import grab_parent_dir
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
path = grab_parent_dir("../../") + "/datasets/20200318"

depto_dict = {
    "Amazonas  ": "01",
    "Áncash": "02",
    "Apurímac": "03",
    "Arequipa": "04",
    "Ayacucho": "05",
    "Cajamarca": "06",
    "Callao": "07",
    "Cusco": "08",
    "Huancavelica": "09",
    "Huánuco": "10",
    "Ica": "11",
    "Junín": "12",
    "La Libertad": "13",
    "Lambayeque": "14",
    "Lima": "15",
    "Loreto ": "16",
    "Madre de Dios ": "17",
    "Moquegua": "18",
    "Pasco": "19",
    "Piura": "20",
    "Puno": "21",
    "San Martín": "22",
    "Tacna": "23",
    "Tumbes": "24",
    "Ucayali": "25"
}

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        # Loading data
        df1 = pd.read_excel("{}/{}/{}".format(path, "D. Sociales", "D.37.xlsx"), skiprows = (0,1,2))[4:117]
        df2 = pd.read_excel("{}/{}/{}".format(path, "D. Sociales", "D.38.xlsx"), skiprows = (0,1,2))[1:114]
        df3 = pd.read_excel("{}/{}/{}".format(path, "D. Sociales", "D.39.xlsx"), skiprows = (0,1,2))[1:88]

        dframes_ = [df1, df2, df3]
        np_rows_ = [{0:59, 1:64}, {0:59, 1:64}, {0:45, 1:51}]

        # For each dataframe, 
        temp = []
        for i in range(0,3):
            df = dframes_[i].copy()
            df.rename(columns = {"Departamento/\nNivel Educativo": "ubigeo", "Nivel educativo /\nDepartamento": "ubigeo", "Departamento de inscripción": "ubigeo", "Ámbito geográfico": "ubigeo", "Departamento": "ubigeo", "Ámbito geográfico": "ubigeo"}, inplace = True)
            df["ubigeo"].replace({"Región Lima  1/": "Lima", "Lima  Metropolitana" : "Lima", "Prov. Const. del Callao": "Callao", "    -Inicial": "Inicial", "    -Primaria ": "Primaria", "    -Secundaria": "Secundaria", "    Inicial": "Inicial", "    Primaria ": "Primaria", "    Secundaria": "Secundaria", "    -Básica Alternativa": "Básica Alternativa", "    -Básica Especial": "Básica Especial", "    -Técnico Productiva": "Técnico Productiva", "    -Superior No Universitaria": "Superior No Universitaria", "    -Superior Universitaria": "Superior Universitaria"}, inplace = True)
            df.iloc[np_rows_[i][0]:np_rows_[i][1], 0] = pd.np.nan
            temp.append(df)

        df1, df2, df3 = temp

        # Assign migration flow value
        df1 = df1[df1["ubigeo"].notna()].copy()
        df2 = df2[df2["ubigeo"].notna()].copy()
        df3 = df3[df3["ubigeo"].notna()].copy()
        df1["nivel_educativo"] = pd.np.nan
        df2["nivel_educativo"] = pd.np.nan
        df3["nivel_educativo"] = pd.np.nan

        # Splittin column between geography data and scholarship data
        df1.loc[df1["ubigeo"].str.contains("Inicial|Primaria|Secundaria"), "nivel_educativo"] = df1["ubigeo"]
        df1.loc[df1["ubigeo"].str.contains("Inicial|Primaria|Secundaria"), "ubigeo"] = pd.np.nan
        df2.loc[df2["ubigeo"].str.contains("Alternativa|Especial|Productiva"), "nivel_educativo"] = df2["ubigeo"]
        df2.loc[df2["ubigeo"].str.contains("Alternativa|Especial|Productiva"), "ubigeo"] = pd.np.nan
        df3.loc[df3["ubigeo"].str.contains("Superior"), "nivel_educativo"] = df3["ubigeo"]
        df3.loc[df3["ubigeo"].str.contains("Superior"), "ubigeo"] = pd.np.nan
        df1["ubigeo"].fillna(method="ffill", inplace = True)
        df2["ubigeo"].fillna(method="ffill", inplace = True)
        df3["ubigeo"].fillna(method="ffill", inplace = True)
        df1.dropna(axis=0, how="any", inplace = True)
        df2.dropna(axis=0, how="any", inplace = True)
        df3.dropna(axis=0, how="any", inplace = True)

        # Melt of the dataframes to append later
        df1 = pd.melt(df1, id_vars = ["ubigeo", "nivel_educativo"], value_vars = [2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018], var_name = "year", value_name = "estudiantes") 
        df2 = pd.melt(df2, id_vars = ["ubigeo", "nivel_educativo"], value_vars = [2007, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018], var_name = "year", value_name = "estudiantes") 
        df3 = pd.melt(df3, id_vars = ["ubigeo", "nivel_educativo"], value_vars = [2007, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018], var_name = "year", value_name = "estudiantes") 

        # Append 3 datasets
        df = df1.append(df2, sort = True)
        df = df.append(df3, sort = True)

        # Removing non number values, to gruopby step (Lima issue)
        df["estudiantes"].replace({"…": 0}, inplace = True)
        df = df.groupby(["ubigeo", "year", "nivel_educativo"]).sum().reset_index()

        # Replacing educational levels with id's (schema)
        df["nivel_educativo"].replace({'Inicial': 1, 'Primaria': 2, 'Secundaria': 3, 'Básica Alternativa': 4,
                                        'Básica Especial': 5, 'Técnico Productiva': 6, 'Superior No Universitaria': 7,
                                        'Superior Universitaria': 8}, inplace = True)

        # Creating ubigeo id"s
        df["ubigeo"].replace(depto_dict, inplace = True)

        return df

class met_education_y_level_dep_Pipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "year":                          "UInt16",
            "ubigeo":                        "String",
            "nivel_educativo":               "UInt16",
            "estudiantes":                   "Float"
        }

        transform_step = TransformStep()
        load_step = LoadStep(
            "met_education_y_level_dep", db_connector, if_exists="drop", pk=["ubigeo"], dtype=dtype,
        )

        return [transform_step, load_step]


if __name__ == "__main__":
    pp = met_education_y_level_dep_Pipeline()
    pp.run({})
