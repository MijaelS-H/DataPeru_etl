import pandas as pd
from bamboo_lib.helpers import grab_parent_dir
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
path = grab_parent_dir('../../') + "/datasets/20200318"

depto_dict = {"Amazonas": 1, "Amazonas ": 1, "Áncash": 2, "Áncash 1/": 2, "Apurímac": 3, "Arequipa": 4, "Ayacucho": 5, "Cajamarca": 6, "Cajamarca 1/": 6, "Callao": 7, "Prov. Const. del Callao 2/": 7, "Cusco": 8, "Huancavelica": 9, "Huánuco": 10, "Huánuco 1/": 10, "Ica": 11, "Junín": 12, "Junín 1/": 12, "La Libertad": 13, "La Libertad 1/": 13, "Lambayeque": 14, "Lima": 15, "Región Lima 2/": 15, "Región Lima    2/": 15, "Loreto": 16, "Loreto 1/": 16, "Madre de Dios": 17, "Moquegua": 18, "Pasco": 19, "Pasco 1/": 19, "Piura": 20, "Piura ": 20, "Puno": 21, "San Martín": 22, "Tacna": 23, "Tumbes": 24, "Ucayali": 25, "Ucayali 1/": 25}
edu_years = [2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018]
evo_years = [1940, 1961, 1972, 1981, 1993, 2007, 2017]
header_pop = ["ubigeo", 1940, 1961, 1972, 1981, 1993, 2007, 2017]

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        # Data load
        df1 = pd.read_excel(io = '{}/{}/{}'.format(path, 'B. Población y Vivienda', "B.4.xls"), usecols = "A:H", skiprows = range(0,7), header = None)[0:25]
        df2 = pd.read_excel(io = '{}/{}/{}'.format(path, 'B. Población y Vivienda', "B.5.xls"), usecols = "A:H", skiprows = range(0,7), header = None)[0:25]
        df3 = pd.read_excel(io = '{}/{}/{}'.format(path, 'D. Sociales', "D.43.xlsx"), skiprows = (0,1,2,3,5,6,7))[0:26]
        df4 = pd.read_excel(io = '{}/{}/{}'.format(path, 'D. Sociales', "D.42.xlsx"), skiprows = (0,1,2,3,5,6,7))[0:26]

        # Renaming columns from datasets
        df1.columns = header_pop
        df2.columns = header_pop
        df3.rename(columns = {"Departamento": "ubigeo"}, inplace = True)
        df4.rename(columns = {"Departamento": "ubigeo"}, inplace = True)

        # Shorting dataframes to common years
        df4 = df4[["ubigeo", 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018]]

        # Removing no related rows to department geo level
        df3.drop(df3.loc[df3['ubigeo'].str.contains("Provincia")].index, inplace=True)
        df4.drop(df4.loc[df4['ubigeo'].str.contains("Provincia")].index, inplace=True)

        # Replacing name for code of department
        for item in [df1, df2, df3, df4]:
            item["ubigeo"].replace(depto_dict,inplace = True)
            item.replace("-", pd.np.nan, inplace = True)

        # Exchanging values to 1.000 to prevent float errors
        for i in [2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018]:
            df3[i] = df3[i] * 1000
            df4[i] = df4[i] * 1000

        # Type of value for each table
        df1["measure"] = "evo_pob_censada_urb"
        df2["measure"] = "evo_pob_censada_rur"
        df3["measure"] = "matriculados_sist_educa_urb"
        df4["measure"] = "matriculados_sist_educa_rur"

        # Melt step for each table
        df_1 = pd.melt(df1, id_vars = ["ubigeo", "measure"], value_vars = evo_years, var_name = 'year', value_name = "poblacion")
        df_2 = pd.melt(df2, id_vars = ["ubigeo", "measure"], value_vars = evo_years, var_name = 'year', value_name = "poblacion")
        df_3 = pd.melt(df3, id_vars = ["ubigeo", "measure"], value_vars = edu_years, var_name = 'year', value_name = "poblacion")
        df_4 = pd.melt(df4, id_vars = ["ubigeo", "measure"], value_vars = edu_years, var_name = 'year', value_name = "poblacion")

        # Append tables to df
        df = pd.DataFrame(columns=['ubigeo', 'measure', 'year', 'poblacion'])
        for item in [df_1, df_2, df_3, df_4]:
            df = df.append(item)

        # String type ubigeo
        df["ubigeo"] = df["ubigeo"].astype("str").str.zfill(2)

        return df

class inei_population_y_n_dep_urb_rur_pipeline(EasyPipeline):
  
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))
        dtype = {
            "ubigeo":                         "String",
            "measure":                        "String",
            "year":                           "UInt16",
            "poblacion":                      "Float"
            }

        transform_step = TransformStep()
        load_step = LoadStep(
            "inei_population_y_n_dep_urb_rur", db_connector, if_exists="drop", pk=["ubigeo"], dtype=dtype, 
            nullable_list=["poblacion"]
        )

        return [transform_step, load_step]

if __name__ == "__main__":
    pp = inei_population_y_n_dep_urb_rur_pipeline()
    pp.run({})