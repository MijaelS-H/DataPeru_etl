from os import path
import pandas as pd
from bamboo_lib.helpers import query_to_df
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from etl.consistency import AggregatorStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        # Loading data
        dim_country_query = "SELECT * FROM dim_shared_country"
        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))
        countries = query_to_df(db_connector, raw_query=dim_country_query)

        df1_1 = pd.read_excel(io = path.join(params["datasets"],"20200318", "A. Economía", "A.147.xls"), sheet_name = "19.2 (c)", skiprows = (0,1,2,3), usecols = "A:K")[3:44]
        df1_2 = pd.read_excel(io = path.join(params["datasets"],"20200318", "A. Economía", "A.147.xls"), sheet_name = "19.2 (d)", skiprows = (0,1,2,3), usecols = "A:K")[2:45]
        df2_1 = pd.read_excel(io = path.join(params["datasets"],"20200318", "A. Economía", "A.149.xls"), sheet_name = "cap22005c", skiprows = (0,1,2,3))[3:45]
        df2_2 = pd.read_excel(io = path.join(params["datasets"],"20200318", "A. Economía", "A.149.xls"), sheet_name = "cap22005d", skiprows = (0,1,2,3))[2:37]
        df_3 =  pd.read_excel(io = path.join(params["datasets"],"20200318", "A. Economía", "A.154.xls"), skiprows = (0,1,2), usecols = "A:D,F:H,J:L,N:P,R:T,V:X,Z:AB")[5:52]

        df1 = df1_1.append(df1_2, sort = True)
        df2 = df2_1.append(df2_2, sort = True)

        df_3['Residencia Habitual'].replace({"Sud-Africa" : "Sudáfrica", "Oceanía (Australia)": "Australia"}, inplace = True)

        df_1_ = df_3[['Residencia Habitual', 2012, 'Unnamed: 2', 'Unnamed: 3']].copy()
        df_2_ = df_3[['Residencia Habitual', 2013, 'Unnamed: 6', 'Unnamed: 7']].copy()
        df_3_ = df_3[['Residencia Habitual', 2014, 'Unnamed: 10', 'Unnamed: 11']].copy()
        df_4_ = df_3[['Residencia Habitual', 2015, 'Unnamed: 14', 'Unnamed: 15']].copy()
        df_5_ = df_3[['Residencia Habitual', 2016, 'Unnamed: 18', 'Unnamed: 19']].copy()
        df_6_ = df_3[['Residencia Habitual', 2017, 'Unnamed: 22', 'Unnamed: 23']].copy()
        df_7_ = df_3[['Residencia Habitual', 2018, 'Unnamed: 26', 'Unnamed: 27']].copy()

        df_ = [df_1_, df_2_, df_3_, df_4_, df_5_, df_6_, df_7_]
        df3 = pd.DataFrame(columns = ["country_name_es", "arribos_turistas_extranjeros", "prenoctacion_turistas_extranjeros", "permanencia_prom_noche_turistas_extranjeros", "year"])

        for i in range(0,7):
            df_[i].rename(columns = {df_[i].columns[0] : "country_name_es", df_[i].columns[1] : "arribos_turistas_extranjeros", df_[i].columns[2] : "prenoctacion_turistas_extranjeros", df_[i].columns[3] : "permanencia_prom_noche_turistas_extranjeros"}, inplace = True)
            df_[i]["year"] = 2012 + i
            df3 = df3.append(df_[i])

        df1.rename(columns = {"Zona Geográfica y": "country_name_es"}, inplace = True)
        df2.rename(columns = {"Zona Geográfica y": "country_name_es"}, inplace = True)

        df1.iloc[16, df1.columns.get_loc("country_name_es")] = "Otros C"
        df1.iloc[27, df1.columns.get_loc("country_name_es")] = "Otros S"
        df1.iloc[57, df1.columns.get_loc("country_name_es")] = "Otros E"
        df1.iloc[73, df1.columns.get_loc("country_name_es")] = "Otros A"
        df1.iloc[78, df1.columns.get_loc("country_name_es")] = "Otros Á"
        df1.iloc[82, df1.columns.get_loc("country_name_es")] = "Otros O"

        df2.iloc[17, df2.columns.get_loc("country_name_es")] = "Otros C"
        df2.iloc[28, df2.columns.get_loc("country_name_es")] = "Otros S"
        df2.iloc[51, df2.columns.get_loc("country_name_es")] = "Otros E"
        df2.iloc[68, df2.columns.get_loc("country_name_es")] = "Otros A"
        df2.iloc[72, df2.columns.get_loc("country_name_es")] = "Otros Á"
        df2.iloc[75, df2.columns.get_loc("country_name_es")] = "Otros O"

        for item in [df1, df2, df3]:
            item.drop(item.loc[item["country_name_es"].str.contains("Norteamérica|Centroamérica|Sudamérica|Europa|Asia|África|Oceanía|Africa|Centro América")].index, axis = 0, inplace = True)
            item["country_name_es"] = item["country_name_es"].str.strip()
            item["country_name_es"].replace({"Estados Unidos de América" : "Estados Unidos", "Países bajos (Holanda)" : "Países Bajos", "Rep. Checa" : "Chequia", "Rumanía" : "Rumania", "Nueva Zelanda" : "Nueva Zelandia", "Bielorusia": "Bielorrusia", "Corea del  Norte": "Corea del Norte", "Holanda-Países Bajos": "Países Bajos", "Bahamas": "Las Bahamas", "Paises Bajos (Holanda)": "Países Bajos", "China (R.P.)": "China", "Taiwán": "Taiwan", "República Popular China": "China", "Inglaterra-Reino Unido": "Reino Unido", "Panama": "Panamá", "Otros 1/": "Aguas Internacionales", "Otros C": "Otros Centroamérica", "Otros S": "Otros América del Sur", "Otros E": "Otros Europa", "Otros A": "Otros Asia", "Otros Á": "Otros África", "Otros O": "Otros Oceanía", "Otros paises": "Otros"}, inplace = True)

        df1 = pd.melt(df1, id_vars = "country_name_es", value_vars = [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018],
                    var_name = "year", value_name = "n_ingresos_turistas_internacionales")
        df2 = pd.melt(df2, id_vars = "country_name_es", value_vars = [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018],
                    var_name = "year", value_name = "n_salidas_turistas_internacionales")

        df_1 = pd.merge(df1, countries[["iso3", "continent_id", "continent_es", "country_name_es"]], on = "country_name_es", how = "left")
        df_2 = pd.merge(df2, countries[["iso3", "continent_id", "continent_es", "country_name_es"]], on = "country_name_es", how = "left")
        df_3 = pd.merge(df3, countries[["iso3", "continent_id", "continent_es", "country_name_es"]], on = "country_name_es", how = "left")

        temp = []

        for item in [df_1, df_2, df_3]:
            item["code"] = item["iso3"].astype("str") + item["year"].astype("str")
            temp = temp + list(item["code"].unique())

        temp = list(set(temp))

        temp = pd.DataFrame(temp, columns=["code"])
        temp["year"] = temp["code"].str[-4:]
        temp["iso3"] = temp["code"].str[:-4]

        df = pd.merge(temp, df_1, on = "code", how = "left", suffixes=("", "_drop"))
        df = pd.merge(df, df_2[["code", "n_salidas_turistas_internacionales"]], on = "code", how = "left", suffixes=("", "_drop"))
        df = pd.merge(df, df_3[["code", "arribos_turistas_extranjeros", "prenoctacion_turistas_extranjeros", "permanencia_prom_noche_turistas_extranjeros"]], on = "code", how = "left", suffixes=("", "_drop"))

        df.replace("-", 0, inplace = True)
        df["ubigeo"] = "per"
        df = df[["ubigeo", "year", "iso3", "n_ingresos_turistas_internacionales", "n_salidas_turistas_internacionales", "arribos_turistas_extranjeros", "prenoctacion_turistas_extranjeros", "permanencia_prom_noche_turistas_extranjeros"]]

        return df

class itp_indicators_y_n_tourism_pipeline(EasyPipeline):
  
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))

        dtype = {
            "ubigeo":                                                 "String",
            "year":                                                   "UInt16",
            "n_ingresos_turistas_internacionales":                    "UInt32",
            "n_salidas_turistas_internacionales":                     "UInt32",
            "arribos_turistas_extranjeros":                           "UInt32",
            "prenoctacion_turistas_extranjeros":                      "UInt32",
            "permanencia_prom_noche_turistas_extranjeros":            "Float32",
            "iso3":                                                   "String"
            }

        transform_step = TransformStep()
        agg_step = AggregatorStep("itp_indicators_y_n_tourism", measures=["n_ingresos_turistas_internacionales", "n_salidas_turistas_internacionales", "arribos_turistas_extranjeros", "prenoctacion_turistas_extranjeros", "permanencia_prom_noche_turistas_extranjeros"])
        load_step = LoadStep("itp_indicators_y_n_tourism", db_connector, if_exists="drop", pk=["ubigeo"], dtype=dtype, 
                    nullable_list=["iso3", "n_ingresos_turistas_internacionales", "n_salidas_turistas_internacionales", "arribos_turistas_extranjeros",
                    "prenoctacion_turistas_extranjeros", "permanencia_prom_noche_turistas_extranjeros"])

        return [transform_step, agg_step, load_step]

def run_pipeline(params: dict):
    pp = itp_indicators_y_n_tourism_pipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys

    run_pipeline({
        "connector": params["connector"],
        "datasets": sys.argv[1]
    })
