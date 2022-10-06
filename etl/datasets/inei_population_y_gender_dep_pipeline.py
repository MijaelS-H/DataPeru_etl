from os import path
import numpy as np
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from etl.consistency import AggregatorStep

depto_dict = {"Amazonas": 1, "Áncash": 2, "Apurímac": 3, "Arequipa": 4, "Ayacucho": 5, "Cajamarca": 6, "Prov. Const.Callao": 7, "Prov. Const. del Callao": 7, "Callao": 7, "Cusco": 8, "Huancavelica": 9, "Huánuco": 10, "Ica": 11, "Junín": 12, "La Libertad": 13, "Lambayeque": 14, "Lima": 15, "Loreto": 16, "Madre de Dios": 17, "Moquegua": 18, "Pasco": 19, "Piura": 20, "Puno": 21, "San Martín": 22, "Tacna": 23, "Tumbes": 24, "Ucayali": 25}
years = [{0: 2000, 1: 2001, 2: 2002}, {0: 2003, 1: 2004, 2: 2005}, {0: 2006, 1: 2007, 2: 2008}, {0: 2009, 1: 2010, 2: 2011}, {0: 2012, 1: 2013, 2: 2014}, {0: 2015, 1: 2016, 2: 2017}, {0: 2018, 1: 2019, 2: 2020}, {0: 2021}]
sheets = ["2000-2002", "2003-2005", "2006-2008", "2009-2011", "2012-2014", "2015-2017", "2018-2020", "2021"]

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        # Creating inicial empty dataframe
        df = pd.DataFrame(columns = ["Unnamed: 0", "Hombre", "Mujer", "year"])

        # First reading step
        xls = pd.ExcelFile(path.join(params["datasets"], "B_Poblacion_y_Vivienda", "B.12.xls"))

        # For each sheet, adding year columns and corrections to dataframe
        for i in list(range(0,6)):
            pivote = pd.read_excel(xls, sheets[i], dtype=str, skiprows = (0,1,2,4,5,6))[0:25]
            pivote.columns = pivote.columns.str.strip()
            pivote_1 = pivote[["Unnamed: 0", "Hombre", "Mujer"]].copy()
            pivote_2 = pivote[["Unnamed: 0", "Hombre.1", "Mujer.1"]].copy()
            pivote_3 = pivote[["Unnamed: 0", "Hombre.2", "Mujer.2"]].copy()
            pivote_1["year"] = years[i][0]
            pivote_2["year"] = years[i][1]
            pivote_3["year"] = years[i][2]
            pivote_2.rename(columns = {"Hombre.1": "Hombre", "Mujer.1" : "Mujer"}, inplace = True)
            pivote_3.rename(columns = {"Hombre.2": "Hombre", "Mujer.2" : "Mujer"}, inplace = True)
            df = df.append(pivote_1)
            df = df.append(pivote_2)
            df = df.append(pivote_3)

        df.rename(columns = {"Unnamed: 0": "ubigeo", "Hombre": "poblacion_masculina", "Mujer": "poblacion_femenina"}, inplace = True)
        df["ubigeo"] = df["ubigeo"].str.slice(0,2)

        # Step to add 2018 year missing from data
        for i in list(range(1,26)):
            pivote = pd.DataFrame({"ubigeo" : i, "poblacion_masculina" : [np.nan], "poblacion_femenina" : [np.nan], "year" : 2020})
            df = df.append(pivote)

        df["ubigeo"] = df["ubigeo"].astype("str").str.zfill(2)

        # Second reading step
        df2 = pd.read_excel(path.join(params["datasets"], "C_Empleo", "C.8.xlsx"), skiprows = (0,1,2,3))[12:46]
        df3 = pd.read_excel(path.join(params["datasets"], "C_Empleo", "C.9.xlsx"), skiprows = (0,1,2,3))[12:46]
        df4 = pd.read_excel(path.join(params["datasets"], "C_Empleo", "C.11.xlsx"), skiprows = (0,1,2,3,4))[14:47]
        df5 = pd.read_excel(path.join(params["datasets"], "C_Empleo", "C.12.xlsx"), skiprows = (0,1,2,3,4))[14:47]
        df6 = pd.read_excel(path.join(params["datasets"], "C_Empleo", "C.22.xlsx"), skiprows = (0,1,2,3,4))[14:47]
        df7 = pd.read_excel(path.join(params["datasets"], "C_Empleo", "C.23.xlsx"), skiprows = (0,1,2,3,4))[14:47]
        df8 = pd.read_excel(path.join(params["datasets"], "C_Empleo", "C.24.xlsx"), skiprows = (0,1,2,3,4))[14:47]

        for item in [df2, df3, df4, df5, df6 ,df7, df8]:
            item.rename(columns = {"Ámbito geográfico": "ubigeo"}, inplace = True)
            item.drop(item.loc[item["ubigeo"].str.contains("Provincia|Regi|Departamento")].index, inplace=True)

        df2 = pd.melt(df2, id_vars = ["ubigeo"], value_vars = [2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020], var_name = "year", value_name = "pea_total_masculina")
        df3 = pd.melt(df3, id_vars = ["ubigeo"], value_vars = [2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020], var_name = "year", value_name = "pea_total_femenina")
        df2["pea_total_masculina"] = df2["pea_total_masculina"]*1000
        df3["pea_total_femenina"]  = df3["pea_total_femenina"]*1000

        df4 = pd.melt(df4, id_vars = ["ubigeo"], value_vars = [2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020], var_name = "year", value_name = "pea_ocupada_masculina")
        df5 = pd.melt(df5, id_vars = ["ubigeo"], value_vars = [2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020], var_name = "year", value_name = "pea_ocupada_femenina")
        df4["pea_ocupada_masculina"] = df4["pea_ocupada_masculina"]*1000
        df5["pea_ocupada_femenina"]  = df5["pea_ocupada_femenina"]*1000


        df6 = pd.melt(df6, id_vars = ["ubigeo"], value_vars = [2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020], var_name = "year", value_name = "ingreso_promedio_mensual_soles_nom")
        df7 = pd.melt(df7, id_vars = ["ubigeo"], value_vars = [2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020], var_name = "year", value_name = "ingreso_promedio_m_mensual_soles_nom")
        df8 = pd.melt(df8, id_vars = ["ubigeo"], value_vars = [2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020], var_name = "year", value_name = "ingreso_promedio_f_mensual_soles_nom")

        df["code"] = df["ubigeo"].astype(str) + df["year"].astype(str)

        # Turning id"s from int to string # Replacing values with id"s
        for item in [df2, df3, df4, df5, df6 ,df7, df8]:
            item["ubigeo"].replace(depto_dict, inplace = True)
            item["ubigeo"] = item["ubigeo"].astype("str").str.zfill(2)
            item["code"] = item["ubigeo"].astype(str) + item["year"].astype(str)

        df = pd.merge(df,  df2[["code", "pea_total_masculina"]], on = "code", how = "left")
        df = pd.merge(df,  df3[["code", "pea_total_femenina"]], on = "code", how = "left")
        df = pd.merge(df,  df4[["code", "pea_ocupada_masculina"]], on = "code", how = "left")
        df = pd.merge(df,  df5[["code", "pea_ocupada_femenina"]], on = "code", how = "left")
        df = pd.merge(df,  df6[["code", "ingreso_promedio_mensual_soles_nom"]], on = "code", how = "left")
        df = pd.merge(df,  df7[["code", "ingreso_promedio_m_mensual_soles_nom"]], on = "code", how = "left")
        df = pd.merge(df,  df8[["code", "ingreso_promedio_f_mensual_soles_nom"]], on = "code", how = "left")

        # Dropping un used values
        df.drop(["code"], axis = 1, inplace = True)

        # setting corrections to types
        for i in ["poblacion_masculina", "poblacion_femenina", "pea_total_masculina", "pea_total_femenina", "pea_ocupada_masculina", "pea_ocupada_femenina",
                  "ingreso_promedio_mensual_soles_nom", "ingreso_promedio_m_mensual_soles_nom", "ingreso_promedio_f_mensual_soles_nom"]:
            df[i] = df[i].astype(float)


        return df

class inei_population_y_gender_dep_Pipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))

        dtype = {
            "ubigeo":                                 "String",
            "year":                                   "UInt16",
            "poblacion_masculina":                    "Float32",
            "poblacion_femenina":                     "Float32",
            "pea_total_masculina":                    "Float32",
            "pea_total_femenina":                     "Float32",
            "pea_ocupada_masculina":                  "Float32",
            "pea_ocupada_femenina":                   "Float32",
            "ingreso_promedio_mensual_soles_nom":     "Float32",
            "ingreso_promedio_m_mensual_soles_nom":   "Float32",
            "ingreso_promedio_f_mensual_soles_nom":   "Float32"
        }

        transform_step = TransformStep()
        agg_step = AggregatorStep("inei_population_y_gender_dep", measures=["poblacion_masculina", "poblacion_femenina", "pea_total_masculina", "pea_total_femenina", "pea_ocupada_masculina", "pea_ocupada_femenina", "ingreso_promedio_mensual_soles_nom", "ingreso_promedio_m_mensual_soles_nom", "ingreso_promedio_f_mensual_soles_nom"])
        load_step = LoadStep("inei_population_y_gender_dep", db_connector, if_exists="drop", pk=["ubigeo"], dtype=dtype, 
                    nullable_list=["pea_total_masculina", "pea_total_femenina", "pea_ocupada_masculina", "pea_ocupada_femenina",
                    "ingreso_promedio_mensual_soles_nom", "ingreso_promedio_m_mensual_soles_nom", "ingreso_promedio_f_mensual_soles_nom"])

        return [transform_step, load_step]

def run_pipeline(params: dict):
    pp = inei_population_y_gender_dep_Pipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
