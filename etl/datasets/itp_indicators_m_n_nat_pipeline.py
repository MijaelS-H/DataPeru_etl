from os import path
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from etl.consistency import AggregatorStep

month_dict = {"Enero": "01", "Febrero": "02", "Marzo": "03", "Abril": "04", "Mayo": "05", "Junio": "06", "Julio": "07", "Agosto": "08", "Septiembre": "09", "Setiembre": "09", "Octubre": "10", "Noviembre": "11", "Diciembre": "12", "Ene": "01", "Feb": "02", "Mar": "03", "Abr": "04", "May": "05", "Jun": "06", "Jul": "07", "Ago": "08", "Set": "09", "Set": "09", "Oct": "10", "Nov": "11", "Dic": "12"}
df_columns = ["year", "m_nacional_arquelogia_historia_Peru", "museo_de_la_Nacion", "museo_de_arte_italiano", "m_de_la_cultura_peruana", "z_arqueologica_m_sitio_Pachacamac", "z_arqueologica_m_sitio_Puruchuco", "z_arqueologica_m_sitio_Huallamarca", "z_arqueologica_m_sitio_Otros", "m_sitio_cerro_San_Cristobal", "m_sitio_centro_arqueologico_Pucllana", "casa_m_Jose_Carlos_Mariategui", "complejo_arqueologico_Mateo_Salado", "casa_gastronomia_peruana"]

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        # Loading data
        df1 = pd.read_excel(io = path.join(params["datasets"], "20200318", "A. Economía", "A.158.xlsx"), skiprows = (0,1,2))
        df2 = pd.read_excel(io = path.join(params["datasets"], "20200318", "A. Economía", "A.159.xlsx"), skiprows = (0,1,2))

        df3 = pd.read_excel(io = path.join(params["datasets"], "20200318", "A. Economía", "A.166.xlsx"), skiprows = (0,1,2,4), usecols = "A:O")[0:12]
        df4 = pd.read_excel(io = path.join(params["datasets"], "20200318", "A. Economía", "A.156.xls"), skiprows = (0,1,2), usecols = "A,C:F,H:P")[24:102]

        # Common steps
        for item in [df1,df2]:
            item["Año"].fillna(method = "ffill", inplace = True)
            item["Año"] = item["Año"].astype(int)
            item["Mes"].replace(month_dict, inplace = True)
            item["month_id"] = item["Año"].astype(str) + item["Mes"].astype(str)

        # Renaming columns to understandable names
        df1.rename(columns = {"Índice": "ipc_base_2011_observado", "Mensual": "ipc_base_2011_var_mes_anterior", "Acumulada": "ipc_base_2011_var_acumulado", "Anual": "ipc_base_2011_var_anio_anterior"}, inplace = True)
        df2.rename(columns = {"Índice": "ipm_base_2013_observado", "Mensual": "ipm_base_2013_var_mes_anterior", "Acumulada": "ipm_base_2013_var_acumulado"}, inplace = True)

        # Morosity dataset
        df3 = pd.melt(df3, id_vars = ["Mes"], value_vars = [2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014,2015, 2016, 2017, 2018, 2019], var_name = "year", value_name = "perc_morosidad_credi_banca_mult")
        df3["month_id"] = df3["year"].astype(str) + df3["Mes"]

        # Monthly tourism dataset
        df4.columns = df_columns
        df4["year"] = df4["year"].astype(str)
        df4["month"] = pd.np.nan
        df4.loc[df4["year"].str.contains("a|e|i|o|u|A|O"), "month"] = df4["year"]
        df4.loc[df4["year"].str.contains("a|e|i|o|u|A|O"), "year"] = pd.np.nan
        df4["month"].replace(month_dict, inplace = True)
        df4["year"].fillna(method = "ffill", inplace = True)
        df4.dropna(axis=0, how="any", inplace = True)
        df4["month_id"] = df4["year"].astype(str) + df4["month"]
        df4.replace({"-": pd.np.nan, " -": pd.np.nan}, inplace = True)

        # Formatting data
        df1["ipc_base_2011_var_anio_anterior"].replace("-", pd.np.nan, inplace = True)
        df1 = df1[["month_id", "ipc_base_2011_observado", "ipc_base_2011_var_mes_anterior", "ipc_base_2011_var_acumulado", "ipc_base_2011_var_anio_anterior"]]

        df = pd.merge(df1, df2[["month_id", "ipm_base_2013_observado", "ipm_base_2013_var_mes_anterior", "ipm_base_2013_var_acumulado"]], on = "month_id", how = "left")
        df = pd.merge(df,  df3[["month_id", "perc_morosidad_credi_banca_mult"]], on = "month_id", how = "left")
        df = pd.merge(df,  df4[["month_id", "m_nacional_arquelogia_historia_Peru", "museo_de_la_Nacion", "museo_de_arte_italiano", "m_de_la_cultura_peruana", "z_arqueologica_m_sitio_Pachacamac", "z_arqueologica_m_sitio_Puruchuco", "z_arqueologica_m_sitio_Huallamarca", "z_arqueologica_m_sitio_Otros", "m_sitio_cerro_San_Cristobal", "m_sitio_centro_arqueologico_Pucllana", "casa_m_Jose_Carlos_Mariategui", "complejo_arqueologico_Mateo_Salado", "casa_gastronomia_peruana"]], on = "month_id", how = "left")

        for i in df.columns:
            df[i] = df[i].astype(float)

        df["ubigeo"] = "per"

        return df

class itp_indicators_m_n_nat_pipeline(EasyPipeline):

    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))
        dtype = {
            "ubigeo":                                           "String",
            "month_id":                                         "UInt16",
            "ipc_base_2011_observado":                          "Float32",
            "ipc_base_2011_var_mes_anterior":                   "Float32",
            "ipc_base_2011_var_acumulado":                      "Float32",
            "ipc_base_2011_var_anio_anterior":                  "Float32",
            "ipm_base_2013_observado":                          "Float32",
            "ipm_base_2013_var_mes_anterior":                   "Float32",
            "ipm_base_2013_var_acumulado":                      "Float32",
            "perc_morosidad_credi_banca_mult":                  "Float32",
            "m_nacional_arquelogia_historia_Peru":              "UInt16",
            "museo_de_la_Nacion":                               "UInt16",
            "museo_de_arte_italiano":                           "UInt16",
            "m_de_la_cultura_peruana":                          "UInt16",
            "z_arqueologica_m_sitio_Pachacamac":                "UInt16",
            "z_arqueologica_m_sitio_Puruchuco":                 "UInt16",
            "z_arqueologica_m_sitio_Huallamarca":               "UInt16",
            "z_arqueologica_m_sitio_Otros":                     "UInt16",
            "m_sitio_cerro_San_Cristobal":                      "UInt16",
            "m_sitio_centro_arqueologico_Pucllana":             "UInt16",
            "casa_m_Jose_Carlos_Mariategui":                    "UInt16",
            "complejo_arqueologico_Mateo_Salado":               "UInt16",
            "casa_gastronomia_peruana":                         "UInt16"
            }

        transform_step = TransformStep()
        agg_step = AggregatorStep("itp_indicators_m_n_nat", measures=["ipc_base_2011_observado", "ipc_base_2011_var_mes_anterior", "ipc_base_2011_var_acumulado", "ipc_base_2011_var_anio_anterior", "ipm_base_2013_observado", "ipm_base_2013_var_mes_anterior", "ipm_base_2013_var_acumulado", "perc_morosidad_credi_banca_mult", "m_nacional_arquelogia_historia_Peru", "museo_de_la_Nacion", "museo_de_arte_italiano", "m_de_la_cultura_peruana", "z_arqueologica_m_sitio_Pachacamac", "z_arqueologica_m_sitio_Puruchuco", "z_arqueologica_m_sitio_Huallamarca", "z_arqueologica_m_sitio_Otros", "m_sitio_cerro_San_Cristobal", "m_sitio_centro_arqueologico_Pucllana", "casa_m_Jose_Carlos_Mariategui", "complejo_arqueologico_Mateo_Salado", "casa_gastronomia_peruana"])
        load_step = LoadStep("itp_indicators_m_n_nat", db_connector, if_exists="drop", pk=["ubigeo"], dtype=dtype, 
                    nullable_list=["ipc_base_2011_observado", "ipc_base_2011_var_mes_anterior", "ipc_base_2011_var_acumulado", "ipc_base_2011_var_anio_anterior", 
                    "ipm_base_2013_observado", "ipm_base_2013_var_mes_anterior", "ipm_base_2013_var_acumulado", "perc_morosidad_credi_banca_mult",
                    "m_nacional_arquelogia_historia_Peru", "museo_de_la_Nacion", "museo_de_arte_italiano", "m_de_la_cultura_peruana", "z_arqueologica_m_sitio_Pachacamac",
                    "z_arqueologica_m_sitio_Puruchuco", "z_arqueologica_m_sitio_Huallamarca", "z_arqueologica_m_sitio_Otros", "m_sitio_cerro_San_Cristobal",
                    "m_sitio_centro_arqueologico_Pucllana", "casa_m_Jose_Carlos_Mariategui", "complejo_arqueologico_Mateo_Salado", "casa_gastronomia_peruana"])

        return [transform_step, agg_step, load_step]

def run_pipeline(params: dict):
    pp = itp_indicators_m_n_nat_pipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
