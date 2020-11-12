from os import path
import pandas as pd
from etl.helpers import clean_tables
from bamboo_lib.helpers import grab_parent_dir
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from etl.consistency import AggregatorStep
from .itp_indicators_y_act_dept_pipeline_data import datalist

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        data_object = params.get("data")

        depto_dict = {
            "Amazonas": 1,
            "Ancash": 2,
            "Apurímac": 3,
            "Arequipa": 4,
            "Ayacucho": 5,
            "Cajamarca": 6,
            "Callao": 7,
            "Cusco": 8,
            "Huancavelica": 9,
            "Huánuco": 10,
            "Ica": 11,
            "Junín": 12,
            "La Libertad": 13,
            "Lambayeque": 14,
            "Lima": 15,
            "Loreto": 16,
            "Madre de Dios": 17,
            "Moquegua": 18,
            "Pasco": 19,
            "Piura": 20,
            "Puno": 21,
            "San Martín": 22,
            "Tacna": 23,
            "Tumbes": 24,
            "Ucayali": 25
        }

        act_dict = {
            "Agricultura, ganaderia, caza y silvicultura": 1,
            "Pesca y acuicultura": 2,
            "Extraccion de petroleo, gas, minerales y servicios conexos": 3,
            "Electricidad, gas y agua": 4,
            "Construccion": 5,
            "Comercio, mantenimiento y reparacion de vehiculos automotores y motocicletas": 6,
            "Transporte, almacenamiento, correo y mensajeria": 7,
            "Alojamiento y restaurantes": 8,
            "Telecomunicaciones y otros servicos de informacion": 9,
            "Administración publica y defensa": 10,
            "Otros servicios": 11
        }

        # Creating inicial empty dataframe
        df = pd.DataFrame(columns=["ubigeo", "year", "act_economica", "valor_agregado_bruto_2007", "valor_agregado_bruto_cte"])

        left = pd.read_excel(io = path.join(params["datasets"], "20200318", data_object["path"], data_object["filename"]),
                        sheet_name = data_object["sheet_name_1"],
                        usecols = data_object["cols"],
                        skiprows = data_object["skiprows_"])[0:27]
        right = pd.read_excel(io = path.join(params["datasets"], "20200318", data_object["path"], data_object["filename"]),
                        sheet_name = data_object["sheet_name_2"],
                        usecols = data_object["cols"],
                        skiprows = data_object["skiprows_"])[0:27]
        
        # Renaming columns for 2 datasets
        left.rename(columns = data_object["rename_columns"], inplace = True)
        right.rename(columns = data_object["rename_columns"], inplace = True)

        # Melt step in order to merge the sets for the same economic activity
        df_l = pd.melt(left, id_vars =["ubigeo"], value_vars = data_object["melt_"],
                    var_name = "year", value_name = data_object["var_name_1"])
        df_r = pd.melt(right, id_vars =["ubigeo"], value_vars = data_object["melt_"],
                    var_name = "year", value_name = data_object["var_name_2"])

        # Creating key column for merge step
        df_l["code"] = df_l["ubigeo"].astype("str") + df_l["year"].astype("str")
        df_r["code"] = df_r["ubigeo"].astype("str") + df_r["year"].astype("str")

        pivote = pd.merge(df_l,  df_r[["code", "valor_agregado_bruto_cte"]], on = "code", how = "left")
        pivote.drop(["code"], axis = 1, inplace = True)
        pivote["act_economica"] = data_object["name"]
        
        # Append files to df
        df = df.append(pivote, sort=False)

        # Replacing values with id"s and droping un used values
        df["ubigeo"].replace(depto_dict, inplace = True)
        df["act_economica"].replace(act_dict, inplace = True)
        df = df.loc[(df["ubigeo"] != "Lima Provincias") & (df["ubigeo"] != "Lima Metropolitana")]

        # Turning id"s from int to string
        df["ubigeo"] = df["ubigeo"].astype("str").str.zfill(2)

        return df

class itp_ind_year_Pipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))

        dtype = {
            "ubigeo":                                 "String",
            "act_economica":                          "UInt8",
            "year":                                   "UInt16",
            "valor_agregado_bruto_2007":              "Float32",
            "valor_agregado_bruto_cte":               "Float32"
        }

        transform_step = TransformStep()
        agg_step = AggregatorStep("itp_indicators_y_act_dept", measures=["valor_agregado_bruto_2007", "valor_agregado_bruto_cte"])
        load_step = LoadStep("itp_indicators_y_act_dept", db_connector, if_exists="append", pk=["ubigeo"], dtype=dtype)

        return [transform_step, agg_step, load_step]

def run_pipeline(params: dict):
    clean_tables("itp_indicators_y_act_dept", params["connector"])

    for i in datalist:
        pp = itp_ind_year_Pipeline()
        pp.run({'data': i, **params})

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
