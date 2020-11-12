from os import path
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from etl.consistency import AggregatorStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        # Loading data
        range_ = list(range(0, 4)) + list(range(6,39))
        df1 = pd.read_excel(io = path.join(params["datasets"],"20200318",  "A. Economía","A.3.xlsx"), usecols = "B:J", skiprows = range_, reset_index = True)
        df2 = pd.read_excel(io = path.join(params["datasets"],"20200318",  "A. Economía","A.4.xlsx"), usecols = "B:J", skiprows = range_, reset_index = True)
        df3 = pd.read_excel(io = path.join(params["datasets"],"20200318",  "A. Economía","A.5.xlsx"), usecols = "B:T", skiprows = range_, reset_index = True)
        df4 = pd.read_excel(io = path.join(params["datasets"],"20200318",  "A. Economía","A.6.xlsx"), usecols = "B:C,E:F,H,J,K", skiprows = range_, reset_index = True)
        df5 = pd.read_excel(io = path.join(params["datasets"],"20200318",  "A. Economía","A.7.xlsx"), usecols = "B:C,E:F,H,J,K", skiprows = range_, reset_index = True)
        df6 = pd.read_excel(io = path.join(params["datasets"],"20200318",  "A. Economía","A.8.xlsx"), usecols = "A,F", skiprows = range(0,6), reset_index = True)

        # Dropping unused rows from datasets
        df1.drop([0,66,67,68], axis = 0, inplace = True)
        df2.drop([0,66,67,68], axis = 0, inplace = True)
        df3.drop([0,66,67,68], axis = 0, inplace = True)
        df4.drop([0,66,67,68], axis = 0, inplace = True)
        df5.drop([65,66,67], axis = 0, inplace = True)
        df6.drop([25,26,27], axis = 0, inplace = True)

        # Renaming columns to understandable names
        df1.rename(columns = {"Año / Trimestre": "year", "Unnamed: 2": "quarter", "Producto \nBruto \nInterno": "producto_bruto_interno_2007", "Gasto de \nConsumo Final \nPrivado": "gasto_consumo_final_privado_2007", "Gasto de \nConsumo de Gobierno": "gasto_consumo_gobierno_2007", "Formación \nBruta de \nCapital": "formacion_bruta_capital_2007", "Formación \nBruta de \nCapital Fijo": "formacion_bruta_capital_fijo_2007", "Exportaciones": "exportaciones_2007", "Importaciones": "importaciones_2007"}, inplace = True)
        df2.rename(columns = {"Año / Trimestre": "year", "Unnamed: 2": "quarter", "Producto \nBruto \nInterno": "producto_bruto_interno_nominal", "Gasto de \nConsumo Final \nPrivado": "gasto_consumo_final_privado_nominal", "Gasto de \nConsumo de Gobierno": "gasto_consumo_gobierno_nominal", "Formación \nBruta de \nCapital": "formacion_bruta_capital_nominal", "Formación \nBruta de \nCapital Fijo": "formacion_bruta_capital_fijo_nominal", "Exportaciones": "exportaciones_nominal", "Importaciones": "importaciones_nominal"}, inplace = True) 
        df3.rename(columns = {"Año / Trimestre": "year", "Unnamed: 2": "quarter", "Producto Bruto Interno" : "producto_interno_bruto" , "Derechos de Importación y Otros Impuestos" : "derechos_importacion_otros_impuestos" , "Valor Agregado Bruto Total (VAB)" : "valor_agregado_bruto_total", "Extractivas" : "agricultura_ganaderia_caza_silvicultura", "Unnamed: 7" : "pesca_acuicultura", "Unnamed: 8" : "extraccion_petroleo_gas_minerales_servicios_conexos", "Transformación" : "manufactura", "Unnamed: 10" : "construccion", "Servicios" : "electricidad_gas_agua", "Unnamed: 12" : "comercio_mantenimiento_reparacion_vehiculos", "Unnamed: 13" : "transporte_almacenamiento_correo_mensajeria", "Unnamed: 14" : "alojamiento_restaurantes", "Unnamed: 15" : "telecomunicaciones_otros_servicios_informacion", "Unnamed: 16" : "servicios_financieros_seguros_pensiones", "Unnamed: 17" : "servicios_prestados_empresas", "Unnamed: 18" : "administracion_publica_defensa", "Unnamed: 19" : "otros_servicios"}, inplace = True) 
        df4.rename(columns = {"Año / Trimestre": "year", "Unnamed: 2": "quarter", "Unnamed: 4": "formacion_bruta_cap_fijo_publico_2007", "Unnamed: 5": "formacion_bruta_cap_fijo_privado_2007", "Unnamed: 7": "formacion_bruta_cap_fijo_construccion_2007", "Unnamed: 9": "formacion_bruta_cap_fijo_maq_nac_2007", "Unnamed: 10": "formacion_bruta_cap_fijo_maq_imp_2007"}, inplace = True) 
        df5.rename(columns = {"Año / Trimestre": "year", "Unnamed: 2": "quarter", "Unnamed: 4": "formacion_bruta_cap_fijo_publico_nom", "Unnamed: 5": "formacion_bruta_cap_fijo_privado_nom", "Unnamed: 7": "formacion_bruta_cap_fijo_construccion_nom", "Unnamed: 9": "formacion_bruta_cap_fijo_maq_nac_nom", "Unnamed: 10": "formacion_bruta_cap_fijo_maq_imp_nom"}, inplace = True) 
        df6.rename(columns = {"Unnamed: 0": "year", "Unnamed: 5": "poblacion_anual"}, inplace = True)

        # For cycle to clean unused elements an NaN values + creating quarter_id code for merge step
        for item in [df1, df2, df3, df4, df5]:
            item["year"].replace("Trimestre", pd.np.nan, inplace = True)
            item["year"].fillna(method="ffill", inplace = True)
            item["year"].fillna(2007, inplace = True)
            item["year"] = item["year"].astype(int)
            item["quarter"].replace({pd.np.nan: 0, "I": 1,"II": 2,"III": 3,"IV": 4}, inplace = True)
            item["quarter_id"] = item["year"].astype(str) + item["quarter"].astype(str)

        df6["year"].replace({"2016P/": 2016, "2017P/": 2017, "2018E/": 2018}, inplace = True)

        # Deleting rows related to totals
        df1 = df1[~df1["quarter_id"].str.endswith(("0"))]
        df2 = df2[~df2["quarter_id"].str.endswith(("0"))]
        df3 = df3[~df3["quarter_id"].str.endswith(("0"))]
        df4 = df4[~df4["quarter_id"].str.endswith(("0"))]
        df5 = df5[~df5["quarter_id"].str.endswith(("0"))]

        # Merge step for 6 files
        df = pd.merge(df1, df2[["quarter_id", "producto_bruto_interno_nominal", "gasto_consumo_final_privado_nominal", "gasto_consumo_gobierno_nominal", "formacion_bruta_capital_nominal", "formacion_bruta_capital_fijo_nominal", "exportaciones_nominal","importaciones_nominal"]], on = "quarter_id", how = "left")
        df = pd.merge(df, df3[["quarter_id", "producto_interno_bruto", "derechos_importacion_otros_impuestos", "valor_agregado_bruto_total", "agricultura_ganaderia_caza_silvicultura", "pesca_acuicultura", "extraccion_petroleo_gas_minerales_servicios_conexos", "manufactura", "construccion", "electricidad_gas_agua", "comercio_mantenimiento_reparacion_vehiculos", "transporte_almacenamiento_correo_mensajeria", "alojamiento_restaurantes", "telecomunicaciones_otros_servicios_informacion", "servicios_financieros_seguros_pensiones", "servicios_prestados_empresas", "administracion_publica_defensa", "otros_servicios"]], on = "quarter_id", how = "left")
        df = pd.merge(df, df4[["quarter_id", "formacion_bruta_cap_fijo_publico_2007", "formacion_bruta_cap_fijo_privado_2007", "formacion_bruta_cap_fijo_construccion_2007", "formacion_bruta_cap_fijo_maq_nac_2007", "formacion_bruta_cap_fijo_maq_imp_2007"]], on = "quarter_id", how = "left")
        df = pd.merge(df, df5[["quarter_id","formacion_bruta_cap_fijo_publico_nom", "formacion_bruta_cap_fijo_privado_nom", "formacion_bruta_cap_fijo_construccion_nom", "formacion_bruta_cap_fijo_maq_nac_nom", "formacion_bruta_cap_fijo_maq_imp_nom"]], on = "quarter_id", how = "left")
        df = pd.merge(df, df6[["year", "poblacion_anual"]], on = "year", how = "left")

        # Changing values from float to int
        cols_int = ["producto_bruto_interno_2007", "gasto_consumo_final_privado_2007", "gasto_consumo_gobierno_2007", "formacion_bruta_capital_2007", "formacion_bruta_capital_fijo_2007", "exportaciones_2007", "importaciones_2007", "quarter_id", "producto_bruto_interno_nominal", "gasto_consumo_final_privado_nominal", "gasto_consumo_gobierno_nominal", "formacion_bruta_capital_nominal", "formacion_bruta_capital_fijo_nominal", "exportaciones_nominal", "importaciones_nominal", "producto_interno_bruto", "derechos_importacion_otros_impuestos", "valor_agregado_bruto_total", "agricultura_ganaderia_caza_silvicultura", "pesca_acuicultura", "extraccion_petroleo_gas_minerales_servicios_conexos", "manufactura", "construccion", "electricidad_gas_agua", "comercio_mantenimiento_reparacion_vehiculos", "transporte_almacenamiento_correo_mensajeria", "alojamiento_restaurantes", "telecomunicaciones_otros_servicios_informacion", "servicios_financieros_seguros_pensiones", "servicios_prestados_empresas", "administracion_publica_defensa", "otros_servicios", "formacion_bruta_cap_fijo_publico_2007", "formacion_bruta_cap_fijo_privado_2007", "formacion_bruta_cap_fijo_construccion_2007", "formacion_bruta_cap_fijo_maq_nac_2007", "formacion_bruta_cap_fijo_maq_imp_2007","formacion_bruta_cap_fijo_publico_nom","formacion_bruta_cap_fijo_privado_nom","formacion_bruta_cap_fijo_construccion_nom","formacion_bruta_cap_fijo_maq_nac_nom","formacion_bruta_cap_fijo_maq_imp_nom"]
        for item in cols_int:
            df[item] = df[item].astype(int)

        # Dropping unsued columns
        df.drop(["year", "quarter"], axis=1, inplace=True)

        df["ubigeo"] = "per"

        return df

class itp_ind_quarter_n_nat_pipeline(EasyPipeline):
  
    @staticmethod
    def parameter_list():
        return [
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))

        dtype = {
            "ubigeo":                                                     "String",
            "quarter_id":                                                 "UInt16",
            "producto_bruto_interno_2007":                                "UInt32",
            "gasto_consumo_final_privado_2007":                           "UInt32",
            "gasto_consumo_gobierno_2007":                                "UInt32",
            "formacion_bruta_capital_2007":                               "UInt32",
            "formacion_bruta_capital_fijo_2007":                          "UInt32",
            "exportaciones_2007":                                         "UInt32",
            "importaciones_2007":                                         "UInt32",
            "producto_bruto_interno_nominal":                             "UInt32",
            "gasto_consumo_final_privado_nominal":                        "UInt32",
            "gasto_consumo_gobierno_nominal":                             "UInt32",
            "formacion_bruta_capital_nominal":                            "UInt32",
            "formacion_bruta_capital_fijo_nominal":                       "UInt32",
            "exportaciones_nominal":                                      "UInt32",
            "importaciones_nominal":                                      "UInt32",
            "producto_interno_bruto":                                     "UInt32",
            "derechos_importacion_otros_impuestos":                       "UInt32",
            "valor_agregado_bruto_total":                                 "UInt32",
            "agricultura_ganaderia_caza_silvicultura":                    "UInt32",
            "pesca_acuicultura":                                          "UInt32",
            "extraccion_petroleo_gas_minerales_servicios_conexos":        "UInt32",
            "manufactura":                                                "UInt32",
            "construccion":                                               "UInt32",
            "electricidad_gas_agua":                                      "UInt32",
            "comercio_mantenimiento_reparacion_vehiculos":                "UInt32",
            "transporte_almacenamiento_correo_mensajeria":                "UInt32",
            "alojamiento_restaurantes":                                   "UInt32",
            "telecomunicaciones_otros_servicios_informacion":             "UInt32",
            "servicios_financieros_seguros_pensiones":                    "UInt32",
            "servicios_prestados_empresas":                               "UInt32",
            "administracion_publica_defensa":                             "UInt32",
            "otros_servicios":                                            "UInt32",
            "formacion_bruta_cap_fijo_publico_2007":                      "UInt32",
            "formacion_bruta_cap_fijo_privado_2007":                      "UInt32",
            "formacion_bruta_cap_fijo_construccion_2007":                 "UInt32",
            "formacion_bruta_cap_fijo_maq_nac_2007":                      "UInt32",
            "formacion_bruta_cap_fijo_maq_imp_2007":                      "UInt32",
            "formacion_bruta_cap_fijo_publico_nom":                       "UInt32",
            "formacion_bruta_cap_fijo_privado_nom":                       "UInt32",
            "formacion_bruta_cap_fijo_construccion_nom":                  "UInt32",
            "formacion_bruta_cap_fijo_maq_nac_nom":                       "UInt32",
            "formacion_bruta_cap_fijo_maq_imp_nom":                       "UInt32",
            "poblacion_anual":                                            "UInt32"
            }

        transform_step = TransformStep()
        agg_step = AggregatorStep("itp_indicators_q_n_nat", measures=["producto_bruto_interno_2007", "gasto_consumo_final_privado_2007", "gasto_consumo_gobierno_2007", "formacion_bruta_capital_2007", "formacion_bruta_capital_fijo_2007", "exportaciones_2007", "importaciones_2007", "producto_bruto_interno_nominal", "gasto_consumo_final_privado_nominal", "gasto_consumo_gobierno_nominal", "formacion_bruta_capital_nominal", "formacion_bruta_capital_fijo_nominal", "exportaciones_nominal", "importaciones_nominal", "producto_interno_bruto", "derechos_importacion_otros_impuestos", "valor_agregado_bruto_total", "agricultura_ganaderia_caza_silvicultura", "pesca_acuicultura", "extraccion_petroleo_gas_minerales_servicios_conexos", "manufactura", "construccion", "electricidad_gas_agua", "comercio_mantenimiento_reparacion_vehiculos", "transporte_almacenamiento_correo_mensajeria", "alojamiento_restaurantes", "telecomunicaciones_otros_servicios_informacion", "servicios_financieros_seguros_pensiones", "servicios_prestados_empresas", "administracion_publica_defensa", "otros_servicios", "formacion_bruta_cap_fijo_publico_2007", "formacion_bruta_cap_fijo_privado_2007", "formacion_bruta_cap_fijo_construccion_2007", "formacion_bruta_cap_fijo_maq_nac_2007", "formacion_bruta_cap_fijo_maq_imp_2007", "formacion_bruta_cap_fijo_publico_nom", "formacion_bruta_cap_fijo_privado_nom", "formacion_bruta_cap_fijo_construccion_nom", "formacion_bruta_cap_fijo_maq_nac_nom", "formacion_bruta_cap_fijo_maq_imp_nom", "poblacion_anual"])
        load_step = LoadStep("itp_indicators_q_n_nat", db_connector, if_exists="drop", pk=["ubigeo"], dtype=dtype, nullable_list=["poblacion_anual"])

        return [transform_step, agg_step, load_step]

def run_pipeline(params: dict):
    pp = itp_ind_quarter_n_nat_pipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
