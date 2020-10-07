import pandas as pd
from bamboo_lib.helpers import grab_parent_dir
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
path = grab_parent_dir('../../') + "/datasets/20200318"

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        # Loading data
        years_ = [2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018]
        df1 = pd.read_excel(io = '{}/{}/{}'.format(path, 'A. Economía', "A.14.xlsx"), skiprows = (0,1,2,4,5,6,7,8,9), reset_index = True)
        df2 = pd.read_excel(io = '{}/{}/{}'.format(path, 'A. Economía', "A.21.xlsx"), skiprows = (0,1,2,4,5,6,7,8,9), reset_index = True)

        # For cycle to repeated step for the 2 datasets
        for item in [df1, df2]: 
            item.rename(columns = {'Unnamed: 1': 'actividad_economica_id', 'Unnamed: 2': 'sub_actividad_economica_id', '2016P/': 2016, '2017P/': 2017, '2018E/': 2018}, inplace = True)
            item["sub_actividad_economica_id"].fillna(item["actividad_economica_id"], inplace=True)
            item["actividad_economica_id"].fillna(method = "ffill", inplace = True)

            item.drop([2,5,42,45,48,54,60,61], axis = 0, inplace = True)
            item.drop(["Actividad Económica"], axis = 1, inplace = True)
            
            item["actividad_economica_id"].replace({'Agricultura, ganadería, caza y silvicultura' : '01', 'Pesca y acuicultura' : '02', 'Extracción de petróleo, gas, minerales y servicios conexos' : '03', 'Manufactura' : '04', 'Electricidad, gas y agua' : '05', 'Construcción' : '06', 'Comercio, mantenimiento y reparación de vehículos automotores y motocicletas' : '07', 'Transporte, almacenamiento, correo y mensajería' : '08', 'Alojamiento y restaurantes' : '09', 'Telecomunicaciones y otros servicios de información' : '10', 'Telecomunicaciones y otros servicos de información' : '10', 'Servicios financieros, seguros y pensiones' : '11', 'Servicios prestados a empresas' : '12', 'Administración pública y defensa' : '13', 'Otros servicios' : '14'}, inplace = True)

            item["sub_actividad_economica_id"].replace({"Agricultura, ganadería, caza y silvicultura": "0101", "Pesca y acuicultura": "0201", "Extracción de petróleo crudo, gas natural y servicios conexos": "0301", "Extracción de minerales y servicios conexos": "0302", "Procesamiento y conservación de carnes": "0401", "Elaboración y preservación de pescado": "0402", "Elaboración de harina y aceite de pescado": "0403", "Procesamiento y conservación de frutas y vegetales": "0404", "Elaboración de aceites y grasas de origen vegetal y animal": "0405", "Fabricación de productos lácteos": "0406", "Molinería, fideos, panadería y otros": "0407", "Elaboración y refinación de azúcar": "0408", "Elaboración de otros productos alimenticios": "0409", "Elaboración de alimentos preparados para animales": "0410", "Elaboración de bebidas y productos del tabaco": "0411", "Fabricación de textiles": "0412", "Fabricación de prendas de vestir": "0413", "Fabricación de cuero y calzado": "0414", "Fabricación de madera y productos de madera": "0415", "Fabricación de papel y productos de papel": "0416", "Impresión y reproducción de grabaciones": "0417", "Refinación de petróleo": "0418", "Fabricación de sustancias químicas básicas y abonos": "0419", "Fabricación de productos químicos": "0420", "Fabricación de productos farmacéuticos y medicamentos": "0421", "Fabricacion de productos de caucho y plástico": "0422", "Fabricación de productos minerales no metálicos": "0423", "Industria básica de hierro y acero": "0424", "Industria de metales preciosos y de metales no ferrosos": "0425", "Fabricación de productos metálicos diversos": "0426", "Fabricación de productos informáticos, electrónicos y ópticos": "0427", "Fabricación de maquinaria y equipo": "0428", "Construcción de material de transporte": "0429", "Fabricación de muebles": "0430", "Otras industrias manufactureras": "0431", "Electricidad, gas y agua": "0501", "Construcción": "0601", "Comercio, mantenimiento y reparación de vehículos automotores y motocicletas": "0701", "Transporte, almacenamiento, correo y mensajería": "0801", "Alojamiento y restaurantes": "0901", "Telecomunicaciones": "1001", "Otros servicios de información y comunicación": "1002", "Servicios financieros": "1101", "Seguros y pensiones": "1102", "Servicios profesionales, científicos y técnicos": "1201", "Alquiler de vehículos, maquinaria y equipo y otros": "1202", "Agencias de viaje y operadores turísticos": "1203", "Otros servicios administrativos y de apoyo a empresas": "1204", "Administración pública y defensa": "1301", "Actividades inmobiliarias": "1401", "Educación": "1402", "Salud": "1403", "Servicios sociales y de asociaciones u organizaciones no mercantes": "1404", "Otras actividades de servicios personales": "1405"}, inplace = True)

        # Melt step for each dataset, given the currency used (2007 / cte)
        df_1 = pd.melt(df1, id_vars = ["actividad_economica_id", "sub_actividad_economica_id"], value_vars = years_,
                    var_name = 'year', value_name = "pib_54_2007")

        df_2 = pd.melt(df2, id_vars = ["actividad_economica_id", "sub_actividad_economica_id"], value_vars = years_,
                    var_name = 'year', value_name = "pib_54_cte")

        # Creating cod and merge step
        df_1['code'] = df_1['sub_actividad_economica_id'].astype('str') + df_1['year'].astype('str')
        df_2['code'] = df_2['sub_actividad_economica_id'].astype('str') + df_2['year'].astype('str')
        df = pd.merge(df_1,  df_2[['code', 'pib_54_cte']], on = 'code', how = 'left')

        # Dropping unused columns and change type of value columns
        df.drop(["actividad_economica_id", "code"], axis=1, inplace=True)
        df["pib_54_2007"] = df["pib_54_2007"].astype(int)
        df["pib_54_cte"] = df["pib_54_cte"].astype(int)

        return df

class itp_indicators_y_n_nat_54(EasyPipeline):
  
    @staticmethod
    def parameter_list():
        return [
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "year":                                      "UInt8",
            "sub_actividad_economica_id":                "String",
            "pib_54_2007":                               "UInt32",
            "pib_54_cte":                                "UInt32"
            }

        transform_step = TransformStep()
        load_step = LoadStep(
            "itp_indicators_y_n_nat_54", db_connector, if_exists="drop", pk=["sub_actividad_economica_id"], dtype=dtype, 
        )

        return [transform_step, load_step]

if __name__ == "__main__":
    pp = itp_indicators_y_n_nat_54()
    pp.run({})