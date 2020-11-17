from os import path
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from etl.consistency import AggregatorStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        # Loading data
        df_ = pd.read_excel(io = path.join(params["datasets"],"20200318", "A. Economía", "A.94.xlsx"), skiprows = (0,1,2,3,5,6), usecols = "A:M")[0:31]

        # Renaming columns + removing " " strings elements
        df_.rename(columns = {'Actividad Económica': 'sub_actividad_economica_id', '2014': 2014, '2015 P/': 2015, '2016 P/': 2016, '2017 E/': 2017, '2018 E/': 2018}, inplace = True)
        df_["sub_actividad_economica_id"] = df_["sub_actividad_economica_id"].str.strip()

        # Replacing sub activities id's with the ones from _54 shared table
        df_["sub_actividad_economica_id"].replace({"Procesamiento y conservación de carnes": "0401", "Elaboración y preservación de pescado": "0402", "Elaboración de harina y aceite de pescado": "0403", "Procesamiento y conservación de frutas y vegetales": "0404", "Elaboración de aceites y grasas de origen vegetal y animal": "0405", "Fabricación de productos lácteos": "0406", "Molinería, fideos, panadería y otros": "0407", "Elaboración y refinación de azúcar": "0408", "Elaboración de otros productos alimenticios": "0409", "Elaboración de alimentos preparados para animales": "0410", "Elaboración de bebidas y productos del tabaco": "0411", "Fabricación de textiles": "0412", "Fabricación de prendas de vestir": "0413", "Fabricación de cuero y calzado": "0414", "Fabricación de madera y productos de madera": "0415", "Fabricación de papel y productos de papel": "0416", "Impresión y reproducción de grabaciones": "0417", "Refinación de petróleo": "0418", "Fabricación de sustancias químicas básicas y abonos": "0419", "Fabricación de productos químicos": "0420", "Fabricación de productos farmacéuticos y medicamentos": "0421", "Fabricación de productos de caucho y plástico": "0422", "Fabricación de productos minerales no metálicos": "0423", "Industria básica de hierro y acero": "0424", "Industria de metales preciosos y de metales no ferrosos": "0425", "Fabricación de productos metálicos diversos": "0426", "Fabricación de productos informáticos, electrónicos y ópticos": "0427", "Fabricación de maquinaria y equipo": "0428", "Construcción de material de transporte": "0429", "Fabricación de muebles": "0430", "Otras industrias manufactureras": "0431"         }, inplace = True)

        # Melt step to get VAB production by year
        df = pd.melt(df_, id_vars = ["sub_actividad_economica_id"], value_vars = [2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018], var_name = 'year', value_name = "valor_agregado_bruto_cte_2007")

        df["year"] = df["year"].astype(int)
        df["nation_id"] = "per"

        return df

class itp_indicators_y_n_manu_mill_soles_2007_pipeline(EasyPipeline):

    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))
        dtype = {
            "nation_id":                                "String",
            "sub_actividad_economica_id":               "String",
            "valor_agregado_bruto_cte_2007":            "Float64",
            "year":                                     "UInt16",
            }

        transform_step = TransformStep()
        agg_step = AggregatorStep("itp_indicators_y_n_manu_mill_soles_2007", measures=["valor_agregado_bruto_cte_2007"])
        load_step = LoadStep("itp_indicators_y_n_manu_mill_soles_2007", db_connector, if_exists="drop", pk=["nation_id"], dtype=dtype)

        return [transform_step, agg_step, load_step]

def run_pipeline(params: dict):
    pp = itp_indicators_y_n_manu_mill_soles_2007_pipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })

