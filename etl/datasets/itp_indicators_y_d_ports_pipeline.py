from os import path
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from etl.consistency import AggregatorStep

depto_dict = {"Amazonas": "01", "Áncash": "02", "Apurímac": "03", "Arequipa": "04", "Ayacucho": "05", "Cajamarca": "06", "Callao": "07", "Callao 1/": "07", "Prov. Const. del Callao": "07", "Prov. Const.Callao": "07", "Cusco": "08", "Huancavelica": "09", "Huánuco": "10", "Ica": "11", "Junín": "12", "La Libertad": "13", "La libertad": "13", "Lambayeque": "14", "Lima": "15", "Lima 1/": "15", "Lima y Callao": "15", "Proyecto CARAL (PEZAC) 2/": "15", "Loreto": "16", "Madre de Dios": "17", "Moquegua": "18", "Pasco": "19", "Pasco 1/": "19", "Piura": "20", "Puno": "21", "San Martín": "22", "Tacna": "23", "Tumbes": "24", "Ucayali": "25", "Ucayali 1/": "25"}
puerto_dict = {"Atico": 1, "Bayovar": 2, "Callao": 3, "Casma": 4, "Chicama": 5, "Chimbote": 6, "Culebras": 7, "Huacho": 8, "Huarmey": 9, "Ilo": 10, "Lomas": 11, "Mancora": 12, "Matarani": 13, "Mollendo": 14, "Otros": 15, "Paita": 16, "Pimentel": 17, "Pisco": 18, "Puerto de Salaverry": 19, "Samanco": 20, "San José": 21, "Supe": 22, "Tambo de Mora": 23, "Vegeta": 24, "Chancay": 25, "Quilca": 26, "Zorritos": 27}
output = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRsfSo_N9dGWqzuCAoSEbCtu9TSeWXBspcnRXHOfgytwsqmwoQuLw02YCm1isYNMrJzSlnMOXkGChhH/pub?output=xlsx"

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        # Loading data
        df1 = pd.read_excel(io = path.join(params["datasets"],"20200318", "A. Economía", "A.73.xlsx"), skiprows = (0,1,2,4,5))[0:31]
        ports = pd.read_excel(output, usecols ="A,D")

        df1.rename(columns = {'2018 P/': 2018}, inplace = True)

        df_1  = pd.melt(df1,  id_vars = ["Puerto"], value_vars = [2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018], var_name = "year", value_name = "desembarque_recursos_marinos")

        df_1["Puerto"].replace({"Máncora": "Mancora", "Sechura/Parachique" : "Otros", "Bayóvar": "Bayovar", "Pimentel/Santa Rosa": "Pimentel", "Salaverry": "Puerto de Salaverry", "Coishco": "Otros", "Supe/Vidal": "Supe", "Végueta": "Vegeta", "Huacho/Carquín": "Huacho", "Pucusana": "Otros", "Pisco/San Andrés" : "Pisco", "La Planchada": "Otros"}, inplace = True)

        df = pd.merge(df_1, ports[["Puerto", "ubigeo"]], on ="Puerto", how="left")

        df.rename(columns = {'Puerto': 'puerto'}, inplace = True)

        df["ubigeo"].replace(depto_dict, inplace = True)
        df["puerto"].replace(puerto_dict, inplace = True)

        df["desembarque_recursos_marinos"].replace({"-": pd.np.nan}, inplace = True)

        df["ubigeo"].fillna("99", inplace = True)
        df["year"] = df["year"].astype(int)
        df["ubigeo"] = df["ubigeo"].astype(str)

        return df

class itp_indicators_y_d_ports_Pipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))

        dtype = {
            "ubigeo":                                   "String",
            "year":                                     "UInt16",
            "puerto":                                   "UInt8",
            "desembarque_recursos_marinos":             "UInt32"
        }

        transform_step = TransformStep()
        agg_step = AggregatorStep("itp_indicators_y_d_ports", measures=["desembarque_recursos_marinos"])
        load_step = LoadStep("itp_indicators_y_d_ports", db_connector, if_exists="drop", pk=["ubigeo", "puerto"], dtype=dtype, nullable_list=["desembarque_recursos_marinos"])

        return [transform_step, agg_step, load_step]

def run_pipeline(params: dict):
    pp = itp_indicators_y_d_ports_Pipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
