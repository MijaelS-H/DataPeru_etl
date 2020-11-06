import pandas as pd
from bamboo_lib.helpers import grab_parent_dir
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from etl.consistency import AggregatorStep

path = grab_parent_dir("../../") + "/datasets/20200318"

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        # Loading data
        df = pd.read_excel(io = "{}/{}/{}".format(path, "A. Economía", "A.153.xls"), skiprows = (0,1,2,3), usecols = "A,C,D,G,H,K,L,N,O")[0:108]
        df.columns = ["categoria", "n_arribos_nacionales", "n_arribos_extranjeros", "n_pernoctaciones_nacionales", "n_pernoctaciones_extranjeros", "permanencia_prom_nacionales", "permanencia_prom_extranjeros", "tasa_ocupacion_hab", "tasa_ocupacion_camas"]
        df["categoria"] = df["categoria"].astype(str)

        df["year"] = pd.np.nan
        df.loc[df["categoria"].str.contains("00|01"), "year"] = df["categoria"]
        df.loc[df["categoria"].str.contains("00|01"), "categoria"] = pd.np.nan
        df["year"].fillna(method = "ffill", inplace = True)

        df.dropna(axis=0, how="any", inplace = True)
        df["categoria"].replace({'5 Estrellas': 5, '4 Estrellas': 4, '3 Estrellas': 3, '2 Estrellas': 2,'1 Estrella': 1, 'Albergue': 6, 'Ecolodge': 7, 'No Categorizados': 8 }, inplace = True)
        df["year"] = df["year"].astype(int)
        df["ubigeo"] = "per"

        return df

class itp_indicators_y_n_tourism_capacity_pipeline(EasyPipeline):

    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))
        dtype = {
            "ubigeo":                                   "String",
            "categoria":                                "UInt8",
            'n_arribos_nacionales':                     "UInt32",
            'n_arribos_extranjeros':                    "UInt32",
            'n_pernoctaciones_nacionales':              "UInt32",
            'n_pernoctaciones_extranjeros':             "UInt32",
            'permanencia_prom_nacionales':              "Float32",
            'permanencia_prom_extranjeros':             "Float32",
            'tasa_ocupacion_hab':                       "Float32",
            'tasa_ocupacion_camas':                     "Float32",
            "year":                                     "UInt16",
            }

        transform_step = TransformStep()
        agg_step = AggregatorStep("itp_indicators_y_n_tourism_capacity", measures=["n_arribos_nacionales", "n_arribos_extranjeros", "n_pernoctaciones_nacionales", "n_pernoctaciones_extranjeros", "permanencia_prom_nacionales", "permanencia_prom_extranjeros", "tasa_ocupacion_hab", "tasa_ocupacion_camas"])
        load_step = LoadStep("itp_indicators_y_n_tourism_capacity", db_connector, if_exists="drop", pk=["ubigeo"], dtype=dtype)

        return [transform_step, agg_step, load_step]

if __name__ == "__main__":
    pp = itp_indicators_y_n_tourism_capacity_pipeline()
    pp.run({})