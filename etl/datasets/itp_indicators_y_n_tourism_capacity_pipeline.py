from os import path
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from etl.consistency import AggregatorStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        # Loading data
        df = pd.read_excel(io = path.join(params["datasets"],"20200318", "A. Economía", "A.153.xls"), skiprows = (0,1,2,3), usecols = "A,C,D,G,H,K,L,N,O")[0:108]
        df.columns = ["categoria", "n_arribos_nacionales", "n_arribos_extranjeros", "n_pernoctaciones_nacionales", "n_pernoctaciones_extranjeros", "permanencia_prom_nacionales", "permanencia_prom_extranjeros", "tasa_ocupacion_hab", "tasa_ocupacion_camas"]
        df["categoria"] = df["categoria"].astype(str)

        df["year"] = pd.np.nan
        df.loc[df["categoria"].str.contains("00|01"), "year"] = df["categoria"]
        df.loc[df["categoria"].str.contains("00|01"), "categoria"] = pd.np.nan
        df["year"].fillna(method = "ffill", inplace = True)

        df.dropna(axis=0, how="any", inplace = True)
        df["categoria"].replace({'5 Estrellas': 5, '4 Estrellas': 4, '3 Estrellas': 3, '2 Estrellas': 2,'1 Estrella': 1, 'Albergue': 6, 'Ecolodge': 7, 'No Categorizados': 8 }, inplace = True)
        df["year"] = df["year"].astype(int)
        df["nation_id"] = "per"

        list_ = ["n_arribos_nacionales", "n_arribos_extranjeros", "n_pernoctaciones_nacionales", "n_pernoctaciones_extranjeros", "permanencia_prom_nacionales", "permanencia_prom_extranjeros", "tasa_ocupacion_hab", "tasa_ocupacion_camas", "year"]
        for item in list_:
            df[item] = df[item].astype(float)

        return df

class itp_indicators_y_n_tourism_capacity_pipeline(EasyPipeline):

    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))
        dtype = {
            "nation_id":                                   "String",
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
        load_step = LoadStep("itp_indicators_y_n_tourism_capacity", db_connector, if_exists="drop", pk=["nation_id"], dtype=dtype)

        return [transform_step, agg_step, load_step]

def run_pipeline(params: dict):
    pp = itp_indicators_y_n_tourism_capacity_pipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })


