import glob
import os

import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import LoadStep


class TransformStep(PipelineStep):
    def run_step(self, prev_result, params):
        dimension = params["dimension"]
        dimension_nombre = "{}_nombre".format(dimension)

        filelist = os.path.join(params["datasets"], "download", "ING_*.csv")

        df = pd.DataFrame()
        for filename in glob.glob(filelist):
            with open(filename, 'r', encoding='latin-1') as f:
                temp = pd.read_csv(f)
            temp.rename(columns={"fuente_financ": "fuente_financiamiento"}, inplace=True)
            df = df.append(temp, sort=False)

        df.columns = df.columns.str.lower()
        df = df[[x for x in df.columns if dimension in x]]
        df.dropna(subset=[dimension], inplace=True)
        df[dimension] = df[dimension].astype(int)

        df.drop_duplicates(subset=[dimension_nombre], inplace=True)

        df["data_name"] = df[dimension_nombre]
        df[dimension_nombre] = df[dimension_nombre].str.capitalize()
        df[dimension] = range(1, df.shape[0] + 1)

        return df


class DimensionsPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(name="dimension", dtype=str),
            Parameter(name="dim_type", dtype=str),
        ]

    @staticmethod
    def steps(params):
        dimension = params["dimension"]

        table_name = "dim_mef_ingresos_{}".format(dimension)
        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))

        transform_step = TransformStep()
        load_step = LoadStep(
            table_name,
            db_connector,
            if_exists="drop",
            pk=[dimension],
            dtype={dimension: params["dim_type"]},
        )

        return [transform_step, load_step]


def run_pipeline(params: dict):
    pp = DimensionsPipeline()
    dims = {
        "sector": "UInt8",
        "pliego": "UInt16",
        "rubro": "UInt8",
        "fuente_financiamiento": "UInt8",
    }

    for dim, dim_type in dims.items():
        pp.run({"dimension": dim, "dim_type": dim_type, **params})


if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"), 
        "datasets": sys.argv[1]
    })
