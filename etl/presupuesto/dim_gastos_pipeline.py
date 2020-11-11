
import glob
import os
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import LoadStep

from .static import FOLDER


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        dimension = params["dimension"]

        filelist = glob.glob(os.path.join('etl', 'presupuesto', '{}'.format(FOLDER), '*_dim_{}.csv'.format(dimension)))

        df = pd.DataFrame()
        for filename in filelist:
            temp = pd.read_csv(filename, encoding='latin-1')
            df = df.append(temp)

        df.drop_duplicates(subset=[dimension], inplace=True)
        df.drop(columns=[dimension], inplace=True)
        df.dropna(inplace=True)

        df.rename(columns={
            'id': dimension,
            'name': '{}_name'.format(dimension)
        }, inplace=True)

        df[dimension] = df[dimension].astype(int)
        df.drop_duplicates(inplace=True)

        if dimension == 'ejecutora':
            df =  df.append({
                'ejecutora': 9999,
                'ejecutora_name': 'No especificado'
            }, ignore_index=True)

        return df


class DimensionsPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(name='dimension', dtype=str),
            Parameter(name='dim_type', dtype=str)
        ]

    @staticmethod
    def steps(params):
        dimension = params["dimension"]

        table_name = 'dim_mef_{}'.format(dimension)
        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        transform_step = TransformStep()
        load_step = LoadStep(table_name, db_connector,
                             if_exists='drop',
                             pk=[dimension],
                             dtype={dimension: params["dim_type"]})

        return [transform_step, load_step]


def run_pipeline(params: dict):
    pp = DimensionsPipeline()
    dims = {
        'sector': 'UInt8', 
        'pliego': 'UInt8',
        'ejecutora': 'UInt16',
        'funcion': 'UInt8',
        'division_funcional': 'UInt8',
        'programa_ppto': 'UInt8',
        'producto_proyecto': 'UInt32',
    }

    for dim, dim_type in dims.items():
        pp_params = {"dimension": dim, "dim_type": dim_type}
        pp_params.update(params)
        pp.run(pp_params)


if __name__ == "__main__":
    import sys
    from os import path
    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })