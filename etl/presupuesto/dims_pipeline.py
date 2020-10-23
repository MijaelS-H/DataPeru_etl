
import glob
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep
from static import FOLDER


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        data = glob.glob('{}/*_dim_{}.csv'.format(FOLDER, params.get('dimension')))
        print(data)

        df = pd.DataFrame()
        for file in data:
            temp = pd.read_csv(file)
            df = df.append(temp)
        temp = []

        df.drop_duplicates(subset=[params.get('dimension')], inplace=True)

        df.drop(columns=[params.get('dimension')], inplace=True)

        df.dropna(inplace=True)

        df.rename(columns={
            'id': params.get('dimension'),
            'name': '{}_name'.format(params.get('dimension'))
        }, inplace=True)

        df[params.get('dimension')] = df[params.get('dimension')].astype(int)

        return df

class DimensionsPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return[
            Parameter(name='dimension', dtype=str),
            Parameter(name='dim_type', dtype=str)
        ]

    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            params.get('dimension'): params.get('dim_type')
        }

        transform_step = TransformStep()
        load_step = LoadStep('dim_mef_{}'.format(params.get('dimension')), db_connector, if_exists='drop', 
                             pk=[params.get('dimension')], dtype=dtype)

        return [transform_step, load_step]

if __name__ == "__main__":
    pp = DimensionsPipeline()

    for dim, dim_type in {'sector': 'UInt8', 
                          'pliego': 'UInt8',
                          'ejecutora': 'UInt16',
                          'funcion': 'UInt8',
                          'division_funcional': 'UInt8',
                          'programa_ppto': 'UInt8',
                          'producto_proyecto': 'UInt32'}.items():
        pp.run({
            'dimension': dim,
            'dim_type': dim_type
        })