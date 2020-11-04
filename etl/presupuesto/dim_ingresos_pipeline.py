
import glob
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep
from static import DATA_FOLDER


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        data = glob.glob('{}/ING_*.csv'.format(DATA_FOLDER))
        df = pd.DataFrame()
        for ele in data:
            temp = pd.read_csv(ele, encoding='latin-1')
            temp.rename(columns={'fuente_financ': 'fuente_financiamiento'}, inplace=True)
            df = df.append(temp, sort=False)

        df.columns = df.columns.str.lower()
        df = df[[x for x in df.columns if params.get('dimension') in x]]
        df.dropna(subset=[params.get('dimension')], inplace=True)
        df[params.get('dimension')] = df[params.get('dimension')].astype(int)

        df.drop_duplicates(subset=['{}_nombre'.format(params.get('dimension'))], inplace=True)

        df['data_name'] = df['{}_nombre'.format(params.get('dimension'))]
        df['{}_nombre'.format(params.get('dimension'))] = df['{}_nombre'.format(params.get('dimension'))].str.capitalize()
        df[params.get('dimension')] = range(1, df.shape[0] + 1)

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
        load_step = LoadStep('dim_mef_ingresos_{}'.format(params.get('dimension')), db_connector, if_exists='drop', 
                             pk=[params.get('dimension')], dtype=dtype)

        return [transform_step, load_step]

if __name__ == "__main__":
    pp = DimensionsPipeline()
    for dim, dim_type in {'sector': 'UInt8', 
                          'pliego': 'UInt16',
                          'rubro': 'UInt8',
                          'fuente_financiamiento': 'UInt8'
                          }.items():
        pp.run({
            'dimension': dim,
            'dim_type': dim_type
        })