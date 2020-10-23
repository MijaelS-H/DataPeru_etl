
import glob
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep
from static import TIPO_GOBIERNO, BASE, DIMENSIONS, DTYPE, DATA_FOLDER
from helpers import return_dimension


class ReadStep(PipelineStep):
    def run_step(self, prev, params):

        base = BASE[params.get('prefix')]

        temp = pd.DataFrame()
        df_all_files =pd.DataFrame()

        for year in range(2014, 2020 + 1):
            df = pd.read_csv(params.get('data'), encoding='latin-1')
            df.columns = df.columns.str.lower()
            df = df[base + ['pia_{}'.format(year), 'pim_{}'.format(year), 'devengado_{}'.format(year)]].copy()

            df['year'] = year

            df.columns = base + ['pia', 'pim', 'devengado'] + ['year']
            temp = temp.append(df)
        df_all_files = df_all_files.append(temp)
        temp = pd.DataFrame()

        return df

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev

        # dimensions replace
        dims = {}
        for column in list(DIMENSIONS[params.get('prefix')].values()):
            dims[column] = return_dimension(params.get('prefix'), column)
            df[column] = df[column].map(dict(zip(dims[column][column], dims[column]['id'])))

        if params.get('prefix') == 'GL':
            df['ubigeo'] = df['ubigeo'].astype(str).str.zfill(6)
            df['pliego'] = 0
            df['sector'] = 0
        else:
            df['ubigeo'] = 0

        if params.get('prefix') == 'GR':
            df['sector'] = 0

        df['tipo_gobierno'] = df['tipo_gobierno'].replace(TIPO_GOBIERNO)
        df['departamento_meta'] = df['departamento_meta'].str.split('. ', n=1, expand=True)[0]
        df['ubigeo'] = df['ubigeo'].astype(str)

        return df


class PresupuestoPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return[
            Parameter(name='data', dtype=str),
            Parameter(name='prefix', dtype=str)
        ]

    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = DTYPE[params.get('prefix')]

        read_step = ReadStep()
        transform_step = TransformStep()
        load_step = LoadStep('mef_presupuesto_gastos', db_connector, if_exists='append', 
                             pk=['departamento_meta', 'year'], dtype=dtype, 
                             nullable_list=['pia', 'pim', 'devengado', 'ejecutora'])

        return [read_step, transform_step, load_step]

if __name__ == "__main__":
    pp = PresupuestoPipeline()

    for PREFIX in ['GN', 'GR', 'GL']:
        data = glob.glob('{}/G_{}_*.csv'.format(DATA_FOLDER, PREFIX))

        for file in data:
            print(file)
            pp.run({
                'data': file,
                'prefix': PREFIX
            })
