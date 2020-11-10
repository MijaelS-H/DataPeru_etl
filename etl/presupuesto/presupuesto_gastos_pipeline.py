
import glob

import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import LoadStep
from etl.consistency import AggregatorStep

from .helpers import return_dimension
from .static import BASE, DATA_FOLDER, DIMENSIONS, DTYPE, FOLDER, TIPO_GOBIERNO


class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        prefix = params["prefix"]
        data = params["data"]

        base = BASE[prefix]

        temp = pd.DataFrame()
        for year in range(2014, 2020 + 1):
            # read files
            df = pd.read_csv(data, encoding='latin-1')
            df.columns = df.columns.str.lower()

            df = df[base + ['pia_{}'.format(year), 'pim_{}'.format(year), 'devengado_{}'.format(year)]].copy()
            df['year'] = year

            df.columns = base + ['pia', 'pim', 'devengado', 'year']
            temp = temp.append(df)

        # ubigeo replace
        if 'MANC' in data:
            temp['ubigeo'] = temp['sec_ejec']

        return temp


class ReplaceStep(PipelineStep):
    def run_step(self, df, params):
        prefix = params["prefix"]

        # dimensions replace
        dims = {}
        for column in list(DIMENSIONS[prefix].values()):
            dims[column] = return_dimension(prefix, column)
            df[column] = df[column].map(dict(zip(dims[column][column], dims[column]['id'])))

        # ids without name
        df['ejecutora'].replace({
            2171: 9999,
            2099: 9999
        }, inplace=True)

        return df


class TransformStep(PipelineStep):
    def run_step(self, df, params):
        prefix = params["prefix"]

        if prefix == 'GL':
            df['ubigeo'] = df['ubigeo'].astype(str).str.zfill(6)
            df['pliego'] = 0
            df['sector'] = 0
        else:
            df['ubigeo'] = 0

        if prefix == 'GR':
            df['sector'] = 0

        df['tipo_gobierno'] = df['tipo_gobierno'].replace(TIPO_GOBIERNO)
        df['departamento_meta'] = df['departamento_meta'].str.split('. ', n=1, expand=True)[0]
        df['ubigeo'] = df['ubigeo'].astype(str)

        return df


class PresupuestoPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(name='data', dtype=str),
            Parameter(name='prefix', dtype=str),
            Parameter(name='table', dtype=str)
        ]

    @staticmethod
    def steps(params):
        table_name = params["table"]
        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtype = DTYPE[params.get('prefix')]

        read_step = ReadStep()
        replace_step = ReplaceStep()
        transform_step = TransformStep()
        agg_step = AggregatorStep(table_name, measures=[])
        load_step = LoadStep(table_name, db_connector, if_exists='append', 
                             pk=['departamento_meta', 'year'], dtype=dtype, 
                             nullable_list=['pia', 'pim', 'devengado'])

        return [read_step, replace_step, transform_step, agg_step, load_step]


def run_pipeline(params: dict):
    pp = PresupuestoPipeline()

    for PREFIX in ['GN', 'GR', 'GL']:
        filelist = glob.glob('{}/G_{}_*.csv'.format(DATA_FOLDER, PREFIX))

        for filename in filelist:
            pp_params = {
                'data': filename,
                'prefix': PREFIX,
                'table': 'mef_presupuesto_gastos'
            }
            pp_params.update(params)
            pp.run(pp_params)


if __name__ == "__main__":
    import sys

    run_pipeline({
        "connector": "../conns.yaml",
        "datasets": sys.argv[1]
    })
