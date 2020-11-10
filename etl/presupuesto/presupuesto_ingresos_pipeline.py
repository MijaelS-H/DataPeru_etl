
import glob
import re

import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.helpers import query_to_df
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import LoadStep
from etl.consistency import AggregatorStep

from .helpers import return_dimension
from .static import BASE, DATA_FOLDER, DTYPE, TIPO_GOBIERNO


class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        data = params["data"]

        base = BASE[re.findall('(?<=2020_)(.*)(?=.csv)', data)[0]]

        temp = pd.DataFrame()

        for year in range(2014, 2020 + 1):
            df = pd.read_csv(data, encoding='latin-1')
            df.columns = df.columns.str.lower()
            df = df[base + ['monto_pia_{}'.format(year), 'monto_pim_{}'.format(year), 'monto_recaudado_{}'.format(year)]].copy()

            df['year'] = year

            df.columns = base + ['pia', 'pim', 'monto_recaudado'] + ['year']
            temp = temp.append(df)

        temp.rename(columns={
            'fuente_financ_nombre': 'fuente_financiamiento'
        }, inplace=True)

        return temp


class TransformStep(PipelineStep):
    def run_step(self, df, params):
        for column in ['sector_nombre', 'pliego_nombre', 'ubigeo']:
            if column not in df.columns:
                df[column] = '0'

        df.rename(columns={
            'sector_nombre': 'sector',
            'pliego_nombre': 'pliego',
            'fuente_financiamiento_nombre': 'fuente_financiamiento',
            'rubro_nombre': 'rubro'
        }, inplace=True)

        for column in ['sector', 'pliego', 'fuente_financiamiento', 'rubro']:
            # replace dimensions
            dim_query = 'SELECT {}, data_name, {}_nombre FROM dim_mef_ingresos_{}'.format(column, column, column)
            dim_result = query_to_df(self.connector, raw_query=dim_query)
            df[column] = df[column].replace(dict(zip(dim_result['data_name'], dim_result[column])))

        df['nivel_gobierno'] = df['nivel_gobierno'].replace(TIPO_GOBIERNO)
        df['ubigeo'] = df['ubigeo'].astype(int).astype(str).str.zfill(6)

        df['ubigeo'].replace({
            '000000': '999999'
        }, inplace=True)

        df[['sector', 'pliego', 'fuente_financiamiento', 'rubro']] = df[['sector', 'pliego', 'fuente_financiamiento', 'rubro']].astype(int)

        return df


class PresupuestoPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return[
            Parameter(name='data', dtype=str)
        ]

    @staticmethod
    def steps(params):
        data = params["data"]

        table_name = 'mef_presupuesto_ingresos'
        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtype = DTYPE[re.findall('(?<=2020_)(.*)(?=.csv)', data)[0]]

        read_step = ReadStep()
        transform_step = TransformStep(connector=db_connector)
        agg_step = AggregatorStep(table_name, measures=[])
        load_step = LoadStep(table_name, db_connector, if_exists='append',
                             pk=['ubigeo', 'year'], dtype=dtype)

        return [read_step, transform_step, agg_step, load_step]


def run_pipeline(params: dict):
    pp = PresupuestoPipeline()

    filelist = glob.glob('{}/ING_*.csv'.format(DATA_FOLDER))

    for filename in filelist:
        pp_params = {
            'data': filename
        }
        pp_params.update(params)
        pp.run(pp_params)


if __name__ == "__main__":
    import sys

    run_pipeline({
        "connector": "../conns.yaml",
        "datasets": sys.argv[1]
    })
