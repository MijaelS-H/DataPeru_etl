
import os
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, UnzipToFolderStep, LoadStep
from bamboo_lib.helpers import query_to_df
from static import INGRESO_DIMENSIONS_COLS, INGRESO_DTYPES_COLS


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        column = params.get('dimension')
        query = 'SELECT month_id, {}, {}_nombre FROM temp_mef_ingresos'.format(column, column)
        df = query_to_df(self.connector, raw_query=query)

        # make sure the latest ids are at the end
        df.sort_values(by=['month_id'], ascending=True, inplace=True)

        # str type id
        if column != 'tipo_gobierno':
            print('Set column type:', column, 'as "int"')
            df[column] = df[column].astype(int)

        print('Creating dimension:', column)
        # keep last name of id over time
        df = df[[column, '{}_nombre'.format(column)]].drop_duplicates(subset=[column], keep='last').copy()
        df['{}_nombre'.format(column)] = df['{}_nombre'.format(column)].str.capitalize()

        return df

class DimensionsPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(name='dimension', dtype=str)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open(params.get("connector")))

        dtypes = {
            params.get('dimension'):                    INGRESO_DTYPES_COLS[params.get('dimension')],
            '{}_name'.format(params.get('dimension')):  'String'
        }

        transform_step = TransformStep(connector=db_connector)

        load_step = LoadStep('dim_mef_ingresos_{}'.format(params.get('dimension')), db_connector, if_exists='drop', pk=[params.get('dimension')], dtype=dtypes)

        return [transform_step, load_step]


def run_pipeline(params: dict):
    pp = DimensionsPipeline()
    for dimension in INGRESO_DIMENSIONS_COLS:
        pp_params = {"dimension": dimension}
        pp_params.update(params)
        pp.run(pp_params)


if __name__ == "__main__":
    import sys
    from os import path
    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml")
    })