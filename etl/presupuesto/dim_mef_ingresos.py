
import os
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, UnzipToFolderStep, LoadStep
from bamboo_lib.helpers import query_to_df
from .static import INGRESO_DIMENSIONS_COLS, INGRESO_DTYPES_COLS


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        column = params.get('dimension')
        dim_type = params.get('type')

        if (column == 'pliego' and dim_type == 'temp'):
            query = 'SELECT DISTINCT month_id, sector, {}, {}_nombre, tipo_gobierno FROM temp_mef_ingresos'.format(column, column)
        elif (column == 'pliego' and dim_type == 'dim'):
            query = 'SELECT * FROM temp_dim_mef_ingresos_pliego'.format(column, column)
        elif column == 'ejecutora':
            query = 'SELECT DISTINCT month_id, sec_ejec AS {}, {}_nombre FROM temp_mef_ingresos'.format(column, column)
        else:    
            query = 'SELECT DISTINCT month_id, {}, {}_nombre FROM temp_mef_ingresos'.format(column, column)

        df = query_to_df(self.connector, raw_query=query)

        # make sure the latest ids are at the end
        if not (column == 'pliego' and dim_type == 'dim'):
            df.sort_values(by=['month_id'], ascending=True, inplace=True)

        # str type id
        if column not in ['tipo_gobierno', 'pliego']:
            print('Set column type:', column, 'as "int"')
            df[column] = df[column].astype(int)

        elif (column == 'pliego' and dim_type == 'temp'):
            print('Creating unique pliego id using sector and pliego ids as "str"')
            df['sector'] = df['sector'].astype(float).astype(int)
            df['pliego'] = df['pliego'].astype(float).astype(int)
            df_nac_reg = df.loc[df['tipo_gobierno'].isin(['E', 'R'])].copy()
            df_loc = df.loc[df['tipo_gobierno'] == 'M'].copy()
            df_loc_w_sector = df_loc.loc[df_loc.sector != 0].copy()
            df_loc_wo_sector = df_loc.loc[df_loc.sector == 0].drop_duplicates(subset=[column, '{}_nombre'.format(column)], keep='last').reset_index(drop=True).copy()
    
            df_nac_reg['temp_pliego_id'] = df_nac_reg['tipo_gobierno'] + df_nac_reg['sector'].astype('str').str.zfill(2) + df_nac_reg['pliego'].astype('str').str.zfill(4)
            df_loc_w_sector['temp_pliego_id'] = df_loc_w_sector['tipo_gobierno'] + df_loc_w_sector['sector'].astype('str').str.zfill(2) + df_loc_w_sector['pliego'].astype('str').str.zfill(4)
            df_loc_wo_sector['temp_pliego_id'] = df_loc_wo_sector['tipo_gobierno'] + df_loc_wo_sector['sector'].astype('str').str.zfill(2) + df_loc_wo_sector.index.astype('str').str.zfill(4)
    
            df_loc_wo_sector.drop_duplicates(subset='pliego_nombre', keep='last').copy()

            df = df_nac_reg.append([df_loc_w_sector, df_loc_wo_sector])
            df[column] = df['temp_pliego_id']

        if params.get('type') == 'dim':
            df = df.drop_duplicates(subset=[column], keep='last').copy()

        print('Creating dimension:', column)
        # keep last name of id over time
        if params.get('type') == 'dim':
            df = df[[column, '{}_nombre'.format(column)]].drop_duplicates(subset=[column], keep='last').copy()
        else:
            df = df[[column, '{}_nombre'.format(column)]].drop_duplicates(subset=[column, '{}_nombre'.format(column)], keep='last').copy()

        return df

class DimensionsPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(name='dimension', dtype=str),
            Parameter(name='type', dtype=str)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open(params.get("connector")))

        dtypes = {
            params.get('dimension'):                    INGRESO_DTYPES_COLS[params.get('dimension')],
            '{}_name'.format(params.get('dimension')):  'String'
        }

        transform_step = TransformStep(connector=db_connector)

        if params.get('type') == 'dim':
            load_step = LoadStep('dim_mef_ingresos_{}'.format(params.get('dimension')), db_connector, if_exists='drop', pk=[params.get('dimension')], dtype=dtypes)
        else:
            load_step = LoadStep('temp_dim_mef_ingresos_{}'.format(params.get('dimension')), db_connector, if_exists='drop', pk=[params.get('dimension')], dtype=dtypes)

        return [transform_step, load_step]


def run_pipeline(params: dict):
    pp = DimensionsPipeline()
    for dimension in INGRESO_DIMENSIONS_COLS:
        if dimension == "pliego":
            pp_params = {"dimension": dimension, "type": "temp"}
            pp_params.update(params)
            pp.run(pp_params)

        pp_params = {"dimension": dimension, "type": "dim"}
        pp_params.update(params)
        pp.run(pp_params)


if __name__ == "__main__":
    import sys
    from os import path
    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml")
    })