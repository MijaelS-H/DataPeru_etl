import re
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep
from bamboo_lib.steps import LoadStep
from etl.survey_indicators.helpers import join_files
from static import COLUMNS_RENAME, DEPARTMENT_EXCEPTIONS
from shared import ReplaceStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        data = pd.ExcelFile('../../../datasets/20201007/03. Indicadores estimados DSE - Encuestas (06-10-2020-07-10-2020))/07 Encuesta Nacional de Programas Presupuestales (ENAPRES) (18-20-2020)/181020 ENAPRES _Indicadores.xlsx')

        # sheet names
        geo = [x for x in data.sheet_names if re.findall('IND_.*_A', x) != []]
        sector_type = [x for x in data.sheet_names if re.findall('IND_.*_B', x) != []]
        location_type = [x for x in data.sheet_names if re.findall('IND_.*_C', x) != []]
        department = [x for x in data.sheet_names if re.findall('IND_.*_D', x) != []]

        df = join_files(data, geo, 'nivel_desagregación', 'nation_id')
        df = df.append(join_files(data, sector_type, 'nivel_desagregación', 'sector_type_id'), sort=False)
        df = df.append(join_files(data, location_type, 'nivel_desagregación', 'location_type_id'), sort=False)
        df = df.append(join_files(data, department, 'nivel_desagregación', 'department_id'), sort=False)

        # pre-processing
        df["department_id"] = df["department_id"].replace(DEPARTMENT_EXCEPTIONS)
        df = df.drop(df[df["categoría"]== '6'].index)

        return df

class FormatStep(PipelineStep):
    def run_step(self, prev, params):
        # df subset
        df = prev[0]

        df = df[['año', 'indicador', 'department_id', 'nation_id', 'sector_type_id', 'location_type_id','categoría', 'estimate', 'coef_var', 'popul_size',]].copy()

        df.rename(columns=COLUMNS_RENAME, inplace=True)

        # column types
        df[['department_id', 'nation_id', 'sector_type_id', 'location_type_id']] = df[['department_id', 'nation_id', 'sector_type_id', 'location_type_id']].fillna(0)

        df['nation_id'].replace({'Nacional': 'per'}, inplace=True)
        df['nation_id'] = df['nation_id'].astype(str)

        df['department_id'] = df['department_id'].astype(str)

        df[['year', 'location_type_id', 'sector_type_id', 'indicator_id', 'category_id', 'estimate', 'coef_var', 'popul_size']] = df[['year', 'location_type_id', 'sector_type_id', 'indicator_id', 'category_id', 'estimate', 'coef_var', 'popul_size']].astype(float)
        
        return df

class ENAPRESPipeline(EasyPipeline):
    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'nation_id':        'String',
            'department_id':    'String',
            'sector_type_id':   'UInt8',
            'location_type_id': 'UInt8',
            'indicator_id':     'UInt8',
            'year':             'UInt16',
            'category_id':      'UInt8',
            'estimate':         'Float32',
            'coef_var':         'Float32',
            'popul_size':       'Float32'
        }

        transform_step = TransformStep()
        replace_step = ReplaceStep()
        format_step = FormatStep()
        load_step = LoadStep('inei_enapres', db_connector, if_exists='drop', 
                             pk=['nation_id', 'department_id', 
                             'sector_type_id', 'location_type_id', 'year'], dtype=dtype,
                             nullable_list=['coef_var'])

        return [transform_step, replace_step, format_step, load_step]

if __name__ == "__main__":
    pp = ENAPRESPipeline()
    pp.run({})