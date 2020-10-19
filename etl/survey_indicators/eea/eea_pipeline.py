
import re
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep
from bamboo_lib.steps import LoadStep
from etl.survey_indicators.helpers import join_files
from static import COLUMNS_RENAME
from shared import ReplaceStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        data = pd.ExcelFile('../../../datasets/20201007/03. Indicadores estimados DSE - Encuestas (06-10-2020-07-10-2020))/06 Encuesta Económica Anual  (EEA) (07-10-2020)/071020 EEA_Indicadores.xlsx')

        # sheet names
        geo = [x for x in data.sheet_names if re.findall('IND_.*_A', x) != []]
        industry = [x for x in data.sheet_names if re.findall('IND_.*_B', x) != []]

        df = join_files(data, geo, 'nivel_desagregación', 'nation_id')
        df = df.append(join_files(data, industry, 'nivel_desagregación', 'industry_id'), sort=False)

        # pre-processing
        for col in ['indicador', 'industry_id']:
            df[col] = df[col].str.strip()

        return df

class FormatStep(PipelineStep):
    def run_step(self, prev, params):
        # df subset
        df = prev[0]

        df = df[['año', 'indicador', 'industry_id', 'nation_id', 'estimate', 'coef_var', 'popul_size',]].copy()

        df.rename(columns=COLUMNS_RENAME, inplace=True)

        # column types
        df['nation_id'].replace({'Nacional': 'per'}, inplace=True)
        df['nation_id'] = df['nation_id'].astype(str)

        df[['industry_id', 'nation_id']] = df[['industry_id', 'nation_id']].fillna(0)

        df[['year', 'indicator_id', 'estimate', 'coef_var', 'popul_size']] \
            = df[['year', 'indicator_id', 'estimate', 'coef_var', 'popul_size']].astype(float)

        df['industry_id'] = df['industry_id'].astype(int).astype(str)

        return df

class EEAPipeline(EasyPipeline):
    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'nation_id':    'String',
            'industry_id':  'String',
            'indicator_id': 'UInt8',
            'year':         'UInt16',
            'estimate':     'Float64',
            'coef_var':     'Float32',
            'popul_size':   'Float32'
        }

        transform_step = TransformStep()
        replace_step = ReplaceStep()
        format_step = FormatStep()
        load_step = LoadStep('inei_eea', db_connector, if_exists='drop', 
                             pk=['nation_id', 'industry_id', 'year'], dtype=dtype)

        return [transform_step, replace_step, format_step, load_step]

if __name__ == "__main__":
    pp = EEAPipeline()
    pp.run({})