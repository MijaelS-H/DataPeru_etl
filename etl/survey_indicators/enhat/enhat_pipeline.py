import re
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep
from bamboo_lib.steps import LoadStep
from etl.survey_indicators.helpers import join_files
from static import COLUMNS_RENAME, REPLACE_DICT
from shared import ReplaceStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        data = pd.ExcelFile('../../../datasets/20201007/03. Indicadores estimados DSE - Encuestas (06-10-2020-07-10-2020))/01 Encuesta Nacional de Habilidades al Trabajo (ENHAT) (06-10-2020)/061020 ENHAT_Indicadores.xlsx')

        nation = [x for x in data.sheet_names if re.findall('IND_.*_A', x) != []]
        nation.remove('IND_66_A')
        industry = [x for x in data.sheet_names if re.findall('IND_.*_C', x) != []]
        org_size = [x for x in data.sheet_names if re.findall('IND_.*_B', x) != []]
        workforce = ['IND_66_A']

        df = join_files(data, nation, 'nivel_desagregación', 'nation_id')
        df = df.append(join_files(data, industry, 'nivel_desagregación', 'industry_id'), sort=False)
        df = df.append(join_files(data, org_size, 'nivel_desagregación', 'size_id'), sort=False)
        df = df.append(join_files(data, workforce, 'nivel_desagregación', 'workforce_id'), sort=False)
        df['industry_id'] = df['industry_id'].replace(REPLACE_DICT)
        df['workforce_id'] = df['workforce_id'].replace(REPLACE_DICT)

        return df

class FormatStep(PipelineStep):
    def run_step(self, prev, params):

        df = prev[0]

        df = df[['año', 'indicador', 'industry_id','size_id','workforce_id','nation_id','categoría', 'estimate', 'coef_var', 'popul_size']].copy()
        df.rename(columns=COLUMNS_RENAME, inplace=True)

        df['nation_id'].replace({'Nacional': 'per'}, inplace=True)
        df['nation_id'] = df['nation_id'].astype(str)

        df[['nation_id', 'size_id', 'category_id','workforce_id']] = df[['nation_id', 'size_id', 'category_id','workforce_id']].fillna(0)
        df['industry_id'] = df['industry_id'].astype(str)

        df[['year', 'indicator_id',  'size_id', 'category_id','workforce_id', 'estimate', 'coef_var', 'popul_size']] = df[['year', 'indicator_id',  'size_id', 'category_id','workforce_id',  'estimate', 'coef_var', 'popul_size']].astype(float)
        
       
        return df


class ENHATPipeline(EasyPipeline):
    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'nation_id':        'String',
            'industry_id':      'String',
            'indicator_id':     'UInt8',
            'workforce_id':     'UInt8',
            'year':             'UInt16',
            'category_id':      'UInt8',
            'size_id':          'UInt8',
            'estimate':         'Float32',
            'coef_var':         'Float32',
            'popul_size':       'Float32'
        }

        transform_step = TransformStep()
        replace_step = ReplaceStep()
        format_step = FormatStep()
        load_step = LoadStep('inei_enhat', db_connector, if_exists='drop', 
                             pk=['nation_id', 'industry_id', 'size_id', 'workforce_id', 'year'], dtype=dtype,
                             nullable_list=['coef_var'])

        return [transform_step, replace_step, format_step, load_step]

if __name__ == "__main__":
    pp = ENHATPipeline()
    pp.run({})
        