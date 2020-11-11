import re
import os
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep
from bamboo_lib.steps import LoadStep
from etl.survey_indicators.helpers import join_files
from .static import COLUMNS_RENAME
from .shared import ReplaceStep

from etl.consistency import AggregatorStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        data = pd.ExcelFile(os.path.join(params['datasets'], '20201007', '03. Indicadores estimados DSE - Encuestas (06-10-2020-07-10-2020))', '04  Encuesta Nacional de Victimización a Empresas (ENAVE) (07-10-2020)', '071020 Indicadores_ENAVE.xlsx'))

        nation = [x for x in data.sheet_names if re.findall('IND_.*_A', x) != []]
        department = [x for x in data.sheet_names if re.findall('IND_.*_B', x) != []]
        industry = [x for x in data.sheet_names if re.findall('IND_.*_C_2', x) != []]
        org_size = [x for x in data.sheet_names if re.findall('IND_.*_D', x) != []]

        df = join_files(data, nation, 'nivel_desagregación', 'nation_id')
        df = df.append(join_files(data, department, 'nivel_desagregación', 'department_id'), sort=False)
        df = df.append(join_files(data, industry, 'nivel_desagregación', 'industry_id'), sort=False)
        df = df.append(join_files(data, org_size, 'nivel_desagregación', 'size_id'), sort=False)

        return df

class FormatStep(PipelineStep):
    def run_step(self, prev, params):

        df = prev[0]

        df = df[['año', 'indicador', 'industry_id', 'nation_id', 'department_id', 'size_id', 'categoría', 'estimate', 'coef_var', 'popul_size']].copy()
        df.rename(columns=COLUMNS_RENAME, inplace=True)

        df['nation_id'].replace({'Nacional': 'per'}, inplace=True)
        df['nation_id'] = df['nation_id'].astype(str)

        df[['industry_id', 'nation_id', 'department_id', 'size_id', 'category_id']] = df[['industry_id', 'nation_id', 'department_id', 'size_id', 'category_id']].fillna(0)

        df['department_id'] = df['department_id'].astype(str)

        df[['year', 'indicator_id', 'industry_id', 'size_id', 'category_id', 'estimate', 'coef_var', 'popul_size']] = df[['year', 'indicator_id', 'industry_id', 'size_id', 'category_id', 'estimate', 'coef_var', 'popul_size']].astype(float)

        return df

class ENAVEPipeline(EasyPipeline):
    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open(params['connector']))

        dtype = {
            'nation_id':        'String',
            'department_id':    'String',
            'industry_id':      'UInt8',
            'indicator_id':     'UInt8',
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

        agg_step = AggregatorStep('inei_enave', measures=['estimate', 'coef_var', 'popul_size'])

        load_step = LoadStep('inei_enave', db_connector, if_exists='drop', 
                             pk=['nation_id', 'department_id', 'industry_id', 'size_id', 'year'], dtype=dtype,
                             nullable_list=['coef_var'])

        return [transform_step, replace_step, format_step, agg_step, load_step]

def run_pipeline(params: dict):
    pp = ENAVEPipeline()
    pp.run(params)


if __name__ == "__main__":
    import sys
    from os import path
    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline({
        "connector": path.join(__dirname, "..", "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })