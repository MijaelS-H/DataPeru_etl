
import re
import os
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep
from bamboo_lib.steps import LoadStep
from etl.survey_indicators.helpers import join_files
from .static import TEXT_REPLACE, COLUMNS_RENAME
from .shared import ReplaceStep
from etl.consistency import AggregatorStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        data = pd.ExcelFile(os.path.join(params['datasets'], '03_Indicadores_estimados_DSE_Encuestas', '03_Encuesta_Nacional_de_Hogares_(ENAHO)', 'ENAHO_Indicadores.xlsx'))

        # sheet names
        nation = [x for x in data.sheet_names if re.findall('IND_.*_A', x) != []]
        region = [x for x in data.sheet_names if re.findall('IND_.*_B', x) != []]
        geo = [x for x in data.sheet_names if re.findall('IND_.*_C', x) != []]
        department = [x for x in data.sheet_names if re.findall('IND_.*_D', x) != []]

        # join sheets
        df = join_files(data, nation, 'nivel_desagregación', 'nation_id')
        df = df.append(join_files(data, region, 'nivel_desagregación', 'region_id'), sort=False)
        df = df.append(join_files(data, geo, 'nivel_desagregación', 'geo_id'), sort=False)
        df = df.append(join_files(data, department, 'nivel_desagregación', 'department_id'), sort=False)

        for ele in TEXT_REPLACE:
            df['categoría'] = df['categoría'].str.replace(ele[0], ele[1])
        df['categoría'] = df['categoría'].str.strip().str.capitalize()

        df = df[['año', 'indicador', 'nation_id', 'region_id', 'geo_id', 'department_id', 'categoría', 'estimate', 'coef_var', 'popul_size']].copy()

        df['department_id'] = df['department_id'].replace({'Ancash': 'Áncash'})

        return df

class FormatStep(PipelineStep):
    def run_step(self, prev, params):
        # df subset
        df = prev[0]

        df.rename(columns=COLUMNS_RENAME, inplace=True)

        df[['nation_id', 'region_id', 'geo_id', 'department_id']] = df[['nation_id', 'region_id', 'geo_id', 'department_id']].fillna(0)

        # column types
        df['nation_id'].replace({'Nacional': 'per'}, inplace=True)
        df['nation_id'] = df['nation_id'].astype(str)

        df[['year', 'indicator_id', 'region_id', 'geo_id', 'category_id', 'estimate', 'coef_var', 'popul_size']] \
            = df[['year', 'indicator_id', 'region_id', 'geo_id', 'category_id', 'estimate', 'coef_var', 'popul_size']].astype(float)

        df['department_id'] = df['department_id'].astype(str)

        return df

class ENAHOPipeline(EasyPipeline):
    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open(params['connector']))

        dtype = {
            'nation_id':     'String',
            'department_id': 'String',
            'geo_id':        'UInt8',
            'region_id':     'UInt8',
            'indicator_id':  'UInt8',
            'category_id':   'UInt8',
            'year':          'UInt16',
            'estimate':      'Float32',
            'coef_var':      'Float32',
            'popul_size':    'Float32'
        }

        transform_step = TransformStep()
        replace_step = ReplaceStep(connector=db_connector)
        format_step = FormatStep()

        agg_step = AggregatorStep('inei_enaho', measures=['estimate', 'coef_var', 'popul_size'])

        load_step = LoadStep('inei_enaho', db_connector, if_exists='drop', 
                             pk=['nation_id', 'department_id', 'year'], dtype=dtype,
                             nullable_list=['coef_var', 'category_id', 'geo_id', 'region_id'])

        return [transform_step, replace_step, format_step, load_step]

def run_pipeline(params: dict):
    pp = ENAHOPipeline()
    pp.run(params)


if __name__ == "__main__":
    import sys
    from os import path
    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline({
        "connector": path.join(__dirname, "..", "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })