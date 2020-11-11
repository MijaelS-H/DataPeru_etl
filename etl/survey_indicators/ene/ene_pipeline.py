
import re
import pandas as pd
import os
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep
from bamboo_lib.steps import LoadStep
from etl.survey_indicators.helpers import join_files
from .static import COLUMNS_RENAME
from .shared import ReplaceStep

from etl.consistency import AggregatorStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        data = pd.ExcelFile(os.path.join(params['datasets'], '20201007', '03. Indicadores estimados DSE - Encuestas (06-10-2020-07-10-2020))', '05 Encuestas Nacional de Empresas (ENE) (07-10-2020)', '071020 ENE_Indicadores.xlsx'))

        # sheet names
        geo = [x for x in data.sheet_names if re.findall('IND_.*_A', x) != []]
        industry = [x for x in data.sheet_names if re.findall('IND_.*_B', x) != []]

        df = join_files(data, geo, 'nivel_desagregación', 'nation_id')
        df = df.append(join_files(data, industry, 'nivel_desagregación', 'industry_id'), sort=False)

        # pre-processing
        for text_exception in [['ù', 'ú'], ['è', 'é'], ['.', '' ]]:
            df['categoría'] = df['categoría'].str.replace(text_exception[0], text_exception[1])
        df['categoría'] = df['categoría'].astype(str).str.strip()

        df['industry_id'] = df['industry_id'].str.strip()

        return df

class FormatStep(PipelineStep):
    def run_step(self, prev, params):
        # df subset
        df = prev[0]

        df = df[['año', 'indicador', 'industry_id', 'nation_id', 'categoría', 'estimate', 'coef_var', 'popul_size',]].copy()

        df.rename(columns=COLUMNS_RENAME, inplace=True)

        # column types
        df['nation_id'].replace({'Nacional': 'per'}, inplace=True)
        df['nation_id'] = df['nation_id'].astype(str)

        df[['industry_id', 'nation_id']] = df[['industry_id', 'nation_id']].fillna(0)

        df[['year', 'indicator_id', 'industry_id', 'category_id', 'estimate', 'coef_var', 'popul_size']] \
            = df[['year', 'indicator_id', 'industry_id', 'category_id', 'estimate', 'coef_var', 'popul_size']].astype(float)

        return df

class ENEPipeline(EasyPipeline):
    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open(params['connector']))

        dtype = {
            'nation_id':    'String',
            'industry_id':  'UInt8',
            'indicator_id': 'UInt8',
            'year':         'UInt16',
            'category_id':  'UInt8',
            'estimate':     'Float32',
            'coef_var':     'Float32',
            'popul_size':   'Float32'
        }

        transform_step = TransformStep()
        replace_step = ReplaceStep()
        format_step = FormatStep()

        agg_step = AggregatorStep('inei_ene', measures=['estimate', 'coef_var', 'popul_size'])

        load_step = LoadStep('inei_ene', db_connector, if_exists='drop', 
                             pk=['nation_id', 'industry_id', 'year'], dtype=dtype,
                             nullable_list=['coef_var'])

        return [transform_step, replace_step, format_step, agg_step, load_step]

def run_pipeline(params: dict):
    pp = ENEPipeline()
    pp.run(params)


if __name__ == "__main__":
    import sys
    from os import path
    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline({
        "connector": path.join(__dirname, "..", "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })