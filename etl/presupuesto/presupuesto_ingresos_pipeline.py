
import re
import glob
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep
from static import TIPO_GOBIERNO, BASE, DTYPE, DATA_FOLDER
from helpers import return_dimension


class ReadStep(PipelineStep):
    def run_step(self, prev, params):

        base = BASE[re.findall('(?<=2020_)(.*)(?=.csv)', params.get('data'))[0]]

        temp = pd.DataFrame()

        for year in range(2014, 2020 + 1):
            df = pd.read_csv(params.get('data'), encoding='latin-1')
            df.columns = df.columns.str.lower()
            df = df[base + ['monto_pia_{}'.format(year), 'monto_pim_{}'.format(year), 'monto_recaudado_{}'.format(year)]].copy()

            df['year'] = year

            df.columns = base + ['pia', 'pim', 'monto_recaudado'] + ['year']
            temp = temp.append(df)
        
        temp.rename(columns={
            'fuente_financ': 'fuente_financiamiento'
        }, inplace=True)

        return temp

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev

        for column in ['sector', 'pliego', 'ubigeo']:
            if column not in df.columns:
                df[column] = 0

        df['nivel_gobierno'] = df['nivel_gobierno'].replace(TIPO_GOBIERNO)
        df['ubigeo'] = df['ubigeo'].astype(int).astype(str).str.zfill(6)

        df['ubigeo'].replace({
            '000000': '999999'
        }, inplace=True)

        return df


class PresupuestoPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return[
            Parameter(name='data', dtype=str)
        ]

    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = DTYPE[re.findall('(?<=2020_)(.*)(?=.csv)', params.get('data'))[0]]

        read_step = ReadStep()
        transform_step = TransformStep()
        load_step = LoadStep('mef_presupuesto_ingresos', db_connector, if_exists='append', 
                             pk=['ubigeo', 'year'], dtype=dtype)

        return [read_step, transform_step, load_step]

if __name__ == "__main__":
    pp = PresupuestoPipeline()

    data = glob.glob('{}/ING_*.csv'.format(DATA_FOLDER))

    for file in data:
        print(file)
        
        pp.run({
            'data': file
        })
