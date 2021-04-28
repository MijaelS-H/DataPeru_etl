import numpy as np
import pandas as pd
from os import path
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        # df = pd.read_excel('../../../datasets/anexos/2.Ubigeo_descripcion.xlsx', header=2, usecols='B,E,F')
        df = pd.read_excel(path.join(params["datasets"], "anexos", "2.Ubigeo_descripcion.xlsx"), header=2, usecols='B,E,F')

        df.replace(' ', np.nan, inplace=True)
        df.dropna(inplace=True)
        df = df[df['DEPARTAMENTO'] != 'DEPARTAMENTO'].copy()

        df.rename(columns={
            'DEPARTAMENTO': 'department_name',
            'PROVINCIA': 'province_name'
        }, inplace=True)

        columns = df.columns

        for column in columns:
            df['{}_id'.format(column.split('_')[0])] = df[column].astype(str).str[0:2]
            df[column] = df[column].astype(str).str[3:]

        df['province_id'] = df['department_id'] + df['province_id']
        df['nation_id'] = 'per'
        df['nation_name'] = 'Perú'

        df.drop_duplicates(subset=['province_id'], inplace = True)

        df.drop(columns=['DISTRITO', 'DISTRITO_id'], inplace=True)

        return df

class Ubigeo_Province_Pipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtype = {
            'nation_id': 'String',
            'nation_name': 'String',
            'department_id': 'String',
            'department_name': 'String',
            'province_id': 'String',
            'province_name': 'String'
        }

        transform_step = TransformStep()

        load_step = LoadStep(
            "dim_shared_ubigeo_province", db_connector, if_exists="drop", pk=["province_id"], dtype=dtype)

        return [transform_step, load_step]

def run_pipeline(params: dict):
    pp = Ubigeo_Province_Pipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
