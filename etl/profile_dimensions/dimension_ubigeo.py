import pandas as pd
from os import path
import numpy as np
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep

MISSING_UBIGEO = [
    {
        'department_name': 'Loreto',
        'province_name': 'Maynas',
        'district_name': 'Putumayo',
        'department_id': '16',
        'province_id': '1601',
        'district_id': '160109'
    },
    {
        'department_name': 'Loreto',
        'province_name': 'Maynas',
        'district_name': 'Teniente Manuel Clavero',
        'department_id': '16',
        'province_id': '1601',
        'district_id': '160114'
    },
    {
        'department_name': 'Loreto',
        'province_name': 'Alto Amazonas',
        'district_name': 'Barranca',
        'department_id': '16',
        'province_id': '1602',
        'district_id': '160203'
    },
    {
        'department_name': 'Loreto',
        'province_name': 'Alto Amazonas',
        'district_name': 'Manseriche',
        'department_id': '16',
        'province_id': '1602',
        'district_id': '160207'
    },
    {
        'department_name': 'Loreto',
        'province_name': 'Alto Amazonas',
        'district_name': 'Morona',
        'department_id': '16',
        'province_id': '1602',
        'district_id': '160208'
    },
    {
        'department_name': 'Loreto',
        'province_name': 'Alto Amazonas',
        'district_name': 'Pastaza',
        'department_id': '16',
        'province_id': '1602',
        'district_id': '160209'
    }
]

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_excel(path.join(params["datasets"], "anexos", "2.Ubigeo_descripcion.xlsx"), header=2, usecols='B,E,F')

        df.rename(columns={
            'DEPARTAMENTO': 'department_name',
            'PROVINCIA': 'province_name',
            'DISTRITO': 'district_name'}, inplace=True)

        df = df.replace(' ', np.nan)
        df.dropna(how = "any", axis = 0, inplace=True)

        df.drop(df.loc[df["department_name"] == "DEPARTAMENTO"].index, inplace = True)

        for column in df.columns:
            df["{}_id".format(column.split('_')[0])] = df[column].astype(str).str[0:2]
            df[column] = df[column].astype(str).str[3:]

        df['department_name'] = df['department_name'].str.strip()
        df['province_name'] = df['province_name'].str.strip()
        df['district_name'] = df['district_name'].str.strip()

        df['province_id'] = df['department_id'] + df['province_id']
        df['district_id'] = df['province_id'] + df['district_id']

        df = df.append(MISSING_UBIGEO)
        df = df.drop_duplicates(subset=['district_id'])

        df['nation_id'] = 'per'
        df['nation_name'] = 'Perú'

        df = df.reset_index(drop=True)

        df.replace({'Anco_Huallo': 'Anco-Huallo'}, inplace=True)

        return df

class Dimension_Ubigeo_Pipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtype = {
            'nation_id':                  'String',
            'nation_name':                'String',
            'department_id':              'String',
            'department_name':            'String',
            'province_id':                'String',
            'province_name':              'String',
            'district_id':                'String',
            'district_name':              'String'
        }

        transform_step = TransformStep()
        load_step = LoadStep("dimension_ubigeo_district", db_connector, if_exists="drop", pk=["district_id"], dtype=dtype)

        return [transform_step, load_step]

def run_pipeline(params: dict):
    pp = Dimension_Ubigeo_Pipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
