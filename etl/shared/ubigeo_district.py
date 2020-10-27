import numpy as np
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep

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
    },
    {
        'department_name': 'Otro',
        'province_name': 'Otro',
        'district_name': 'Otro',
        'department_id': '99',
        'province_id': '9999',
        'district_id': '999999'
    }
]

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_excel('../../../datasets/anexos/2.Ubigeo_descripción.xlsx', header=2, usecols='B,E,F')
        
        df.replace(' ', np.nan, inplace=True)
        df.dropna(inplace=True)
        df = df[df['DEPARTAMENTO'] != 'DEPARTAMENTO'].copy()

        df.rename(columns={
            'DEPARTAMENTO': 'department_name',
            'PROVINCIA': 'province_name',
            'DISTRITO': 'district_name'
        }, inplace=True)

        columns = df.columns

        for column in columns:
            df['{}_id'.format(column.split('_')[0])] = df[column].astype(str).str[0:2]
            df[column] = df[column].astype(str).str[3:]

        df['province_id'] = df['department_id'] + df['province_id']
        df['district_id'] = df['province_id'] + df['district_id']

        df['department_name'] = df['department_name'].str.strip()
        df['province_name'] = df['province_name'].str.strip()
        df['district_name'] = df['district_name'].str.strip()

        df = df.append(MISSING_UBIGEO)

        provinces = df[['department_name', 'province_name', 'department_id', 'province_id']].copy()
        provinces = provinces.drop_duplicates()
        provinces['district_name'] = provinces.apply(lambda x: 'Otros distritos de la provincia de {}'.format(x['province_name']), axis=1)
        provinces['district_id'] = provinces['province_id'] + '00'
        provinces = provinces[['department_name', 'province_name', 'district_name', 'department_id', 'province_id', 'district_id']].copy()
        
        departments = df[['department_name', 'department_id']].copy()
        departments = departments.drop_duplicates()
        departments['province_name'] = departments.apply(lambda x: 'Otras provincias del departamento de {}'.format(x['department_name']), axis=1)
        departments['district_name'] = departments.apply(lambda x: 'Otros distritos del departamento de {}'.format(x['department_name']), axis=1)
        departments['province_id'] = departments['department_id'] + '00'
        departments['district_id'] = departments['province_id'] + '00'
        departments = departments[['department_name', 'province_name', 'district_name', 'department_id', 'province_id', 'district_id']].copy()
        
        df = df.append(provinces)
        df = df.append(departments)

        df['nation_id'] = 'per'
        df['nation_name'] = 'Perú'

        return df

class UbigeoPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'nation_id': 'String',
            'nation_name': 'String',
            'department_id': 'String',
            'department_name': 'String',
            'province_id': 'String',
            'province_name': 'String',
            'district_id': 'String',
            'district_name': 'String'
        }

        transform_step = TransformStep()

        load_step = LoadStep(
            "dim_shared_ubigeo_district", db_connector, if_exists="drop", pk=["district_id"], dtype=dtype)

        return [transform_step, load_step]

if __name__ == "__main__":

    pp = UbigeoPipeline()
    pp.run({})