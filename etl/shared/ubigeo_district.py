import numpy as np
import pandas as pd
from os import path
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

MISSING_MANC = [
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidades Municipales',
        'district_name': 'Mancomundad Municipal de la Amazonía de Puno',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350001'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal de Uscovilca',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350002'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal del Valle de la Leche',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350003'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal de Salhuana',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350004'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal Valle Sur - Cusco',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350005'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal de Huaytapallana',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350006'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal de Qapaq Ñan',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350007'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal de Páramos y Cuencas del Jaen',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350008'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal Integración Fronteriza Collpa',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350009'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal Frente Norte del Ilucán',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350010'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal del Norte de Celendin',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350011'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal del Valle de los Volcanes',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350013'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal de la Cuenca del Río San Juan',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350014'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal Tallán',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350015'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal Nueva Requena - Pade Marquez-NR-PM',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350017'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal Los Wari - Manwari',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350018'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal Cuenca Mantaro - Mantaro',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350019'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal del Mantaro Vizcatán - VRAE',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350020'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal de la Quebrada del Mantaro',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350021'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal Rio Cachi - Manriocachi',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350023'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal de la Subcuenca del Río Chipillico',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350024'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal Tupac Amaru II',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350025'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal de la Cuenca Valle de Lurán',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350026'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal del Corredor Mantaro',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350027'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal de Hatun Huaylas',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350028'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal Cuenca Cachi',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350029'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal Angaraes Sur',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350030'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal Lima Centro',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350031'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal de la Cuenca del Río Santo Tomás',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350032'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal Chillaos - Región Amazonas',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350033'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal del Valle Santa Eulalia',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350036'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal del Valle Fortaleza y del Santa',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350037'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal Margen Derecha de Caylloma',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350038'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal de las Cabezadas del Sur de Lucanas - Ayacucho',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350039'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal Señor Cautivo de Ayabaca',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350040'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal del Yacus',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350041'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal Waraq',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350042'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal Cuenca del Río Cumbaza',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350043'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal del Valle de Yanamarca',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350044'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal Tres Cuencas: Santa - Fortaleza - Pativilca',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350045'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal Circuito Mochica',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350046'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal de los Distritos de Oxapampa',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350047'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal Lima Sur',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350049'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal de las Cuencas del Chotano - Conchano "Manuel José Becerra Silva"',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350050'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': "Mancomunidad Municipal Q'Anchi de la Provincia de Canchis",
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350051'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal por la Integración de San Martín y Loreto',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350052'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal VRAEM del Norte',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350054'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal de Cuencas de Selva Central',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350055'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal del Nor Oriente del Perú',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350056'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal Cono Norte - Huancavelica',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350057'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal Cuenca del Mantaro Huanchuy - VRAEM',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350058'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal de la Cuenca Sur Oriental de Arequipa',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350059'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal Tilacancha',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350061'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal Valle de las Cataratas',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350062'
    },
    {
        'department_name': 'Mancomunidades Municipales',
        'province_name': 'Mancomunidad Municipales',
        'district_name': 'Mancomunidad Municipal Alto Utcubamba',
        'department_id': '35',
        'province_id': '3500',
        'district_id': '350063'
    }
]

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        # df = pd.read_excel('../../../datasets/anexos/2.Ubigeo_descripción.xlsx', header=2, usecols='B,E,F')
        df = pd.read_excel(path.join(params["datasets"],"anexos", "2.Ubigeo_descripción.xlsx"), header=2, usecols='B,E,F')

        df.rename(columns={
            'DEPARTAMENTO': 'department_name',
            'PROVINCIA': 'province_name',
            'DISTRITO': 'district_name'
        }, inplace=True)
        
        
        df = df.replace(' ', np.nan)
        df.dropna(how='any', inplace=True)
                
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
        df = df.drop_duplicates(subset=['district_id'])
        
        provinces = df[['department_name', 'province_name', 'department_id', 'province_id']].copy()
        provinces = provinces.drop_duplicates()
        provinces['district_name'] = provinces.apply(lambda x: 'Otros distritos de la provincia de {}'.format(x['province_name']) if x['province_name'] != "Otros" else 'Otros distritos', axis=1)
        provinces['district_id'] = provinces['province_id'] + '00'
        provinces = provinces[['department_name', 'province_name', 'district_name', 'department_id', 'province_id', 'district_id']].copy()
                
        departments = df[['department_name', 'department_id']].copy()
        departments = departments.drop_duplicates()
        departments['province_name'] = departments.apply(lambda x: 'Otras provincias del departamento de {}'.format(x['department_name']) if x['department_name'] != "Otros" else "Otras provincias", axis=1)
        departments['district_name'] = departments.apply(lambda x: 'Otros distritos del departamento de {}'.format(x['department_name']) if x['department_name'] != "Otros" else "Otros distritos", axis=1)
        departments['province_id'] = departments['department_id'] + '00'
        departments['district_id'] = departments['province_id'] + '00'
        departments = departments[['department_name', 'province_name', 'district_name', 'department_id', 'province_id', 'district_id']].copy()
                
        df = df.append(provinces)
        df = df.append(departments)
        df = df.append(MISSING_MANC)
        
        df['nation_id'] = 'per'
        df['nation_name'] = 'Perú'
        
        df = df.reset_index(drop=True)
        
        df = df.drop_duplicates()
        df.drop([43, 1887, 2081], inplace=True)
        
        df.replace({
            'Anco_Huallo': 'Anco-Huallo'
        }, inplace=True)

        return df

class Ubigeo_District_Pipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

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
        load_step = LoadStep("dim_shared_ubigeo_district", db_connector, if_exists="drop", pk=["district_id"], dtype=dtype)

        return [transform_step, load_step]

def run_pipeline(params: dict):
    pp = Ubigeo_District_Pipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
