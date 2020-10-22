import os
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.helpers import grab_parent_dir
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from static import COLUMNS_RENAME, COUNTRIES_DICT, HS_DICT, REGIMEN_DICT, UBIGEO_DICT, UNIT_DICT

path = grab_parent_dir('../../') + "/datasets/20200318/180320 Inf. Administrativa SUNAT/"

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.DataFrame()

        # Open and read exports files in defined path
        for file in os.listdir(path):
            _df = pd.read_csv(path + file, sep='|', encoding='latin-1', low_memory=False, dtype={
                'ubigeo': 'str'
            })
    
            df = df.append(_df)

        # Select columns to use
        df = df[['regimen', 'cadu', 'cpaides', 'femb', 'freg', 'cnan', 'vfobserdol', 'vpesnet', 'tunifis', 'qunifis', 'ubigeo']].copy()

        # Rename columns to comprensive name
        df.rename(columns=COLUMNS_RENAME, inplace=True)

        # Replace NANDINA code by HS code at 6-digit level
        df['hs6_id'] = df['hs6_id'].str.replace('.','').str[:-4]

        # Replace
        df['country_id'] = df['country_id'].str.lower()

        df['trade_flow_id'].replace(REGIMEN_DICT, inplace=True)
        df['unit'].replace(UNIT_DICT, inplace=True)
        df['country_id'].replace(COUNTRIES_DICT, inplace=True)
        df['hs6_id'].replace(HS_DICT, inplace=True)
        df['ubigeo'].replace(UBIGEO_DICT, inplace=True)

        df['shipment_date_id'] = df.apply(lambda x: x['registry_date_id'] if x['shipment_date_id'] == 0 else x['shipment_date_id'], axis=1)
        df['registry_date_id'] = df.apply(lambda x: x['shipment_date_id'] if x['registry_date_id'] == 0 else x['registry_date_id'], axis=1)

        df = df.groupby(by=['trade_flow_id', 'aduana_id', 'country_id', 'shipment_date_id', 'registry_date_id', 'hs6_id', 'unit', 'ubigeo']).sum()

        df = df.reset_index()

        return df

class ExportsPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtypes = {
            'trade_flow_id':        'UInt8',
            'aduana_id':            'UInt16',
            'country_id':           'String',
            'shipment_date_id':     'UInt32',
            'registry_date_id':     'UInt32',
            'hs6_id':               'String',
            'unit':                 'String',
            'ubigeo':               'String',
            'trade_value':          'Float64',
            'net_weight_value':     'Float64',
            'quatity':              'Float64'
        }

        transform_step = TransformStep()
        load_step = LoadStep(
            'sunat_exports', 
            connector = db_connector, 
            if_exists = 'drop', 
            pk = ['trade_flow_id', 'aduana_id', 'country_id', 'shipment_date_id', 'registry_date_id', 'hs6_id', 'unit', 'ubigeo'],
            dtype = dtypes, 
            nullable_list = []
        )

        return [transform_step, load_step]

if __name__ == '__main__':
   pp = ExportsPipeline()
   pp.run({})
   