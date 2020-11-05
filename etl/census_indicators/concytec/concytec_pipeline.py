from os import path

import numpy as np
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep
from bamboo_lib.steps import LoadStep
from etl.consistency import AggregatorStep

from .indicator import INDICATOR_CENTER, INDICATOR_INSTITUTE
from .static import COLUMNS_RENAME, DTYPE, LIST_DICT, LIST_NULL


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        #read modules
        list_name = [
            path.join(params["datasets"], "20201001", "02. Información Censos (01-10-2020)", "04 CENSO NACIONAL DE INVESTIGACIÓN Y DESARROLLO", "03 BASE DE DATOS", "cap200.dta"),
            path.join(params["datasets"], "20201001", "02. Información Censos (01-10-2020)", "04 CENSO NACIONAL DE INVESTIGACIÓN Y DESARROLLO", "03 BASE DE DATOS", "cap300-400.dta"),
        ]

        df = [pd.read_stata(x) for x in list_name]

        #choose columns
        df[0] = df[0][['ID', 'CCDD', 'CCPP', 'CCDI', 'P217', 'P213', 'P214']]

        df[1] = df[1][['CCDD', 'CCDI', 'CCPP', 'ID', 'IDCI', 'UBIGCI', 'P213', 'P328_1_1', 'P328_1_2', 'P328_2_1', 'P328_2_2',
               'P329_1_1', 'P329_1_2', 'P329_1_3', 'P329_1_4', 'P329_1_5', 'P329_1_6', 'P329_2_1', 'P329_2_2', 'P329_2_3',
               'P329_2_4', 'P329_2_5', 'P329_2_6', 'P404_1_1', 'P404_1_2', 'P404_1_3', 'P404_1_4', 'P404_2_1', 
               'P404_2_2', 'P404_2_3', 'P404_2_4']]
        
        #rename modules
        df[0].rename(columns = COLUMNS_RENAME, inplace=True)
        df[1].rename(columns = COLUMNS_RENAME, inplace=True)
    
        #fix geo dimension
        for i in range(2):
            df[i]['nation_id'] = 'per'
            df[i]['province_id'] = df[i]['department_id'] + df[i]['province_id']
            df[i]['district_id'] = df[i]['province_id'] + df[i]['district_id']
            df[i]['count'] = 1
            df[i]['P213'] = df[i]['P213'].str.strip()
            df[i]['P213'] = df[i]['P213'].replace(LIST_DICT[0])

        #change column type
        df[0]['P214'] = df[0]['P214'].replace(LIST_DICT[1])
        
        for i in range(1,8):
            df[0]['P214_aux_'+ str(i)] = df[0].apply(lambda x: 1 if x['P214'] == i else 0, axis=1)  
        
        df[0]['P217'] = df[0]['P217'].astype(str)
        df[0]['P217'] = df[0]['P217'].replace(LIST_DICT[2]).astype(int)
        df[0]['P217'] = df[0]['P217'].fillna(0)
        
        list_int = ['P404_1_4', 'P404_2_1', 'P404_1_2', 'P404_1_1', 'P404_2_4', 'P404_2_2', 'P404_1_3', 'P404_2_3']
        
        list_bin = ['P328_1_1', 'P328_2_1', 'P328_1_2', 'P328_2_2', 'P329_1_1', 'P329_1_2', 'P329_1_3', 'P329_1_4', 
                    'P329_1_5', 'P329_1_6', 'P329_2_1', 'P329_2_2', 'P329_2_3', 'P329_2_4', 'P329_2_5', 'P329_2_6']

        df[1][list_bin] = df[1][list_bin].astype(str)
        df[1][list_bin] = df[1][list_bin].replace(LIST_DICT[2]).astype(int)
        df[1][list_int] = df[1][list_int].fillna(0)

        for i in range(1,6):
            df[0]['P213_aux_'+ str(i)] = df[0].apply(lambda x: 1 if x['P213'] == i else 0, axis=1)
            df[1]['P213_aux_'+ str(i)] = df[1].apply(lambda x: 1 if x['P213'] == i else 0, axis=1)
            df[1]['P328_1_1_aux_'+ str(i)] = df[1].apply(lambda x: x['P328_1_1'] if x['P213'] == i else 0, axis=1)
            df[1]['P328_2_1_aux_'+ str(i)] = df[1].apply(lambda x: x['P328_2_1'] if x['P213'] == i else 0, axis=1)
            df[1]['P328_1_2_aux_'+ str(i)] = df[1].apply(lambda x: x['P328_1_2'] if x['P213'] == i else 0, axis=1)
            df[1]['P328_2_2_aux_'+ str(i)] = df[1].apply(lambda x: x['P328_2_2'] if x['P213'] == i else 0, axis=1)
        
        df[1]['P329_aux_1'] = df[1].apply(lambda x: 1 if ((x['P329_1_1']==1) | (x['P329_1_2']==1) | (x['P329_1_3']==1) | (x['P329_1_4']==1) | (x['P329_1_5']==1) | (x['P329_1_6']==1)) else 0, axis=1)
        df[1]['P329_aux_2'] = df[1].apply(lambda x: 1 if ((x['P329_2_1']==1) | (x['P329_2_2']==1) | (x['P329_2_3']==1) | (x['P329_2_4']==1) | (x['P329_2_5']==1) | (x['P329_2_6']==1)) else 0, axis=1)
        
        #indicators
        df_final = pd.DataFrame()

        for i in ['district_id', 'department_id', 'province_id', 'nation_id']:
            list_geo = ['department_id', 'province_id', 'district_id', 'nation_id']
            list_geo.remove(i)

            df_indicator_1 = df[0].copy()
            df_indicator_2 = df[1].copy()

            df_indicator_1.drop(columns = list_geo, inplace=True)
            df_indicator_2.drop(columns = list_geo, inplace=True)

            df_indicator_1 = df_indicator_1.groupby([i]).sum().reset_index()
            df_indicator_2 = df_indicator_2.groupby([i]).sum().reset_index()

            df_indicator_1 = INDICATOR_INSTITUTE(df_indicator_1, i)
            df_indicator_2 = INDICATOR_CENTER(df_indicator_2, i) 

            df_indicator = df_indicator_1.merge(df_indicator_2, on=[i], how='left')    
            df_final = df_final.append(df_indicator, sort=False)
        
        df_final[['district_id', 'department_id', 'province_id', 'nation_id']] = df_final[['district_id', 'department_id', 'province_id', 'nation_id']].fillna(0).astype(str)
        df_final['year'] = 2016
        
        return df_final


class CONCYTECPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        table_name = "inei_concytec"
        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))

        transform_step = TransformStep()

        agg_step = AggregatorStep(table_name, measures=[])

        load_step = LoadStep(table_name, db_connector, if_exists='drop', 
                             pk=['district_id', 'province_id', 'department_id',
                                 'nation_id', 'year'], dtype=DTYPE,
                             nullable_list=LIST_NULL)

        return [transform_step, agg_step, load_step]


def run_pipeline(params: dict):
    pp = CONCYTECPipeline()
    pp.run(params)


if __name__ == "__main__":
    import sys
    
    run_pipeline({
        "connector": "../../conns.yaml", 
        "datasets": sys.argv[1]
    })
