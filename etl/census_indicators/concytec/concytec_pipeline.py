import numpy as np
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep
from bamboo_lib.steps import LoadStep
from static import COLUMNS_RENAME, LIST_DICT, DTYPE, LIST_NULL
from indicator import INDICATOR

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        #read modules
        list_name = [
            '../../../../datasets/20201001/02. Información Censos (01-10-2020)/04 CENSO NACIONAL DE INVESTIGACIÓN Y DESARROLLO/03 BASE DE DATOS/cap200.dta',
            '../../../../datasets/20201001/02. Información Censos (01-10-2020)/04 CENSO NACIONAL DE INVESTIGACIÓN Y DESARROLLO/03 BASE DE DATOS/cap300-400.dta'
        ]

        df = [pd.read_stata(x) for x in list_name]

        #choose columns
        df[0] = df[0][['ID', 'CCDD', 'CCPP', 'CCDI', 'P217']]

        df[1] = df[1][['CCDD', 'CCDI', 'CCPP', 'ID', 'IDCI', 'UBIGCI', 'P213', 'P214', 'P328_1_1', 'P328_1_2', 'P328_2_1', 'P328_2_2',
               'P329_1_1', 'P329_1_2', 'P329_1_3', 'P329_1_4', 'P329_1_5', 'P329_1_6', 'P329_2_1', 'P329_2_2', 'P329_2_3',
               'P329_2_4', 'P329_2_5', 'P329_2_6', 'P404_1_1', 'P404_1_2', 'P404_1_3', 'P404_1_4', 'P404_2_1', 
               'P404_2_2', 'P404_2_3', 'P404_2_4']]
        
        #merge modules
        df_merge = df[1].merge(df[0], on=['ID', 'CCDD', 'CCPP', 'CCDI'], how='left')
        df_merge.rename(columns = COLUMNS_RENAME, inplace=True)

        #fix geo dimension
        df_merge['nation_id'] = 'per'
        df_merge['province_id'] = df_merge['department_id'] + df_merge['province_id']
        df_merge['district_id'] = df_merge['province_id'] + df_merge['district_id']

        #change column type
        df_merge['count'] = 1

        df_merge['P213'] = df_merge['P213'].str.strip()
        df_merge['P213'] = df_merge['P213'].replace(LIST_DICT[0])
        df_merge['P214'] = df_merge['P214'].replace(LIST_DICT[1])
        
        for i in range(1,8):
            df_merge['P214_aux_'+ str(i)] = df_merge.apply(lambda x: 1 if x['P214'] == i else 0, axis=1)  
        
        list_int = ['P404_1_4', 'P404_2_1', 'P404_1_2', 'P404_1_1', 'P404_2_4', 'P404_2_2', 'P404_1_3', 'P404_2_3']
        
        list_bin = ['P217', 'P328_1_1', 'P328_2_1', 'P328_1_2', 'P328_2_2', 'P329_1_1', 'P329_1_2', 'P329_1_3', 'P329_1_4', 
                    'P329_1_5', 'P329_1_6', 'P329_2_1', 'P329_2_2', 'P329_2_3', 'P329_2_4', 'P329_2_5', 'P329_2_6']

        df_merge[list_bin] = df_merge[list_bin].astype(str)
        df_merge[list_bin] = df_merge[list_bin].replace(LIST_DICT[2]).astype(int)
        df_merge[list_int] = df_merge[list_int].fillna(0)

        for i in range(1,6):
            df_merge['P213_aux_'+ str(i)] = df_merge.apply(lambda x: 1 if x['P213'] == i else 0, axis=1)
            df_merge['P328_1_1_aux_'+ str(i)] = df_merge.apply(lambda x: x['P328_1_1'] if x['P213'] == i else 0, axis=1)
            df_merge['P328_2_1_aux_'+ str(i)] = df_merge.apply(lambda x: x['P328_2_1'] if x['P213'] == i else 0, axis=1)
            df_merge['P328_1_2_aux_'+ str(i)] = df_merge.apply(lambda x: x['P328_1_2'] if x['P213'] == i else 0, axis=1)
            df_merge['P328_2_2_aux_'+ str(i)] = df_merge.apply(lambda x: x['P328_2_2'] if x['P213'] == i else 0, axis=1)
        
        df_merge['P329_aux_1'] = df_merge.apply(lambda x: 1 if ((x['P329_1_1']==1) | (x['P329_1_2']==1) | (x['P329_1_3']==1) | (x['P329_1_4']==1) | (x['P329_1_5']==1) | (x['P329_1_6']==1)) else 0, axis=1)
        df_merge['P329_aux_2'] = df_merge.apply(lambda x: 1 if ((x['P329_2_1']==1) | (x['P329_2_2']==1) | (x['P329_2_3']==1) | (x['P329_2_4']==1) | (x['P329_2_5']==1) | (x['P329_2_6']==1)) else 0, axis=1)
        
        df_merge.drop(columns = ['ID', 'research_center_id', 'UBIGCI'], inplace=True)

        #indicators
        df_final = pd.DataFrame()

        for i in ['district_id', 'department_id', 'province_id', 'nation_id']:
            list_geo = ['department_id', 'province_id', 'district_id', 'nation_id']
            list_geo.remove(i)

            df_indicator = df_merge.copy()
            df_indicator.drop(columns = list_geo, inplace=True)
            df_indicator = df_indicator.groupby([i]).sum().reset_index()

            df_indicator = INDICATOR(df_indicator, i) 

            df_final = df_final.append(df_indicator, sort=False)
        
        df_final[['district_id', 'department_id', 'province_id', 'nation_id']] = df_final[['district_id', 'department_id', 'province_id', 'nation_id']].fillna(0).astype(str)
        df_final['year'] = 2016
        print(df_final)
        return df_final

class CONCYTECPipeline(EasyPipeline):
    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open('../../conns.yaml'))

        transform_step = TransformStep()
    
        load_step = LoadStep('inei_concytec', db_connector, if_exists='drop', 
                             pk=['district_id', 'province_id', 'department_id',
                                 'nation_id', 'year'], dtype=DTYPE,
                             nullable_list=LIST_NULL)

        return [transform_step, load_step]

if __name__ == "__main__":
    pp = CONCYTECPipeline()
    pp.run({})