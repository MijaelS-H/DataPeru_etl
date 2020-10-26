import numpy as np
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep
from bamboo_lib.steps import LoadStep
from static import COLUMNS_RENAME, LIST_DICT, DTYPE, LIST_NULL, DICT_GROUPBY
from indicator import INDICATOR_MARKET, INDICATOR_GEO, INDICATOR_EXCEPTION

def convert_string(column):
    new_data = pd.DataFrame()
    new_data[['number', 'decimal']] = column.str.split(',', expand=True)
    new_data['decimal'] = new_data['decimal'].fillna('0')
    new_data['total'] = new_data['number'] + '.' + new_data['decimal']
    return new_data['total']

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        #read modules
        list_name = [
            '../../../datasets/20201001/02. Información Censos (01-10-2020)/03 CENSO NACIONAL DE MERCADOS DE ABASTO/03 MÓDULO 1118_ Características del Mercado/Capítulo_IV_NACIONAL.dta',
            '../../../datasets/20201001/02. Información Censos (01-10-2020)/03 CENSO NACIONAL DE MERCADOS DE ABASTO/04 MÓDULO 1119_ Infraestructura, Instalaciones, Equipamiento y Otros/Capítulo_V_NACIONAL.sav',
            '../../../datasets/20201001/02. Información Censos (01-10-2020)/03 CENSO NACIONAL DE MERCADOS DE ABASTO/05 MÓDULO 1120_ Gestión Administrativa y Financiera/Capítulo_VI_NACIONAL.sav',
            '../../../datasets/20201001/02. Información Censos (01-10-2020)/03 CENSO NACIONAL DE MERCADOS DE ABASTO/01 MÓDULO 1116_ Localización del Mercado/Capítulo_I_NACIONAL.sav',
            ]
        
        df = [pd.read_spss(x) if x != list_name[0] else pd.read_stata(x) for x in list_name]

        for i in range(len(df)):
            df[i].columns = df[i].columns.str.lower()
        
        #choose columns
        df[0] = df[0][['id', 'ccdd', 'ccpp', 'ccdi', 'p30_1', 'p30_2', 'p30_3', 'p30_4','p31_1', 'p31_2', 'p34a','p36_1', 'p36_2', 
                       'p36_3', 'p37', 'p38','p39_1', 'p39_2', 'p39_3', 'p39_4', 'p39_5', 'p39_6', 'p39_7', 'p39_8', 'p40_1', 'p40_2', 
                       'p40_3']]
        
        df[1] = df[1][['id', 'ccdd', 'ccpp', 'ccdi', 'p47_1', 'p47_2', 'p47_3', 'p49_5', 'p49_7', 'p49_8', 'p49b_1', 'p49c_1', 'p49c_2', 
                       'p49_9', 'p49_10', 'p49_13', 'p50', 'p51', 'p54_1', 'p54_2', 'p54_3']]
        
        df[2] = df[2][['id', 'ccdd', 'ccpp', 'ccdi', 'p55_1', 'p55_2', 'p55_3', 'p55_4', 'p56_1e_total', 'p56_2e_total', 'p56_3e_total', 
                       'p56_4e_total', 'p56_5e_total', 'p56_6e_total', 'p56_7e_total', 'p56a_1a', 'p56a_1b', 'p56a_1c', 'p56a_1d', 'p56a_2a', 
                       'p56a_2b', 'p56a_2c', 'p56a_2d', 'p56a_3a', 'p56a_3b', 'p56a_3c', 'p56a_3d', 'p56a_4a', 'p56a_4b', 'p56a_4c', 'p56a_4d', 
                       'p56a_5a', 'p56a_5b', 'p56a_5c', 'p56a_5d', 'p56a_6a', 'p56a_6b', 'p56a_6c', 'p56a_6d', 'p56a_7a', 'p56a_7b', 'p56a_7c', 
                       'p56a_7d', 'p61_1', 'p61_2', 'p61_3', 'p61a1_4', 'p61a2_4', 'p61a3_4', 'p62', 'p63_1', 'p63_2', 'p63_3', 'p63_4', 'p63_5', 
                       'p63_6', 'p63_8', 'p64_1a', 'p64_1b', 'p64_2a', 'p64_2b', 'p64_3a', 'p64_3b', 'p64_4a', 'p64_5a', 'p64_5b_total', 'p64_7a_total',
                       'p59a_1', 'p59a_2', 'p59a_3', 'p59a_4', 'p59a_5', 'p59a_6', 'p59a_7', 'p59a_9', 'p60_1', 'p60_2',  'p60_3', 'p60_4', 'p60_5', 'p60_6']]
        
        df[2]['count'] = 1

        df[3] = df[3][['id', 'gps_lon', 'gps_lat']]

        for col in ['gps_lon',  'gps_lat']:
            df[3][col] = convert_string(df[3][col]).astype(str)

        #merge modules
        df_merge = df[0].merge(df[1], on = ['id', 'ccdd', 'ccpp', 'ccdi'])
        df_merge = df_merge.merge(df[2], on = ['id', 'ccdd', 'ccpp', 'ccdi'])

        df_merge.rename(columns = COLUMNS_RENAME, inplace=True)

        #change column type        
        list_int = ['p36_1', 'p36_2', 'p36_3', 'p37', 'p38','p39_1', 'p39_2', 'p39_3', 'p39_4', 'p39_5', 'p39_6', 'p49b_1', 'p56_1e_total', 
                    'p56_2e_total', 'p56_3e_total', 'p56_4e_total', 'p56_5e_total', 'p56_6e_total', 'p56_7e_total', 'p56a_1b', 'p56a_1d', 
                    'p56a_2b', 'p56a_2d', 'p56a_3b', 'p56a_3d', 'p56a_4b', 'p56a_4d', 'p56a_5b', 'p56a_5d', 'p56a_6b', 'p56a_6d', 'p56a_7b', 
                    'p56a_7d', 'p56a_1a', 'p56a_1c', 'p56a_2a', 'p56a_2c', 'p56a_3a', 'p56a_3c', 'p56a_4a', 'p56a_4c', 'p56a_5a', 'p56a_5c', 
                    'p56a_6a', 'p56a_6c', 'p56a_7a', 'p56a_7c', 'p40_1', 'p40_2', 'p40_3', 'p50']
            
        df_merge[list_int] = df_merge[list_int].fillna(0)
        df_merge[list_int] = df_merge[list_int].replace('nan', 0)
        df_merge[list_int] = df_merge[list_int].astype(int)
        
        list_bin = ['p47_1', 'p47_2', 'p47_3', 'p55_1', 'p55_2', 'p55_3', 'p55_4', 'p49_7', 'p49_8', 'p49_9','p49_10', 'p49_13', 'p49c_1', 'p49c_2', 'p61_1', 'p61_2', 
                    'p61_3', 'p62', 'p30_1', 'p30_2', 'p30_3', 'p31_1', 'p31_2', 'p30_4', 'p49_5', 'p54_1', 'p54_2', 'p54_3', 'p59a_1', 'p59a_2', 'p59a_3', 'p59a_4', 
                    'p59a_5', 'p59a_6', 'p59a_7', 'p59a_9']

        list_num = ['p63_1', 'p63_2', 'p63_3', 'p63_4', 'p63_5', 'p63_6', 'p63_8', 'p34a', 'p60_1', 'p60_2', 'p60_3', 'p60_4', 'p60_5', 'p60_6']
        
        list_frequency = ['p51']

        list_all = [list_bin, list_num, list_frequency]

        for i in range(len(list_all)):
            df_merge[list_all[i]] = df_merge[list_all[i]].astype(str)
            df_merge[list_all[i]] = df_merge[list_all[i]].replace(LIST_DICT[i])
            df_merge[list_all[i]] = df_merge[list_all[i]].astype(int)
        
        #indicators
        df_merge['nation_id'] = 'per'
        df_merge['province_id'] = df_merge['department_id'] + df_merge['province_id']
        df_merge['district_id'] = df_merge['province_id'] + df_merge['district_id']

        df_merge['indicator_formality'] = pd.cut(df_merge['p30_1'] + df_merge['p30_2'] + df_merge['p30_3'] + df_merge['p31_1'] + df_merge['p31_2'] + df_merge['p34a'], [0, 1, 3, 5, np.inf], labels=[0,1,2,3], right=False).astype(int)
        
        for i in range(4):
            df_merge['formality_'+ str(i)] = df_merge.apply(lambda x: 1 if x['indicator_formality'] == i else 0, axis=1)
        
        df_final = pd.DataFrame()
        for i in ['district_id', 'province_id', 'department_id', 'nation_id', 'market_id']:
            list_geo = ['department_id', 'province_id', 'district_id', 'nation_id', 'market_id']
            list_geo.remove(i)
    
            df_indicator = df_merge.copy()
            df_indicator.drop(columns = list_geo, inplace=True)
            df_indicator_aux = df_indicator.copy()
            df_indicator = df_indicator.groupby([i]).agg(DICT_GROUPBY).reset_index()
    
            df_indicator = INDICATOR_GEO(df_indicator, i) if i != 'market_id' else INDICATOR_MARKET(df_indicator, i)
            
            if i != 'market_id':
                df_exception = INDICATOR_EXCEPTION(df_indicator_aux, i)
                df_indicator = df_indicator.merge(df_exception, on=i, how='left')
            
            df_final = df_final.append(df_indicator, sort=False)
        
        df_final['year'] = 2016
        df_final[['district_id', 'province_id', 'department_id', 'nation_id', 'market_id']] = df_final[['district_id', 'province_id', 'department_id', 'nation_id', 'market_id']].fillna(0).astype(str)

        #add latitude and longitude 
        df_final = df_final.merge(df[3], left_on='market_id', right_on ='id', how='left')
        df_final.drop(columns=['id'], inplace=True)
    
        return df_final

class CENAMAPipeline(EasyPipeline):
    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        transform_step = TransformStep()
    
        load_step = LoadStep('inei_cenama', db_connector, if_exists='drop', 
                             pk=['district_id', 'province_id', 'department_id',
                                 'nation_id', 'market_id', 'year'], dtype=DTYPE,
                             nullable_list=LIST_NULL)

        return [transform_step, load_step]

if __name__ == "__main__":
    pp = CENAMAPipeline()
    pp.run({})