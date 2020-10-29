import re
import requests
import numpy as np
import pandas as pd
from functools import reduce 
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep
from bamboo_lib.steps import LoadStep
from pandas.io.json import json_normalize
from static import MONTHS_DICT, query, parameters
from shared_month import ReplaceStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        
        df_1 = pd.DataFrame()
        for year in [2018, 2019, 2020]: 
            df_1_query = query(parameters[3],year)

            for i in range(0, len(df_1_query)):
                mini_df_1_query = pd.io.json.json_normalize(df_1_query[df_1_query.columns[2]][i],'CANTIDAD')
                mini_df_1_query['department_id'] = df_1_query[df_1_query.columns[0]][i]
                mini_df_1_query['year'] = df_1_query[df_1_query.columns[3]][i]
                df_1 = df_1.append(mini_df_1_query)


        df_1 = pd.melt(df_1, id_vars=['year','MES','department_id'], value_vars=['NACIONAL', 'EXTRANJERO', 'TOTAL'])
        df_1['time'] = df_1['year'].astype(str) + df_1['MES'].map(MONTHS_DICT)
        df_1['indicator_id'] = parameters[3] + '_' + df_1['variable']
        df_1['nation_id'] = 'per'
        df_1['response_name'] = 'Visitas a complejos arqueológicos'
        df_1.rename(columns={'value': 'total'}, inplace=True)
        df_1 = df_1[['time','indicator_id','department_id','nation_id','total','response_name']]

        k = 1
        df_2 = {}
        for year in [2018,2019,2020]:
            df_2[k] = query(parameters[5],year)
            df_2[k] = pd.melt(df_2[k], id_vars=['MES','anio'], value_vars=['CERTIFICADO_EMITIDO', 'ARQUEOLOGICA_VERIFICADA',
               'HISTORICA_VERIFICADA', 'PALEONTOLOGICA_VERIFICADA', 'TOTAL', 'ARQUEOLOGICA_DENEGADA', 'HISTORICA_DENEGADA',
               'PALEONTOLOGICA_DENEGADA', 'PIEZA_DENEGADA'])
            df_2[k]['department_id'] = 0
            df_2[k]['indicator_id'] = parameters[5] + '_' + df_2[k]['variable']
            df_2[k]['nation_id'] = 'per'
            df_2[k]['response_name'] = 'Certificados de bienes no pertenecientes al patrimonio cultural con fines de exportación'

            k = k + 1

        df_2_list = [df_2[i] for i in range(1,3 + 1)]
        df_2 = reduce(lambda df_21,df_22: df_21.append(df_22), df_2_list)
        df_2.rename(columns={'value':'total'}, inplace=True)
        df_2['MES'] = df_2['MES'].str.capitalize()
        df_2['time'] = df_2['anio'].astype(str) + df_2['MES'].map(MONTHS_DICT)
        df_2 = df_2[['time','indicator_id','department_id','nation_id','total','response_name']]

        df_3 = pd.DataFrame()
        for year in [2018, 2019, 2020]: 
            df_3_query = query(parameters[6],year)

            for i in range(0, len(df_3_query)):
                mini_df_3_query = pd.io.json.json_normalize(df_3_query[df_3_query.columns[2]][i])
                mini_df_3_query['department_id'] = df_3_query[df_3_query.columns[0]][i]
                mini_df_3_query['year'] = df_3_query[df_3_query.columns[3]][i]
                df_3 = df_3.append(mini_df_3_query)


        df_3 = pd.melt(df_3, id_vars=['year','MES','department_id'], value_vars=['ARQUEOLOGICO', 'HISTORICO', 'PALEONTOLOGICO', 'TOTAL'])
        df_3['time'] = df_3['year'].astype(str) + df_3['MES'].map(MONTHS_DICT)
        df_3['indicator_id'] = parameters[6] + '_' + df_3['variable']
        df_3['nation_id'] = 'per'
        df_3['response_name'] = 'Alertas de atentados a nivel nacional'
        df_3.rename(columns={'value': 'total'}, inplace=True)
        df_3 = df_3[['time','indicator_id','department_id','nation_id','total','response_name']]

        k = 1
        df_4 = {}
        for year in [2018, 2019, 2020]: 
            df_4[k] = query(parameters[7],year)
            df_4[k] = pd.melt(df_4[k], id_vars=['anio','MES'], value_vars=['ARQUEOLOGICO', 'HISTORICO', 'PALEONTOLOGICO', 'TOTAL'])
            k = k + 1
        df_4_list = [df_4[i] for i in range(1,3 + 1)]
        df_4 = reduce(lambda df_41,df_42: df_41.append(df_42), df_4_list)
        df_4.rename(columns={'value':'total'}, inplace=True)
        df_4['indicator_id'] = parameters[7] + '_' + df_4['variable']
        df_4['department_id'] = 15
        df_4['nation_id'] = 'per'
        df_4['response_name'] = 'Alertas de atentados en Lima'
        df_4['MES'] = df_4['MES'].replace('Setiembre', 'Septiembre')
        df_4['time'] = df_4['anio'].astype(str) + df_4['MES'].map(MONTHS_DICT)
        df_4 = df_4[['time','indicator_id','department_id','nation_id','total','response_name']]

        df_5 = pd.DataFrame()
        for year in [2018, 2019, 2020]: 
            df_5_query = query(parameters[10],year)

            for i in range(0, len(df_5_query)):
                mini_df_5_query = pd.io.json.json_normalize(df_5_query[df_5_query.columns[2]][i], 'CANTIDAD')
                mini_df_5_query['department_id'] = df_5_query[df_5_query.columns[0]][i]
                mini_df_5_query['year'] = df_5_query[df_5_query.columns[3]][i]
                df_5 = df_5.append(mini_df_5_query)


        df_5 = pd.melt(df_5, id_vars=['year','MES','department_id'], value_vars=['ADULTO', 'ESTUDIANTE', 'MENORES', 'ADULTO_MAYOR','TOTAL'])
        df_5['time'] = df_5['year'].astype(str) + df_5['MES'].map(MONTHS_DICT)
        df_5['indicator_id'] = parameters[10] + '_' + df_5['variable']
        df_5['nation_id'] = 'per'
        df_5['response_name'] = 'Visitas a museos - MUA'
        df_5.rename(columns={'value': 'total'}, inplace=True)
        df_5 = df_5[['time','indicator_id','department_id','nation_id','total','response_name']]
        df_5.fillna(0, inplace = True)

        df_6 = pd.DataFrame()
        for year in [2018, 2019, 2020]: 
            df_6_query = query(parameters[13],year)

            for i in range(0, len(df_6_query)):
                mini_df_6_query = pd.io.json.json_normalize(df_6_query[df_6_query.columns[2]][i], 'CANTIDAD')
                mini_df_6_query['department_id'] = df_6_query[df_6_query.columns[0]][i]
                mini_df_6_query['year'] = df_6_query[df_6_query.columns[3]][i]
                df_6 = df_6.append(mini_df_6_query)


        df_6 = pd.melt(df_6, id_vars=['year','MES','department_id'], value_vars=['NACIONAL', 'EXTRANJERO', 'TOTAL'])
        df_6['time'] = df_6['year'].astype(str) + df_6['MES'].map(MONTHS_DICT)
        df_6['indicator_id'] = parameters[13] + '_' + df_6['variable']
        df_6['nation_id'] = 'per'
        df_6['response_name'] = 'Visitantes a museos según tipo de público'
        df_6.rename(columns={'value': 'total'}, inplace=True)
        df_6 = df_6[['time','indicator_id','department_id','nation_id','total','response_name']]
        
        df_7 = pd.DataFrame()
        for year in [2018, 2019, 2020]: 
            df_7_query = query(parameters[14],year)

            for i in range(0, len(df_7_query)):
                mini_df_7_query = pd.io.json.json_normalize(df_7_query[df_7_query.columns[2]][i], 'CANTIDAD')
                mini_df_7_query['department_id'] = df_7_query[df_7_query.columns[0]][i]
                mini_df_7_query['year'] = df_7_query[df_7_query.columns[3]][i]
                df_7 = df_7.append(mini_df_7_query)


        df_7 = pd.melt(df_7, id_vars=['year','MES','department_id'], value_vars=['NACIONAL', 'EXTRANJERO', 'TOTAL'])
        df_7['MES'] = df_7['MES'].replace({'Setiembre' : 'Septiembre'})
        df_7['time'] = df_7['year'].astype(str) + df_7['MES'].map(MONTHS_DICT)
        df_7['indicator_id'] = parameters[14] + '_' + df_7['variable']
        df_7['nation_id'] = 'per'
        df_7['response_name'] = 'Visitantes a salas según tipo de público'
        df_7.rename(columns={'value': 'total'}, inplace=True)
        df_7 = df_7[['time','indicator_id','department_id','nation_id','total','response_name']]

        df_8 = pd.DataFrame()
        for year in [2019]: 
            df_8_query = query(parameters[20],year)

            for i in range(0, len(df_8_query)):
                mini_df_8_query = pd.io.json.json_normalize(df_8_query[df_8_query.columns[2]][i]).rename(columns={'CANTIDAD' : 'TALLERES'})
                mini_df_8_query['BENEFICIARIOS'] = pd.io.json.json_normalize(df_8_query[df_8_query.columns[3]][i],'CANTIDAD').rename(columns={'CANTIDAD' : 'BENEFICIARIOS'})
                mini_df_8_query['department_id'] = df_8_query[df_8_query.columns[0]][i]
                mini_df_8_query['year'] = df_8_query[df_8_query.columns[5]][i]
                df_8 = df_8.append(mini_df_8_query)


        df_8 = pd.melt(df_8, id_vars=['year','IDMES','department_id'], value_vars=['TALLERES', 'BENEFICIARIOS'])
        df_8['time'] = df_8['year'].astype(str) + df_8['IDMES'].map(MONTHS_DICT)
        df_8['indicator_id'] = parameters[20] + '_' + df_8['variable']
        df_8['nation_id'] = 'per'
        df_8['response_name'] = 'Beneficiarios de talleres vinculados a las artes e industrias culturales'
        df_8.rename(columns={'value': 'total'}, inplace=True)
        df_8 = df_8[['time','indicator_id','department_id','nation_id','total','response_name']]

        df_list = df_list = [df_1, df_2, df_3, df_4, df_5, df_6, df_7, df_8]

        df = reduce(lambda df1,df2: df1.append(df2), df_list)
        df.rename(columns={'response_name':'response_id', 'time' : 'month_id'}, inplace=True)
        
       
        return df

class InfoCulturaPipeline(EasyPipeline):
    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open('../../../conns.yaml'))

        dtype = {
            'month_id':                'UInt32',
            'indicator_id':            'UInt16',
            'department_id':           'String',
            'nation_id':               'String',
            'total':                   'UInt16',
            'response_id':             'UInt16',
        }

        transform_step = TransformStep()
        replace_step = ReplaceStep()
        
        load_step = LoadStep('cultura_infocultura_month', db_connector, if_exists='drop', 
                            pk=['department_id'], dtype=dtype, nullable_list = ['total'])

        return [transform_step, replace_step]



if __name__ == "__main__":
    pp = InfoCulturaPipeline()
    pp.run({})