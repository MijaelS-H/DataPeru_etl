import re
import requests
import numpy as np
import pandas as pd
from functools import reduce 
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep
from bamboo_lib.steps import LoadStep
#from pandas.io.json import json_normalize
from .static import MONTHS_DICT, query, parameters
from etl.socios.cultura.infocultura.shared_month import ReplaceStep

from etl.consistency import AggregatorStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        
        df_1 = pd.DataFrame()
        for year in [2018, 2019, 2020]: 
            df_1_query = query(parameters[3],year)

            for i in range(0, len(df_1_query)):
                mini_df_1_query = pd.json_normalize(df_1_query[df_1_query.columns[2]][i],'CANTIDAD', 'NOMBRE')
                mini_df_1_query['department_id'] = df_1_query[df_1_query.columns[0]][i]
                mini_df_1_query['year'] = df_1_query[df_1_query.columns[3]][i]
                df_1 = df_1.append(mini_df_1_query)


        df_1 = pd.melt(df_1, id_vars=['year','MES','department_id', 'NOMBRE'], value_vars=['NACIONAL', 'EXTRANJERO', 'TOTAL'])
        df_1['time'] = df_1['year'].astype(str) + df_1['MES'].map(MONTHS_DICT)
        df_1['indicator_id'] = 'Visitas a complejos arqueológicos'
        df_1['nation_id'] = 0
        df_1.rename(columns={'value': 'response', 'variable' : 'category_id', 'NOMBRE' : 'subcategory_id'}, inplace=True)
        df_1 = df_1[['time', 'indicator_id', 'category_id', 'subcategory_id', 'department_id','nation_id','response']]

        k = 1
        df_2 = {}
        for year in [2018,2019,2020]:
            df_2[k] = query(parameters[5],year)
            df_2[k] = pd.melt(df_2[k], id_vars=['MES','anio'], value_vars=['CERTIFICADO_EMITIDO', 'ARQUEOLOGICA_VERIFICADA',
               'HISTORICA_VERIFICADA', 'PALEONTOLOGICA_VERIFICADA', 'TOTAL', 'ARQUEOLOGICA_DENEGADA', 'HISTORICA_DENEGADA',
               'PALEONTOLOGICA_DENEGADA', 'PIEZA_DENEGADA'])
            df_2[k]['department_id'] = 0
            df_2[k]['subcategory_id'] = np.nan
            df_2[k]['indicator_id'] = 'Certificados de bienes no pertenecientes al patrimonio cultural con fines de exportación'
            df_2[k]['nation_id'] = 'per'
            k = k + 1

        df_2_list = [df_2[i] for i in range(1,3 + 1)]
        df_2 = reduce(lambda df_21,df_22: df_21.append(df_22), df_2_list)
        df_2.rename(columns={'value':'response', 'variable' : 'category_id'}, inplace=True)
        df_2['MES'] = df_2['MES'].str.capitalize()
        df_2['time'] = df_2['anio'].astype(str) + df_2['MES'].map(MONTHS_DICT)
        df_2 = df_2[['time', 'indicator_id', 'category_id', 'subcategory_id', 'department_id','nation_id','response']]

        df_3 = pd.DataFrame()
        for year in [2018, 2019, 2020]: 
            df_3_query = query(parameters[6],year)

            for i in range(0, len(df_3_query)):
                mini_df_3_query = pd.json_normalize(df_3_query[df_3_query.columns[2]][i])
                mini_df_3_query['department_id'] = df_3_query[df_3_query.columns[0]][i]
                mini_df_3_query['year'] = df_3_query[df_3_query.columns[3]][i]
                df_3 = df_3.append(mini_df_3_query)


        df_3 = pd.melt(df_3, id_vars=['year','MES','department_id'], value_vars=['ARQUEOLOGICO', 'HISTORICO', 'PALEONTOLOGICO', 'TOTAL'])
        df_3['time'] = df_3['year'].astype(str) + df_3['MES'].map(MONTHS_DICT)
        df_3['indicator_id'] = 'Alertas de atentados a nivel nacional'
        df_3['nation_id'] = 0
        df_3['subcategory_id'] = np.nan
        df_3.rename(columns={'value':'response', 'variable' : 'category_id'}, inplace=True)
        df_3 = df_3[['time', 'indicator_id', 'category_id', 'subcategory_id', 'department_id','nation_id','response']]

        k = 1
        df_4 = {}
        for year in [2018, 2019, 2020]: 
            df_4[k] = query(parameters[7],year)
            df_4[k] = pd.melt(df_4[k], id_vars=['anio','MES'], value_vars=['ARQUEOLOGICO', 'HISTORICO', 'PALEONTOLOGICO', 'TOTAL'])
            k = k + 1
        df_4_list = [df_4[i] for i in range(1,3 + 1)]
        df_4 = reduce(lambda df_41,df_42: df_41.append(df_42), df_4_list)
        df_4.rename(columns={'value':'response', 'variable' : 'category_id'}, inplace=True)
        df_4['indicator_id'] = 'Alertas de atentados en Lima'
        df_4['department_id'] = 15
        df_4['nation_id'] = 0
        df_4['subcategory_id'] = np.nan
        df_4['MES'] = df_4['MES'].replace('Setiembre', 'Septiembre')
        df_4['time'] = df_4['anio'].astype(str) + df_4['MES'].map(MONTHS_DICT)
        df_4 = df_4[['time', 'indicator_id', 'category_id', 'subcategory_id', 'department_id','nation_id','response']]

        df_5 = pd.DataFrame()
        for year in [2018, 2019, 2020]: 
            df_5_query = query(parameters[10],year)

            for i in range(0, len(df_5_query)):
                mini_df_5_query = pd.json_normalize(df_5_query[df_5_query.columns[2]][i], 'CANTIDAD', 'NOMBRE')
                mini_df_5_query['department_id'] = df_5_query[df_5_query.columns[0]][i]
                mini_df_5_query['year'] = df_5_query[df_5_query.columns[3]][i]
                df_5 = df_5.append(mini_df_5_query)


        df_5 = pd.melt(df_5, id_vars=['year','MES','department_id', 'NOMBRE'], value_vars=['ADULTO', 'ESTUDIANTE', 'MENORES', 'ADULTO_MAYOR','TOTAL'])
        df_5['time'] = df_5['year'].astype(str) + df_5['MES'].map(MONTHS_DICT)
        df_5['indicator_id'] = 'Visitas a museos - MUA'
        df_5['nation_id'] = 'per'
        df_5.rename(columns={'value':'response', 'variable' : 'category_id', 'NOMBRE' : 'subcategory_id'}, inplace=True)
        df_5 = df_5[['time', 'indicator_id', 'category_id', 'subcategory_id', 'department_id','nation_id','response']]
        df_5['response'].fillna(0, inplace = True)

        df_6 = pd.DataFrame()
        for year in [2018, 2019, 2020]: 
            df_6_query = query(parameters[13],year)

            for i in range(0, len(df_6_query)):
                mini_df_6_query = pd.json_normalize(df_6_query[df_6_query.columns[2]][i], 'CANTIDAD', 'NOMBRE')
                mini_df_6_query['department_id'] = df_6_query[df_6_query.columns[0]][i]
                mini_df_6_query['year'] = df_6_query[df_6_query.columns[3]][i]
                df_6 = df_6.append(mini_df_6_query)


        df_6 = pd.melt(df_6, id_vars=['year','MES','department_id', 'NOMBRE'], value_vars=['NACIONAL', 'EXTRANJERO', 'TOTAL'])
        df_6['time'] = df_6['year'].astype(str) + df_6['MES'].map(MONTHS_DICT)
        df_6['indicator_id'] = 'Visitas a museos'
        df_6['nation_id'] = 0
        df_6.rename(columns={'value':'response', 'variable' : 'category_id', 'NOMBRE' : 'subcategory_id'}, inplace=True)
        df_6 = df_6[['time', 'indicator_id', 'category_id', 'subcategory_id', 'department_id','nation_id','response']]

        df_7 = pd.DataFrame()
        for year in [2018, 2019, 2020]: 
            df_7_query = query(parameters[14],year)

            for i in range(0, len(df_7_query)):
                mini_df_7_query = pd.json_normalize(df_7_query[df_7_query.columns[2]][i], 'CANTIDAD', 'NOMBRE')
                mini_df_7_query['department_id'] = df_7_query[df_7_query.columns[0]][i]
                mini_df_7_query['year'] = df_7_query[df_7_query.columns[3]][i]
                df_7 = df_7.append(mini_df_7_query)


        df_7 = pd.melt(df_7, id_vars=['year','MES','department_id', 'NOMBRE'], value_vars=['NACIONAL', 'EXTRANJERO', 'TOTAL'])
        df_7['MES'] = df_7['MES'].replace({'Setiembre' : 'Septiembre'})
        df_7['time'] = df_7['year'].astype(str) + df_7['MES'].map(MONTHS_DICT)
        df_7['indicator_id'] = 'Visitas a salas de exposición'
        df_7['nation_id'] = 0
        df_7.rename(columns={'value':'response', 'variable' : 'category_id', 'NOMBRE' : 'subcategory_id'}, inplace=True)
        df_7 = df_7[['time', 'indicator_id', 'category_id', 'subcategory_id', 'department_id','nation_id','response']]

        df_8 = pd.DataFrame()
        for year in [2018, 2019, 2020]: 
            df_query = query(parameters[19],year)

            for i in range(0, len(df_query)):
                mini_df_query = pd.json_normalize(df_query[df_query.columns[2]][i])
                mini_df_query['month'] = df_query[df_query.columns[0]][i]
                mini_df_query['year'] = df_query[df_query.columns[3]][i]
                df_8 = df_8.append(mini_df_query)

        df_8 = pd.melt(df_8, id_vars=['month','year','ELENCOS'], value_vars=['PRESENTACIONES','ASISTENTES'])
        df_8.rename(columns={'ELENCOS': 'subcategory_id','value': 'response', 'variable' : 'category_id'}, inplace=True)
        df_8['time'] = df_8['year'].astype(str) + df_8['month'].astype(str)
        df_8['department_id'] = 0
        df_8['nation_id'] = 'per'
        df_8['indicator_id'] = "Presentaciones y asistencias a los espectáculo culturales mensuales"
        
        df_8 = df_8[['time', 'indicator_id', 'category_id', 'subcategory_id', 'department_id','nation_id','response']]

        df_9 = pd.DataFrame()
        for year in [2019]: 
            df_9_query = query(parameters[20],year)

            for i in range(0, len(df_9_query)):
                mini_df_9_query = pd.json_normalize(df_9_query[df_9_query.columns[2]][i]).rename(columns={'CANTIDAD' : 'TALLERES'})
                mini_df_9_query['BENEFICIARIOS'] = pd.json_normalize(df_9_query[df_9_query.columns[3]][i])['CANTIDAD']
                mini_df_9_query['department_id'] = df_9_query[df_9_query.columns[0]][i]
                mini_df_9_query['year'] = df_9_query[df_9_query.columns[5]][i]
                df_9 = df_9.append(mini_df_9_query)


        df_9 = pd.melt(df_9, id_vars=['year','IDMES','department_id'], value_vars=['TALLERES', 'BENEFICIARIOS'])
        df_9['time'] = df_9['year'].astype(str) + df_9['IDMES'].map(MONTHS_DICT)
        df_9['indicator_id'] = 'Beneficiarios de talleres vinculados a las artes e industrias culturales'
        df_9['nation_id'] = 'per'
        df_9['subcategory_id'] = np.nan
        df_9.rename(columns={'value':'response', 'variable' : 'category_id'}, inplace=True)
        df_9 = df_9[['time', 'indicator_id', 'category_id', 'subcategory_id', 'department_id','nation_id','response']]   

        df_list = [df_1, df_2, df_3, df_4, df_5, df_6, df_7, df_8, df_9]

        df = reduce(lambda df1,df2: df1.append(df2), df_list)
        df.rename(columns={'response_name':'response_id', 'time' : 'month_id'}, inplace=True)
        
        df['department_id'] = df['department_id'].astype(str)
        df['category_id'] = df['category_id'].str.replace('_', ' ').str.title()
        df['subcategory_id'] = df['subcategory_id'].str.strip()
        df['category_id'] = df['category_id'].str.strip()
        
        return df

class FormatStep(PipelineStep):
    def run_step(self, prev, params):

        df = prev[0]
        df[['month_id', 'indicator_id', 'category_id', 'subcategory_id']] = df[['month_id', 'indicator_id', 'category_id', 'subcategory_id']].fillna(0).astype(int)  
        df['response'] = df['response'].astype(float)
        df['nation_id'] = df['nation_id'].astype(str)
        return df

class InfoCulturaMonthPipeline(EasyPipeline):
    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open(params['connector']))

        dtype = {
            'month_id':                'UInt32',
            'indicator_id':            'UInt8',
            'category_id':             'UInt8',
            'subcategory_id':          'UInt8',
            'department_id':           'String',
            'nation_id':               'String',
            'response':                'Float32',
        }

        transform_step = TransformStep()
        replace_step = ReplaceStep()
        format_step = FormatStep()

        agg_step = AggregatorStep('cultura_infocultura_month', measures=["response"])
        
        load_step = LoadStep('cultura_infocultura_month', db_connector, if_exists='drop', 
                            pk=['department_id'], dtype=dtype, nullable_list = ['subcategory_id', 'response'])

        return [transform_step, replace_step, format_step, load_step]


def run_pipeline(params: dict):
    pp = InfoCulturaMonthPipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path
    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline({
        "connector": path.join(__dirname, "..", "..", "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })