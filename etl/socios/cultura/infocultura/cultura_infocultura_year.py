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
from shared_year import ReplaceStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        
        k = 1
        df_1 = {}
        for year in [2018,2019,2020]:
            df_1[k] = query(parameters[0],year)
            df_1[k]['indicator_id'] = "Expresiones declaradas"
            df_1[k]['year'] = year
            df_1[k]['nation_id'] = 'per'
            df_1[k]['Nombres'] = df_1[k]['EXPRESION'].apply(lambda x: np.nan if isinstance(x, float) else [d['NOMBRE'] for d in x])
            df_1[k]['Nombres'] = df_1[k]['Nombres'].apply(lambda x: ", ".join( repr(e) for e in x ).replace("'","") if isinstance(x, list) else x)
            df_1[k]['category_id'] = "Total"
            df_1[k].rename(columns={'CODDEP': 'department_id', 'TOTAL':'response','Nombres':'subcategory_id'}, inplace=True)
            df_1[k] = df_1[k][['year','indicator_id','category_id','subcategory_id','department_id','nation_id', 'response']]
            k = k + 1

        df_1_list = [df_1[i] for i in range(1,3 + 1)]
        df_1 = reduce(lambda df_11,df_12: df_11.append(df_12), df_1_list)
    
        k = 1
        df_2 = {}
        for year in [2018,2019,2020]:
            df_2[k] = query(parameters[1],year)
            df_2[k] = df_2[k].reset_index()[:-1]

            if year == 2018:    
                df_2[k]['22'] = 0
                df_2[k]['24'] = 0
                df_2[k]['25'] = 0

            elif year == 2019:
                df_2[k]['17'] = 0
                df_2[k]['22'] = 0

            elif year == 2020:
                df_2[k]['17'] = 0
                df_2[k]['22'] = 0
                df_2[k]['24'] = 0
                df_2[k]['25'] = 0

            df_2[k]  = pd.melt(df_2[k], id_vars=['index','anio'], value_vars=['01', '02', '03', '04','05', '06', '07', '08', '09', '10', '11',
                       '12','13','14','15','16','17', '18', '19', '20', '21', '22', '23', '24', '25', '26'])
            df_2[k].rename(columns={'anio': 'year','variable':'department_id','value':'response','index':'subcategory_id'}, inplace= True)
            df_2[k]['indicator_id'] = 'Actividades de preservación'
            df_2[k]['nation_id'] = 'per'
            df_2[k]['category_id'] = 'Total'
            df_2[k] = df_2[k][['year','indicator_id','category_id','subcategory_id','department_id','nation_id','response']]
            k = k + 1

        df_2_list = [df_2[i] for i in range(1,3 + 1)]
        df_2 = reduce(lambda df_21,df_22: df_21.append(df_22), df_2_list)

        k = 1
        df_3 = {}
        for year in [2018, 2019, 2020]:
            df_3[k] = query(parameters[2],year)
            df_3[k] = pd.melt(df_3[k], id_vars=['NOMBRE', 'anio'], value_vars=['TOTAL_NACIONAL','TOTAL_EXTRANJERO','TOTAL'])
            df_3[k]['indicator_id'] = 'Sitios arqueológicos más visitados'
            df_3[k]['department_id'] = 0
            df_3[k]['nation_id'] = 'per'
            df_3[k].rename(columns={'anio': 'year', 'NOMBRE' : 'subcategory_id', 'value' : 'response', 'variable' : 'category_id'}, inplace=True)
            df_3[k] = df_3[k][['year', 'indicator_id', 'category_id', 'subcategory_id', 'department_id','nation_id','response']]
            k = k + 1

        df_3_list = [df_3[i] for i in range(1, 3 + 1)]
        df_3 = reduce(lambda df_31,df_32: df_31.append(df_32), df_3_list)

        k = 1
        df_4 = {}
        for year in [2018,2019,2020]:
            df_4[k] = query(parameters[4],year)
            df_4[k] = pd.melt(df_4[k], id_vars=['anio'], value_vars=['TOTAL_NINOS_NAC', 'TOTAL_NINOS_EXT', 'TOTAL_ESTUDIANTES_NAC',
               'TOTAL_ESTUDIANTES_EXT', 'TOTAL_ADULTOS_NAC', 'TOTAL_ADULTOS_EXT',
               'TOTAL_ADU_BOLESP_NAC', 'TOTAL_ADU_BOLESP_EXT', 'TOTAL_EST_BOLESP_NAC',
               'TOTAL_EST_BOLESP_EXT', 'TOTAL_NIN_BOLESP_NAC', 'TOTAL_NIN_BOLESP_EXT',
               'TOTAL_MIL_BOLESP_NAC', 'TOTAL_MIL_BOLESP_EXT', 'TOTAL_ADM_BOLESP_NAC',
               'TOTAL_ADM_BOLESP_EXT'])
            df_4[k]['indicator_id'] = "Visitas según tipo de público"
            df_4[k]['department_id'] = 0
            df_4[k]['subcategory_id'] = np.nan
            df_4[k]['nation_id'] = 'per'
            df_4[k].rename(columns={'anio': 'year', 'NOMBRE' : 'subcategory_id', 'value' : 'response', 'variable' : 'category_id'}, inplace=True)
            df_4[k] = df_4[k][['year', 'indicator_id', 'category_id', 'subcategory_id', 'department_id','nation_id','response']]
            k = k + 1

        df_4_list = [df_4[i] for i in range(1,3 + 1)]
        df_4 = reduce(lambda df_41,df_42: df_41.append(df_42), df_4_list)
    
        k = 1
        df_5 = {}
        for year in [2018,2019,2020]:
            df_5[k] = query(parameters[8],year)
            df_5[k] = pd.melt(df_5[k], id_vars=['NOMBRE','anio'], value_vars=['TOTAL_NACIONAL', 'TOTAL_EXTRANJERO', 'TOTAL'])
            df_5[k]['indicator_id'] = 'Museos más visitados'
            df_5[k]['department_id'] = 0
            df_5[k]['nation_id'] = 'per'
            df_5[k].rename(columns={'NOMBRE': 'subcategory_id','anio': 'year', 'value': 'response', 'variable' : 'category_id'}, inplace=True)
            df_5[k] = df_5[k][['year', 'indicator_id', 'category_id', 'subcategory_id', 'department_id','nation_id','response']]
            k = k + 1

        df_5_list = [df_5[i] for i in range(1,3 + 1)]
        df_5 = reduce(lambda df_51,df_52: df_51.append(df_52), df_5_list)

        k = 1
        df_6 = {}
        for year in [2018,2019,2020]:
            df_6[k] = query(parameters[9],year)
            df_6[k] = pd.melt(df_6[k], id_vars=['NOMBRE','anio'], value_vars=['TOTAL_NACIONAL', 'TOTAL_EXTRANJERO', 'TOTAL'])
            df_6[k]['indicator_id'] = 'Salas de exposición más visitados'
            df_6[k]['department_id'] = 0
            df_6[k]['nation_id'] = 'per'
            df_6[k].rename(columns={'NOMBRE': 'subcategory_id','anio': 'year', 'value': 'response', 'variable' : 'category_id'}, inplace=True)
            df_6[k] = df_6[k][['year', 'indicator_id', 'category_id', 'subcategory_id', 'department_id','nation_id','response']]
            k = k + 1

        df_6_list = [df_6[i] for i in range(1,3 + 1)]
        df_6 = reduce(lambda df_61,df_62: df_61.append(df_62), df_6_list)

        k = 1
        df_7 = {}
        for year in [2018,2019,2020]:
            df_7[k] = query(parameters[11],year)
            df_7[k] = pd.melt(df_7[k], id_vars=['anio'], value_vars=['TOTAL_NINOS_NAC', 'TOTAL_NINOS_EXT', 'TOTAL_ESTUDIANTES_NAC',
               'TOTAL_ESTUDIANTES_EXT', 'TOTAL_ADULTOS_NAC', 'TOTAL_ADULTOS_EXT',
               'TOTAL_ADU_BOLESP_NAC', 'TOTAL_ADU_BOLESP_EXT', 'TOTAL_EST_BOLESP_NAC',
               'TOTAL_EST_BOLESP_EXT', 'TOTAL_NIN_BOLESP_NAC', 'TOTAL_NIN_BOLESP_EXT',
               'TOTAL_MIL_BOLESP_NAC', 'TOTAL_MIL_BOLESP_EXT', 'TOTAL_ADM_BOLESP_NAC',
               'TOTAL_ADM_BOLESP_EXT'])
            df_7[k]['indicator_id'] = 'Visitantes a museos según tipo de público'
            df_7[k]['department_id'] = 0
            df_7[k]['nation_id'] = 'per'
            df_7[k]['subcategory_id'] = np.nan
            df_7[k].rename(columns={'anio': 'year', 'value': 'response', 'variable' : 'category_id'}, inplace=True)
            df_7[k] = df_7[k][['year', 'indicator_id', 'category_id', 'subcategory_id', 'department_id','nation_id','response']]
            k = k + 1

        df_7_list = [df_7[i] for i in range(1,3 + 1)]
        df_7 = reduce(lambda df_71,df_72: df_71.append(df_72), df_7_list)

        k = 1
        df_8 = {}
        for year in [2018,2019,2020]:
            df_8[k] = query(parameters[12],year)
            df_8[k] = pd.melt(df_8[k], id_vars=['anio'], value_vars=['TOTAL_NINOS_NAC', 'TOTAL_NINOS_EXT', 'TOTAL_ESTUDIANTES_NAC',
               'TOTAL_ESTUDIANTES_EXT', 'TOTAL_ADULTOS_NAC', 'TOTAL_ADULTOS_EXT',
               'TOTAL_ADU_BOLESP_NAC', 'TOTAL_ADU_BOLESP_EXT', 'TOTAL_EST_BOLESP_NAC',
               'TOTAL_EST_BOLESP_EXT', 'TOTAL_NIN_BOLESP_NAC', 'TOTAL_NIN_BOLESP_EXT',
               'TOTAL_MIL_BOLESP_NAC', 'TOTAL_MIL_BOLESP_EXT', 'TOTAL_ADM_BOLESP_NAC',
               'TOTAL_ADM_BOLESP_EXT'])
            df_8[k]['indicator_id'] = 'Visitantes a salas según tipo de público'
            df_8[k]['department_id'] = 0
            df_8[k]['nation_id'] = 'per'
            df_8[k]['subcategory_id'] =  np.nan
            df_8[k].rename(columns={'anio': 'year', 'value': 'response', 'variable' : 'category_id'}, inplace=True)
            df_8[k] = df_8[k][['year', 'indicator_id', 'category_id', 'subcategory_id', 'department_id','nation_id','response']]
            k = k + 1

        df_8_list = [df_8[i] for i in range(1,3 + 1)]
        df_8 = reduce(lambda df_81,df_82: df_81.append(df_82), df_8_list)

        df_9 = pd.DataFrame()

        for year in [2018, 2019, 2020]: 
            df_query = query(parameters[15],year)
            df_query = df_query.dropna().reset_index()
            df_query.drop('index', axis = 1, inplace=True)

            for i in range(0, len(df_query)):
                mini_df_query = pd.io.json.json_normalize(df_query[df_query.columns[2]][i])
                mini_df_query.drop(['TIPO', 'DISCIPLINA','ACTIVIDAD'], axis=1, inplace=True)
                mini_df_query['department_id'] = df_query[df_query.columns[0]][i]
                mini_df_query['year'] = df_query[df_query.columns[3]][i]
                df_9 = df_9.append(mini_df_query)

        df_9['response'] = 1
        df_9['category_id'] = 'Total'
        df_9.rename(columns={'NOMBRE': 'subcategory_id','value': 'total'}, inplace=True)
        df_9['indicator_id'] = 'Puntos de cultura reconocidos' 
        df_9['nation_id'] = 'per'

        df_9 = df_9[['year', 'indicator_id', 'category_id', 'subcategory_id', 'department_id','nation_id','response']]

        k = 1
        df_10 = {}
        for year in [2018,2019,2020]:
            df_10[k] = query(parameters[16],year)  
            df_10[k]['indicator_id'] = 'Asistencia a las presentaciones de los elencos nacionales'
            df_10[k]['nation_id'] = 'per'
            df_10[k].rename(columns={'coddpto': 'department_id','anio': 'year', 'asist':'response'}, inplace=True)
            df_10[k]['category_id'] = 'Total'
            df_10[k]['subcategory_id'] = np.nan
            df_10[k] = df_10[k][['year', 'indicator_id', 'category_id', 'subcategory_id', 'department_id','nation_id','response']]
            k = k + 1
        df_10_list = [df_10[i] for i in range(1,3 + 1)]
        df_10 = reduce(lambda df_101,df_102: df_101.append(df_102), df_10_list)

        k = 1
        df_11 = {}
        for year in [2018,2019,2020]:
            df_11[k] = query(parameters[17],year)
            df_11[k] = pd.melt(df_11[k], id_vars=['ELENCOS','anio'], value_vars=['PRESENTACIONES', 'ASISTENTES'])
            df_11[k]['indicator_id'] = 'Porcentaje de las presentaciones y asistencias a los espectáculos culturales'
            df_11[k]['department_id'] = 0
            df_11[k]['nation_id'] = 'per'
            df_11[k].rename(columns={'ELENCOS': 'subcategory_id', 'anio': 'year', 'value':'response', 'variable' : 'category_id'}, inplace=True)
            df_11[k] = df_11[k][['year', 'indicator_id', 'category_id', 'subcategory_id', 'department_id','nation_id','response']]
            k = k + 1

        df_11_list = [df_11[i] for i in range(1,3 + 1)]
        df_11 = reduce(lambda df_111,df_112: df_111.append(df_112), df_11_list)

        k = 1
        df_12 = {}
        for year in [2016, 2017, 2018, 2019, 2020]:
            df_12[k] = query(parameters[21],year)
            df_12[k] = pd.melt(df_12[k], id_vars=['PUEBLO', 'anio'], value_vars=['HOMBRES', 'MUJERES', 'TOTAL'])
            df_12[k]['indicator_id'] = 'Pueblo Indígena'
            df_12[k]['department_id'] = 0
            df_12[k]['nation_id'] = 'per'
            df_12[k].rename(columns={'anio':'year', 'value': 'response', 'variable' :'category_id', 'PUEBLO' : 'subcategory_id'}, inplace=True)
            df_12[k] = df_12[k][['year', 'indicator_id', 'category_id', 'subcategory_id', 'department_id','nation_id','response']]
            k = k + 1

        df_12_list = [df_12[i] for i in range(1,5 + 1)]
        df_12 = reduce(lambda df_121,df_122: df_121.append(df_122), df_12_list)

        k = 1
        df_13 = {}
        for year in [2016, 2017, 2018, 2019, 2020]:
            df_13[k] = query(parameters[22],year)
            df_13[k] = pd.melt(df_13[k], id_vars=['PUEBLO', 'anio'], value_vars=['LOCALIDADES'])
            df_13[k]['indicator_id'] = 'Localidad por pueblo indígena'
            df_13[k]['department_id'] = 0
            df_13[k]['nation_id'] = 'per'
            df_13[k].rename(columns={'anio':'year', 'value': 'response', 'variable' :'category_id', 'PUEBLO' : 'subcategory_id'}, inplace=True)
            df_13[k] = df_13[k][['year', 'indicator_id', 'category_id', 'subcategory_id', 'department_id','nation_id','response']]
            k = k + 1

        df_13_list = [df_13[i] for i in range(1,5 + 1)]
        df_13 = reduce(lambda df_131,df_132: df_131.append(df_132), df_13_list)

        k = 1
        df_14 = {}
        for year in [2016, 2017, 2018, 2019, 2020]:
            df_14[k] = query(parameters[23],year)
            df_14[k] = pd.melt(df_14[k], id_vars=['CODDEP', 'anio'], value_vars=['LOCALIDADES'])
            df_14[k]['indicator_id'] = 'Localidades por departamento'
            df_14[k]['nation_id'] = 'per'
            df_14[k]['subcategory_id'] = np.nan
            df_14[k].rename(columns={'anio':'year', 'CODDEP' : 'department_id', 'value': 'response', 'variable' :'category_id', 'PUEBLO' : 'subcategory_id'}, inplace=True)

            df_14[k] = df_14[k][['year', 'indicator_id', 'category_id', 'subcategory_id', 'department_id','nation_id','response']]
            k = k + 1
        
        df_14_list = [df_14[i] for i in range(1,5 + 1)]
        df_14 = reduce(lambda df_141,df_142: df_141.append(df_142), df_14_list)

        df_15 = pd.DataFrame()
        for year in [2016, 2017, 2018, 2019, 2020]:
            df_query = query(parameters[24],year)
            for i in range(0, len(df_query)):
                mini_df_query = pd.io.json.json_normalize(df_query[df_query.columns[2]][i])
                mini_df_query = mini_df_query[['TITULO','CATEGORIA']]
                mini_df_query['department_id'] = df_query[df_query.columns[0]][i]
                mini_df_query['year'] = df_query[df_query.columns[4]][i]
                df_15 = df_15.append(mini_df_query)
        df_15['indicator_id'] = 'Alerta contra el racismo'
        df_15['nation_id'] = 'per'
        df_15['response'] = 1
        df_15.rename(columns={'CATEGORIA' : 'category_id', 'TITULO' : 'subcategory_id'}, inplace=True)
        df_15 = df_15[['year', 'indicator_id', 'category_id', 'subcategory_id', 'department_id','nation_id','response']]
        

        df_list = [df_1, df_2, df_3, df_4, df_5, df_6, df_7, df_8, df_9, df_10, df_11, df_12, df_13, df_14, df_15]

        df = reduce(lambda df1,df2: df1.append(df2), df_list)
        df.rename(columns={'response_name':'response_id'}, inplace=True)
        df['category_id'] = df['category_id'].replace('_', ' ').str.title()
        df['department_id'] = df['department_id'].astype(str)
        
        return df

class FormatStep(PipelineStep):
    def run_step(self, prev, params):

        df = prev[0]
        df[['year', 'indicator_id', 'category_id', 'subcategory_id']] = df[['year', 'indicator_id', 'category_id', 'subcategory_id']].fillna(0).astype(int) 
        df['response'] = df['response'].fillna(0).astype(float)
        return df        

class InfoculturaPipeline(EasyPipeline):
    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open('../../conns.yaml'))

        dtype = {
            'year':                    'UInt32',
            'indicator_id':            'UInt8',
            'category_id':             'UInt8',
            'subcategory_id':          'UInt16',
            'department_id':           'String',
            'nation_id':               'String',
            'response':                'Float32',
        }

        transform_step = TransformStep()
        replace_step = ReplaceStep()
        format_step = FormatStep()
        
        load_step = LoadStep('cultura_infocultura_year', db_connector, if_exists='drop', 
                            pk=['department_id'], dtype=dtype, nullable_list = ['subcategory_id', 'response'])

        return [transform_step, replace_step, format_step, load_step]



if __name__ == "__main__":
    pp = InfoculturaPipeline()
    pp.run({})