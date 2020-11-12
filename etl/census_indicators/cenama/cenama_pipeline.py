from os import path

import numpy as np
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep
from bamboo_lib.steps import LoadStep
from etl.consistency import AggregatorStep

from .indicator import INDICATOR_EXCEPTION, INDICATOR_GEO, INDICATOR_MARKET
from .static import COLUMNS_RENAME, DICT_GROUPBY, DTYPE, LIST_DICT, LIST_NULL


def convert_string(column):
    new_data = pd.DataFrame()
    new_data[["number", "decimal"]] = column.str.split(",", expand=True)
    new_data["decimal"] = new_data["decimal"].fillna("0")
    new_data["total"] = new_data["number"] + "." + new_data["decimal"]
    return new_data["total"]


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        #read modules
        list_name = [
            path.join(params["datasets"], "20201001", "02. Información Censos (01-10-2020)", "03 CENSO NACIONAL DE MERCADOS DE ABASTO", "03 MÓDULO 1118_ Características del Mercado", "Capítulo_IV_NACIONAL.dta"),
            path.join(params["datasets"], "20201001", "02. Información Censos (01-10-2020)", "03 CENSO NACIONAL DE MERCADOS DE ABASTO", "04 MÓDULO 1119_ Infraestructura, Instalaciones, Equipamiento y Otros", "Capítulo_V_NACIONAL.sav"),
            path.join(params["datasets"], "20201001", "02. Información Censos (01-10-2020)", "03 CENSO NACIONAL DE MERCADOS DE ABASTO", "05 MÓDULO 1120_ Gestión Administrativa y Financiera", "Capítulo_VI_NACIONAL.sav"),
            path.join(params["datasets"], "20201001", "02. Información Censos (01-10-2020)", "03 CENSO NACIONAL DE MERCADOS DE ABASTO", "01 MÓDULO 1116_ Localización del Mercado", "Capítulo_I_NACIONAL.sav"),
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
        table_name = "inei_cenama"
        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))

        transform_step = TransformStep()

        agg_step = AggregatorStep(table_name, measures=[
            'CENAMA_1', 'CENAMA_2', 'CENAMA_3', 'CENAMA_4', 'CENAMA_5', 'CENAMA_6', 'CENAMA_7', 'CENAMA_8', 'CENAMA_9', 'CENAMA_10', 
            'CENAMA_11', 'CENAMA_12', 'CENAMA_13', 'CENAMA_14', 'CENAMA_15', 'CENAMA_16', 'CENAMA_17', 'CENAMA_18', 'CENAMA_19', 'CENAMA_20', 
            'CENAMA_21', 'CENAMA_22', 'CENAMA_23', 'CENAMA_24', 'CENAMA_25', 'CENAMA_26', 'CENAMA_27', 'CENAMA_28', 'CENAMA_29', 'CENAMA_30', 
            'CENAMA_31', 'CENAMA_32', 'CENAMA_33', 'CENAMA_34', 'CENAMA_35', 'CENAMA_36', 'CENAMA_37', 'CENAMA_38', 'CENAMA_39', 'CENAMA_40', 
            'CENAMA_41', 'CENAMA_42', 'CENAMA_43', 'CENAMA_44', 'CENAMA_45', 'CENAMA_46', 'CENAMA_47', 'CENAMA_48', 'CENAMA_49', 'CENAMA_50', 
            'CENAMA_51', 'CENAMA_52', 'CENAMA_53', 'CENAMA_54', 'CENAMA_55', 'CENAMA_56', 'CENAMA_57', 'CENAMA_58', 'CENAMA_59', 'CENAMA_60', 
            'CENAMA_61', 'CENAMA_62', 'CENAMA_63', 'CENAMA_64', 'CENAMA_65', 'CENAMA_66', 'CENAMA_67', 'CENAMA_68', 'CENAMA_69', 'CENAMA_70', 
            'CENAMA_71', 'CENAMA_72', 'CENAMA_73', 'CENAMA_74', 'CENAMA_75', 'CENAMA_76', 'CENAMA_77', 'CENAMA_78', 'CENAMA_79', 'CENAMA_80', 
            'CENAMA_81', 'CENAMA_82', 'CENAMA_83', 'CENAMA_84', 'CENAMA_85', 'CENAMA_86', 'CENAMA_87', 'CENAMA_88', 'CENAMA_89', 'CENAMA_90', 
            'CENAMA_91', 'CENAMA_92', 'CENAMA_93', 'CENAMA_94', 'CENAMA_95', 'CENAMA_96', 'CENAMA_97', 'CENAMA_98', 'CENAMA_99', 'CENAMA_100', 
            'CENAMA_101', 'CENAMA_102', 'CENAMA_103', 'CENAMA_104', 'CENAMA_105', 'CENAMA_106', 'CENAMA_107', 'CENAMA_108', 'CENAMA_109', 'CENAMA_110', 
            'CENAMA_111', 'CENAMA_112', 'CENAMA_113', 'CENAMA_114', 'CENAMA_115', 'CENAMA_116', 'CENAMA_117', 'CENAMA_118', 'CENAMA_119', 'CENAMA_120', 
            'CENAMA_121', 'CENAMA_122', 'CENAMA_123', 'CENAMA_124', 'CENAMA_125', 'CENAMA_126', 'CENAMA_127', 'CENAMA_128', 'CENAMA_129', 'CENAMA_130', 
            'CENAMA_131', 'CENAMA_132', 'CENAMA_133', 'CENAMA_134', 'CENAMA_135', 'CENAMA_136', 'CENAMA_137', 'CENAMA_138', 'CENAMA_139', 'CENAMA_140', 
            'CENAMA_141', 'CENAMA_142', 'CENAMA_143', 'CENAMA_144', 'CENAMA_145', 'CENAMA_146', 'CENAMA_147', 'CENAMA_148', 'CENAMA_149', 'CENAMA_150', 
            'CENAMA_151', 'CENAMA_152', 'CENAMA_153', 'CENAMA_154', 'CENAMA_155', 'CENAMA_156', 'CENAMA_157', 'CENAMA_158', 'CENAMA_159', 'CENAMA_160', 
            'CENAMA_161', 'CENAMA_162', 'CENAMA_163', 'CENAMA_164', 'CENAMA_165', 'CENAMA_166', 'CENAMA_167', 'CENAMA_168', 'CENAMA_169', 'CENAMA_170', 
            'CENAMA_171', 'CENAMA_172', 'CENAMA_173', 'CENAMA_174', 'CENAMA_175', 'CENAMA_176', 'CENAMA_177', 'CENAMA_178', 'CENAMA_179', 'CENAMA_180', 
            'CENAMA_181', 'CENAMA_182', 'CENAMA_183', 'CENAMA_184', 'CENAMA_185', 'CENAMA_186', 'CENAMA_187', 'CENAMA_188', 'CENAMA_189', 'CENAMA_190', 
            'CENAMA_191', 'CENAMA_192', 'CENAMA_193', 'CENAMA_194', 'CENAMA_195', 'CENAMA_196', 'CENAMA_197', 'CENAMA_198', 'CENAMA_199', 'CENAMA_200', 
            'CENAMA_201', 'CENAMA_202', 'CENAMA_203', 'CENAMA_204', 'CENAMA_205', 'CENAMA_206', 'CENAMA_207', 'CENAMA_208', 'CENAMA_209', 'CENAMA_210', 
            'CENAMA_211', 'CENAMA_212', 'CENAMA_213', 'CENAMA_214', 'CENAMA_215', 'CENAMA_216', 'CENAMA_217', 'CENAMA_218', 'CENAMA_219', 'CENAMA_220', 
            'CENAMA_221', 'CENAMA_222', 'CENAMA_223' 
        ])

        load_step = LoadStep(table_name, db_connector, if_exists='drop', 
                             pk=['district_id', 'province_id', 'department_id',
                                 'nation_id', 'market_id', 'year'], dtype=DTYPE,
                             nullable_list=LIST_NULL)

        return [transform_step, agg_step, load_step]


def run_pipeline(params: dict):
    pp = CENAMAPipeline()
    pp.run(params)


if __name__ == "__main__":
    import sys
    from os import path
    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline({
        "connector": path.join(__dirname, "..", "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })