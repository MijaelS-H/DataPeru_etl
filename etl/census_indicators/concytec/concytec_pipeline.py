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

        agg_step = AggregatorStep(table_name, measures=[
            'CONCYTEC_1', 'CONCYTEC_2', 'CONCYTEC_3', 'CONCYTEC_4', 'CONCYTEC_5', 'CONCYTEC_6', 'CONCYTEC_7', 'CONCYTEC_8', 'CONCYTEC_9', 'CONCYTEC_10', 
            'CONCYTEC_11', 'CONCYTEC_12', 'CONCYTEC_13', 'CONCYTEC_14', 'CONCYTEC_15', 'CONCYTEC_16', 'CONCYTEC_17', 'CONCYTEC_18', 'CONCYTEC_19', 'CONCYTEC_20', 
            'CONCYTEC_21', 'CONCYTEC_22', 'CONCYTEC_23', 'CONCYTEC_24', 'CONCYTEC_25', 'CONCYTEC_26', 'CONCYTEC_27', 'CONCYTEC_28', 'CONCYTEC_29', 'CONCYTEC_30', 
            'CONCYTEC_31', 'CONCYTEC_32', 'CONCYTEC_33', 'CONCYTEC_34', 'CONCYTEC_35', 'CONCYTEC_36', 'CONCYTEC_37', 'CONCYTEC_38', 'CONCYTEC_39', 'CONCYTEC_40', 
            'CONCYTEC_41', 'CONCYTEC_42', 'CONCYTEC_43', 'CONCYTEC_44', 'CONCYTEC_45', 'CONCYTEC_46', 'CONCYTEC_47', 'CONCYTEC_48', 'CONCYTEC_49', 'CONCYTEC_50', 
            'CONCYTEC_51', 'CONCYTEC_52', 'CONCYTEC_53', 'CONCYTEC_54', 'CONCYTEC_55', 'CONCYTEC_56', 'CONCYTEC_57', 'CONCYTEC_58', 'CONCYTEC_59', 'CONCYTEC_60', 
            'CONCYTEC_61', 'CONCYTEC_62', 'CONCYTEC_63', 'CONCYTEC_64', 'CONCYTEC_65', 'CONCYTEC_66', 'CONCYTEC_67', 'CONCYTEC_68', 'CONCYTEC_69', 'CONCYTEC_70', 
            'CONCYTEC_71', 'CONCYTEC_72', 'CONCYTEC_73', 'CONCYTEC_74', 'CONCYTEC_75', 'CONCYTEC_76', 'CONCYTEC_77', 'CONCYTEC_78', 'CONCYTEC_79', 'CONCYTEC_80', 
            'CONCYTEC_81', 'CONCYTEC_82'
        ])

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
    from os import path
    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline({
        "connector": path.join(__dirname, "..", "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
