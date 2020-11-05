import os

import numpy as np
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.helpers import query_to_df
from bamboo_lib.models import EasyPipeline, PipelineStep
from bamboo_lib.steps import LoadStep
from etl.consistency import AggregatorStep

from .static import (COLUMNS_DICT, DTYPES, NULLABLE_LISTS, PRIMARY_KEYS,
                     SELECTED_COLUMNS, VARIABLES_DICT)


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        path = os.path.join(
            params["datasets"],
            "20201001",
            "02. Información Censos (01-10-2020)",
            "02  RENAMU - CENTROS POBLADOS",
        )

        # Open all RENAMU files and creates a DataFrame

        df = pd.DataFrame()
        renamu_versions = {}

        for folder in os.listdir(path):
            _df = pd.DataFrame()
            for subfolder in os.listdir('{}{}'.format(path, folder)):
                for filename in os.listdir('{}{}/{}'.format(path, folder, subfolder)):
                     if filename.endswith('.sav'):
                        temp = pd.read_spss('{}{}/{}/{}'.format(path, folder, subfolder, filename))
                        temp = temp.replace('', np.nan).dropna(how='all')
                        temp.rename(
                            columns = {
                                'DesCenPob': 'populated_center_name',
                                'Codigo_MCP': 'populated_center_id',
                                'Ubigeo': 'populated_center_id',
                                'idimunici': 'populated_center_id',
                                'idmunici': 'populated_center_id'
                            },
                            inplace = True
                        )

                        temp = temp.loc[:, temp.columns.isin(SELECTED_COLUMNS[int(folder[-4:])])].copy()
                        temp.drop_duplicates(inplace = True)

                        if _df.shape[0] != 0:
                            _df = pd.merge(_df, temp, on = 'populated_center_id', suffixes = ('', '_drop'))
                            _df = _df.iloc[:, ~_df.columns.str.contains('_drop')]
                        else:
                            _df = temp

            _df['year'] = int(folder[-4:])

            if int(folder[-4:]) < 2018:
                _df.rename(columns=COLUMNS_DICT, inplace=True)
                _df['P18_T'] = _df['P18A_01'] + _df['P18A_02'] + _df['P18A_03'] + _df['P18A_04']

            for item in VARIABLES_DICT[int(folder[-4:])]:
                _df[item] = _df[item].replace(VARIABLES_DICT[int(folder[-4:])][item])

            renamu_versions[int(folder[-4:])] = _df

        for item in renamu_versions:
            df = df.append(renamu_versions[item], sort=False)

        # Creates aditional geo levels

        df['nation_id'] = 'per'
        df['department_id'] = df['populated_center_id'].str[0:2]
        df['province_id'] = df['populated_center_id'].str[0:4]

        if params.get('level') == 'dimension_table':

            df = df[['province_id', 'populated_center_id', 'populated_center_name']].copy()
            df = df.drop_duplicates()

            dim_geo_query = 'SELECT * FROM dim_shared_ubigeo_province'
            db_connector = Connector.fetch('clickhouse-database', open('../../conns.yaml'))
            dim_geo = query_to_df(db_connector, raw_query=dim_geo_query)

            df = pd.merge(df, dim_geo, on='province_id')

            return df

        # Generates count column to aggregate values
        df['count'] = 1
        df = df.fillna(0)

        # Creates auxiliar columns to aggregate special values

        df['CCPP_66_AUX'] = df['P14'] + df['P16_01'] + df['P16_02'] + df['P16_03'] + df['P16_04'] + df['P16_05'] + df['P16_07'] + df['P16_08'] + df['P16_09'] + df['P16_10'] + df['P16_11'] + df['P16_12'] + df['P16_13'] + df['P16_14']
        df['CCPP_66_0_AUX'] = df.apply(lambda x: 1 if x['CCPP_66_AUX'] == 0 else 0, axis=1)
        df['CCPP_66_1_AUX'] = df.apply(lambda x: 1 if x['CCPP_66_AUX'] < 5 else 0, axis=1)
        df['CCPP_66_2_AUX'] = df.apply(lambda x: 1 if x['CCPP_66_AUX'] >= 5 and x['CCPP_66_AUX'] < 11 else 0, axis=1)
        df['CCPP_66_3_AUX'] = df.apply(lambda x: 1 if x['CCPP_66_AUX'] > 11 else 0, axis=1)

        # Creates aggregated DataFrames for provinces, departments and nation levels

        df_province = df.drop(columns=['populated_center_id']).groupby(by=['province_id', 'year']).sum().reset_index()
        df_department = df.drop(columns=['populated_center_id']).groupby(by=['department_id', 'year']).sum().reset_index()
        df_nation = df.drop(columns=['populated_center_id']).groupby(by=['nation_id', 'year']).sum().reset_index()

        # Calculates populated center level indicators

        output_district = df[['populated_center_id', 'year']].copy()
        output_district['CCPP_1'] = df['P11_01_M'] + df['P11_01_H'] + df['P11_02_M'] + df['P11_02_H']
        output_district['CCPP_2'] = df['P11_01_M'] + df['P11_02_M']
        output_district['CCPP_3'] = ((df['P11_01_M'] + df['P11_02_M']) * 100) / (df['P11_01_M'] + df['P11_01_H'] + df['P11_02_M'] + df['P11_02_H'])
        output_district['CCPP_4'] = df['P11_01_H'] + df['P11_02_H']
        output_district['CCPP_5'] = ((df['P11_01_H'] + df['P11_02_H']) * 100) / (df['P11_01_M'] + df['P11_01_H'] + df['P11_02_M'] + df['P11_02_H'])
        output_district['CCPP_6'] = df['P11_01_M']
        output_district['CCPP_7'] = (df['P11_01_M'] * 100) / (df['P11_01_M'] + df['P11_02_M'])
        output_district['CCPP_8'] = df['P11_01_H']
        output_district['CCPP_9'] = (df['P11_01_H'] * 100) / (df['P11_01_H'] + df['P11_02_H'])
        output_district['CCPP_10'] = df['P11_02_M']
        output_district['CCPP_11'] = (df['P11_02_M'] * 100) / (df['P11_01_M'] + df['P11_02_M'])
        output_district['CCPP_12'] = df['P11_02_H']
        output_district['CCPP_13'] = (df['P11_02_H'] * 100) / (df['P11_01_H'] + df['P11_02_H'])
        output_district['CCPP_14'] = df['P12']
        output_district['CCPP_15'] = np.nan
        output_district['CCPP_16'] = np.nan
        output_district['CCPP_17'] = df['P13']
        output_district['CCPP_18'] = np.nan
        output_district['CCPP_19'] = np.nan
        output_district['CCPP_20'] = df['P14']
        output_district['CCPP_21'] = np.nan
        output_district['CCPP_22'] = np.nan
        output_district['CCPP_23'] = df['P14_01']
        output_district['CCPP_24'] = df['P16_03']
        output_district['CCPP_25'] = np.nan
        output_district['CCPP_26'] = np.nan
        output_district['CCPP_27'] = df['P16_04']
        output_district['CCPP_28'] = np.nan
        output_district['CCPP_29'] = np.nan
        output_district['CCPP_30'] = df['P16_05']
        output_district['CCPP_31'] = np.nan
        output_district['CCPP_32'] = np.nan
        output_district['CCPP_33'] = df['P16_06']
        output_district['CCPP_34'] = np.nan
        output_district['CCPP_35'] = np.nan
        output_district['CCPP_36'] = df['P16_07']
        output_district['CCPP_37'] = np.nan
        output_district['CCPP_38'] = np.nan
        output_district['CCPP_39'] = df['P16_08']
        output_district['CCPP_40'] = np.nan
        output_district['CCPP_41'] = np.nan
        output_district['CCPP_42'] = df['P16_09']
        output_district['CCPP_43'] = np.nan
        output_district['CCPP_44'] = np.nan
        output_district['CCPP_45'] = df['P16_10']
        output_district['CCPP_46'] = np.nan
        output_district['CCPP_47'] = np.nan
        output_district['CCPP_48'] = df['P16_11']
        output_district['CCPP_49'] = np.nan
        output_district['CCPP_50'] = np.nan
        output_district['CCPP_51'] = df['P16_12']
        output_district['CCPP_52'] = np.nan
        output_district['CCPP_53'] = np.nan
        output_district['CCPP_54'] = df['P16_13']
        output_district['CCPP_55'] = np.nan
        output_district['CCPP_56'] = np.nan
        output_district['CCPP_57'] = df['P16_14']
        output_district['CCPP_58'] = np.nan
        output_district['CCPP_59'] = np.nan
        output_district['CCPP_60'] = df['P16_01']
        output_district['CCPP_61'] = np.nan
        output_district['CCPP_62'] = np.nan
        output_district['CCPP_63'] = df['P16_02']
        output_district['CCPP_64'] = np.nan
        output_district['CCPP_65'] = np.nan
        output_district['CCPP_66'] = df.apply(lambda x: 0 if x['CCPP_66_AUX'] == 0 else 1 if x['CCPP_66_AUX'] < 5 else 2 if x['CCPP_66_AUX'] >= 5 and x['CCPP_66_AUX'] < 11 else 3 if x['CCPP_66_AUX'] >= 11 else 0, axis=1)
        output_district['CCPP_67'] = np.nan
        output_district['CCPP_68'] = np.nan
        output_district['CCPP_69'] = np.nan
        output_district['CCPP_70'] = np.nan
        output_district['CCPP_71'] = np.nan
        output_district['CCPP_72'] = np.nan
        output_district['CCPP_73'] = np.nan
        output_district['CCPP_74'] = np.nan
        output_district['CCPP_75'] = df['P17']
        output_district['CCPP_76'] = np.nan
        output_district['CCPP_77'] = np.nan
        output_district['CCPP_78'] = df['P17_01']
        output_district['CCPP_79'] = np.nan
        output_district['CCPP_80'] = df['P18_T']
        output_district['CCPP_81'] = np.nan
        output_district['CCPP_82'] = df['P18A_01']
        output_district['CCPP_83'] = np.nan
        output_district['CCPP_84'] = (df['P18A_01'] * 100) / df['P18_T']
        output_district['CCPP_85'] = df['P18A_02']
        output_district['CCPP_86'] = np.nan
        output_district['CCPP_87'] = (df['P18A_02'] * 100) / df['P18_T']
        output_district['CCPP_88'] = df['P18A_03']
        output_district['CCPP_89'] = np.nan
        output_district['CCPP_90'] = (df['P18A_03'] * 100) / df['P18_T']

        # Calculates province level indicators

        output_province = df_province[['province_id', 'year']].copy()
        output_province['CCPP_1'] = df_province['P11_01_M'] + df_province['P11_01_H'] + df_province['P11_02_M'] + df_province['P11_02_H']
        output_province['CCPP_2'] = df_province['P11_01_M'] + df_province['P11_02_M']
        output_province['CCPP_3'] = ((df_province['P11_01_M'] + df_province['P11_02_M']) * 100) / (df_province['P11_01_M'] + df_province['P11_01_H'] + df_province['P11_02_M'] + df_province['P11_02_H'])
        output_province['CCPP_4'] = df_province['P11_01_H'] + df_province['P11_02_H']
        output_province['CCPP_5'] = ((df_province['P11_01_H'] + df_province['P11_02_H']) * 100) / (df_province['P11_01_M'] + df_province['P11_01_H'] + df_province['P11_02_M'] + df_province['P11_02_H'])
        output_province['CCPP_6'] = df_province['P11_01_M']
        output_province['CCPP_7'] = (df_province['P11_01_M'] * 100) / (df_province['P11_01_M'] + df_province['P11_02_M'])
        output_province['CCPP_8'] = df_province['P11_01_H']
        output_province['CCPP_9'] = (df_province['P11_01_H'] * 100) / (df_province['P11_01_H'] + df_province['P11_02_H'])
        output_province['CCPP_10'] = df_province['P11_02_M']
        output_province['CCPP_11'] = (df_province['P11_02_M'] * 100) / (df_province['P11_01_M'] + df_province['P11_02_M'])
        output_province['CCPP_12'] = df_province['P11_02_H']
        output_province['CCPP_13'] = (df_province['P11_02_H'] * 100) / (df_province['P11_01_H'] + df_province['P11_02_H'])
        output_province['CCPP_14'] = np.nan
        output_province['CCPP_15'] = df_province['P12']
        output_province['CCPP_16'] = (df_province['P12'] * 100) / df_province['count']
        output_province['CCPP_17'] = np.nan
        output_province['CCPP_18'] = df_province['P13']
        output_province['CCPP_19'] = (df_province['P13'] * 100) / df_province['count']
        output_province['CCPP_20'] = np.nan
        output_province['CCPP_21'] = df_province['P14']
        output_province['CCPP_22'] = (df_province['P14'] * 100) / df_province['count']
        output_province['CCPP_23'] = df_province['P14_01']
        output_province['CCPP_24'] = np.nan
        output_province['CCPP_25'] = df_province['P16_03']
        output_province['CCPP_26'] = (df_province['P16_03'] * 100) / df_province['count']
        output_province['CCPP_27'] = np.nan
        output_province['CCPP_28'] = df_province['P16_04']
        output_province['CCPP_29'] = (df_province['P16_04'] * 100) / df_province['count']
        output_province['CCPP_30'] = np.nan
        output_province['CCPP_31'] = df_province['P16_05']
        output_province['CCPP_32'] = (df_province['P16_05'] * 100) / df_province['count']
        output_province['CCPP_33'] = np.nan
        output_province['CCPP_34'] = df_province['P16_06']
        output_province['CCPP_35'] = (df_province['P16_06'] * 100) / df_province['count']
        output_province['CCPP_36'] = np.nan
        output_province['CCPP_37'] = df_province['P16_07']
        output_province['CCPP_38'] = (df_province['P16_07'] * 100) / df_province['count']
        output_province['CCPP_39'] = np.nan
        output_province['CCPP_40'] = df_province['P16_08']
        output_province['CCPP_41'] = (df_province['P16_08'] * 100) / df_province['count']
        output_province['CCPP_42'] = np.nan
        output_province['CCPP_43'] = df_province['P16_09']
        output_province['CCPP_44'] = (df_province['P16_09'] * 100) / df_province['count']
        output_province['CCPP_45'] = np.nan
        output_province['CCPP_46'] = df_province['P16_10']
        output_province['CCPP_47'] = (df_province['P16_10'] * 100) / df_province['count']
        output_province['CCPP_48'] = np.nan
        output_province['CCPP_49'] = df_province['P16_11']
        output_province['CCPP_50'] = (df_province['P16_11'] * 100) / df_province['count']
        output_province['CCPP_51'] = np.nan
        output_province['CCPP_52'] = df_province['P16_12']
        output_province['CCPP_53'] = (df_province['P16_12'] * 100) / df_province['count']
        output_province['CCPP_54'] = np.nan
        output_province['CCPP_55'] = df_province['P16_13']
        output_province['CCPP_56'] = (df_province['P16_13'] * 100) / df_province['count']
        output_province['CCPP_57'] = np.nan
        output_province['CCPP_58'] = df_province['P16_14']
        output_province['CCPP_59'] = (df_province['P16_14'] * 100) / df_province['count']
        output_province['CCPP_60'] = np.nan
        output_province['CCPP_61'] = df_province['P16_01']
        output_province['CCPP_62'] = (df_province['P16_01'] * 100) / df_province['count']
        output_province['CCPP_63'] = np.nan
        output_province['CCPP_64'] = df_province['P16_02']
        output_province['CCPP_65'] = (df_province['P16_02'] * 100) / df_province['count']
        output_province['CCPP_66'] = np.nan
        output_province['CCPP_67'] = df_province['CCPP_66_0_AUX']
        output_province['CCPP_68'] = (df_province['CCPP_66_0_AUX'] * 100) / df_province['count']
        output_province['CCPP_69'] = df_province['CCPP_66_1_AUX']
        output_province['CCPP_70'] = (df_province['CCPP_66_1_AUX'] * 100) / df_province['count']
        output_province['CCPP_71'] = df_province['CCPP_66_2_AUX']
        output_province['CCPP_72'] = (df_province['CCPP_66_2_AUX'] * 100) / df_province['count']
        output_province['CCPP_73'] = df_province['CCPP_66_3_AUX']
        output_province['CCPP_74'] = (df_province['CCPP_66_3_AUX'] * 100) / df_province['count']
        output_province['CCPP_75'] = np.nan
        output_province['CCPP_76'] = df_province['P17']
        output_province['CCPP_77'] = (df_province['P17'] * 100) / df_province['count']
        output_province['CCPP_78'] = np.nan
        output_province['CCPP_79'] = (df_province['P17_01'] * 100) / df_province['count']
        output_province['CCPP_80'] = np.nan
        output_province['CCPP_81'] = df_province['P18_T'] / df_province['count']
        output_province['CCPP_82'] = np.nan
        output_province['CCPP_83'] = df_province['P18A_01'] / df_province['count']
        output_province['CCPP_84'] = np.nan
        output_province['CCPP_85'] = np.nan
        output_province['CCPP_86'] = df_province['P18A_02'] / df_province['count']
        output_province['CCPP_87'] = np.nan
        output_province['CCPP_88'] = np.nan
        output_province['CCPP_89'] = df_province['P18A_03'] / df_province['count']
        output_province['CCPP_90'] = np.nan

        # Calculates department level indicators

        output_department = df_department[['department_id', 'year']].copy()
        output_department['CCPP_1'] = df_department['P11_01_M'] + df_department['P11_01_H'] + df_department['P11_02_M'] + df_department['P11_02_H']
        output_department['CCPP_2'] = df_department['P11_01_M'] + df_department['P11_02_M']
        output_department['CCPP_3'] = ((df_department['P11_01_M'] + df_department['P11_02_M']) * 100) / (df_department['P11_01_M'] + df_department['P11_01_H'] + df_department['P11_02_M'] + df_department['P11_02_H'])
        output_department['CCPP_4'] = df_department['P11_01_H'] + df_department['P11_02_H']
        output_department['CCPP_5'] = ((df_department['P11_01_H'] + df_department['P11_02_H']) * 100) / (df_department['P11_01_M'] + df_department['P11_01_H'] + df_department['P11_02_M'] + df_department['P11_02_H'])
        output_department['CCPP_6'] = df_department['P11_01_M']
        output_department['CCPP_7'] = (df_department['P11_01_M'] * 100) / (df_department['P11_01_M'] + df_department['P11_02_M'])
        output_department['CCPP_8'] = df_department['P11_01_H']
        output_department['CCPP_9'] = (df_department['P11_01_H'] * 100) / (df_department['P11_01_H'] + df_department['P11_02_H'])
        output_department['CCPP_10'] = df_department['P11_02_M']
        output_department['CCPP_11'] = (df_department['P11_02_M'] * 100) / (df_department['P11_01_M'] + df_department['P11_02_M'])
        output_department['CCPP_12'] = df_department['P11_02_H']
        output_department['CCPP_13'] = (df_department['P11_02_H'] * 100) / (df_department['P11_01_H'] + df_department['P11_02_H'])
        output_department['CCPP_14'] = np.nan
        output_department['CCPP_15'] = df_department['P12']
        output_department['CCPP_16'] = (df_department['P12'] * 100) / df_department['count']
        output_department['CCPP_17'] = np.nan
        output_department['CCPP_18'] = df_department['P13']
        output_department['CCPP_19'] = (df_department['P13'] * 100) / df_department['count']
        output_department['CCPP_20'] = np.nan
        output_department['CCPP_21'] = df_department['P14']
        output_department['CCPP_22'] = (df_department['P14'] * 100) / df_department['count']
        output_department['CCPP_23'] = df_department['P14_01']
        output_department['CCPP_24'] = np.nan
        output_department['CCPP_25'] = df_department['P16_03']
        output_department['CCPP_26'] = (df_department['P16_03'] * 100) / df_department['count']
        output_department['CCPP_27'] = np.nan
        output_department['CCPP_28'] = df_department['P16_04']
        output_department['CCPP_29'] = (df_department['P16_04'] * 100) / df_department['count']
        output_department['CCPP_30'] = np.nan
        output_department['CCPP_31'] = df_department['P16_05']
        output_department['CCPP_32'] = (df_department['P16_05'] * 100) / df_department['count']
        output_department['CCPP_33'] = np.nan
        output_department['CCPP_34'] = df_department['P16_06']
        output_department['CCPP_35'] = (df_department['P16_06'] * 100) / df_department['count']
        output_department['CCPP_36'] = np.nan
        output_department['CCPP_37'] = df_department['P16_07']
        output_department['CCPP_38'] = (df_department['P16_07'] * 100) / df_department['count']
        output_department['CCPP_39'] = np.nan
        output_department['CCPP_40'] = df_department['P16_08']
        output_department['CCPP_41'] = (df_department['P16_08'] * 100) / df_department['count']
        output_department['CCPP_42'] = np.nan
        output_department['CCPP_43'] = df_department['P16_09']
        output_department['CCPP_44'] = (df_department['P16_09'] * 100) / df_department['count']
        output_department['CCPP_45'] = np.nan
        output_department['CCPP_46'] = df_department['P16_10']
        output_department['CCPP_47'] = (df_department['P16_10'] * 100) / df_department['count']
        output_department['CCPP_48'] = np.nan
        output_department['CCPP_49'] = df_department['P16_11']
        output_department['CCPP_50'] = (df_department['P16_11'] * 100) / df_department['count']
        output_department['CCPP_51'] = np.nan
        output_department['CCPP_52'] = df_department['P16_12']
        output_department['CCPP_53'] = (df_department['P16_12'] * 100) / df_department['count']
        output_department['CCPP_54'] = np.nan
        output_department['CCPP_55'] = df_department['P16_13']
        output_department['CCPP_56'] = (df_department['P16_13'] * 100) / df_department['count']
        output_department['CCPP_57'] = np.nan
        output_department['CCPP_58'] = df_department['P16_14']
        output_department['CCPP_59'] = (df_department['P16_14'] * 100) / df_department['count']
        output_department['CCPP_60'] = np.nan
        output_department['CCPP_61'] = df_department['P16_01']
        output_department['CCPP_62'] = (df_department['P16_01'] * 100) / df_department['count']
        output_department['CCPP_63'] = np.nan
        output_department['CCPP_64'] = df_department['P16_02']
        output_department['CCPP_65'] = (df_department['P16_02'] * 100) / df_department['count']
        output_department['CCPP_66'] = np.nan
        output_department['CCPP_67'] = df_department['CCPP_66_0_AUX']
        output_department['CCPP_68'] = (df_department['CCPP_66_0_AUX'] * 100) / df_department['count']
        output_department['CCPP_69'] = df_department['CCPP_66_1_AUX']
        output_department['CCPP_70'] = (df_department['CCPP_66_1_AUX'] * 100) / df_department['count']
        output_department['CCPP_71'] = df_department['CCPP_66_2_AUX']
        output_department['CCPP_72'] = (df_department['CCPP_66_2_AUX'] * 100) / df_department['count']
        output_department['CCPP_73'] = df_department['CCPP_66_3_AUX']
        output_department['CCPP_74'] = (df_department['CCPP_66_3_AUX'] * 100) / df_department['count']
        output_department['CCPP_75'] = np.nan
        output_department['CCPP_76'] = df_department['P17']
        output_department['CCPP_77'] = (df_department['P17'] * 100) / df_department['count']
        output_department['CCPP_78'] = np.nan
        output_department['CCPP_79'] = (df_department['P17_01'] * 100) / df_department['count']
        output_department['CCPP_80'] = np.nan
        output_department['CCPP_81'] = df_department['P18_T'] / df_department['count']
        output_department['CCPP_82'] = np.nan
        output_department['CCPP_83'] = df_department['P18A_01'] / df_department['count']
        output_department['CCPP_84'] = np.nan
        output_department['CCPP_85'] = np.nan
        output_department['CCPP_86'] = df_department['P18A_02'] / df_department['count']
        output_department['CCPP_87'] = np.nan
        output_department['CCPP_88'] = np.nan
        output_department['CCPP_89'] = df_department['P18A_03'] / df_department['count']
        output_department['CCPP_90'] = np.nan

        # Calculates nation level indicators

        output_nation = df_nation[['nation_id', 'year']].copy()
        output_nation['CCPP_1'] = df_nation['P11_01_M'] + df_nation['P11_01_H'] + df_nation['P11_02_M'] + df_nation['P11_02_H']
        output_nation['CCPP_2'] = df_nation['P11_01_M'] + df_nation['P11_02_M']
        output_nation['CCPP_3'] = ((df_nation['P11_01_M'] + df_nation['P11_02_M']) * 100) / (df_nation['P11_01_M'] + df_nation['P11_01_H'] + df_nation['P11_02_M'] + df_nation['P11_02_H'])
        output_nation['CCPP_4'] = df_nation['P11_01_H'] + df_nation['P11_02_H']
        output_nation['CCPP_5'] = ((df_nation['P11_01_H'] + df_nation['P11_02_H']) * 100) / (df_nation['P11_01_M'] + df_nation['P11_01_H'] + df_nation['P11_02_M'] + df_nation['P11_02_H'])
        output_nation['CCPP_6'] = df_nation['P11_01_M']
        output_nation['CCPP_7'] = (df_nation['P11_01_M'] * 100) / (df_nation['P11_01_M'] + df_nation['P11_02_M'])
        output_nation['CCPP_8'] = df_nation['P11_01_H']
        output_nation['CCPP_9'] = (df_nation['P11_01_H'] * 100) / (df_nation['P11_01_H'] + df_nation['P11_02_H'])
        output_nation['CCPP_10'] = df_nation['P11_02_M']
        output_nation['CCPP_11'] = (df_nation['P11_02_M'] * 100) / (df_nation['P11_01_M'] + df_nation['P11_02_M'])
        output_nation['CCPP_12'] = df_nation['P11_02_H']
        output_nation['CCPP_13'] = (df_nation['P11_02_H'] * 100) / (df_nation['P11_01_H'] + df_nation['P11_02_H'])
        output_nation['CCPP_14'] = np.nan
        output_nation['CCPP_15'] = df_nation['P12']
        output_nation['CCPP_16'] = (df_nation['P12'] * 100) / df_nation['count']
        output_nation['CCPP_17'] = np.nan
        output_nation['CCPP_18'] = df_nation['P13']
        output_nation['CCPP_19'] = (df_nation['P13'] * 100) / df_nation['count']
        output_nation['CCPP_20'] = np.nan
        output_nation['CCPP_21'] = df_nation['P14']
        output_nation['CCPP_22'] = (df_nation['P14'] * 100) / df_nation['count']
        output_nation['CCPP_23'] = df_nation['P14_01']
        output_nation['CCPP_24'] = np.nan
        output_nation['CCPP_25'] = df_nation['P16_03']
        output_nation['CCPP_26'] = (df_nation['P16_03'] * 100) / df_nation['count']
        output_nation['CCPP_27'] = np.nan
        output_nation['CCPP_28'] = df_nation['P16_04']
        output_nation['CCPP_29'] = (df_nation['P16_04'] * 100) / df_nation['count']
        output_nation['CCPP_30'] = np.nan
        output_nation['CCPP_31'] = df_nation['P16_05']
        output_nation['CCPP_32'] = (df_nation['P16_05'] * 100) / df_nation['count']
        output_nation['CCPP_33'] = np.nan
        output_nation['CCPP_34'] = df_nation['P16_06']
        output_nation['CCPP_35'] = (df_nation['P16_06'] * 100) / df_nation['count']
        output_nation['CCPP_36'] = np.nan
        output_nation['CCPP_37'] = df_nation['P16_07']
        output_nation['CCPP_38'] = (df_nation['P16_07'] * 100) / df_nation['count']
        output_nation['CCPP_39'] = np.nan
        output_nation['CCPP_40'] = df_nation['P16_08']
        output_nation['CCPP_41'] = (df_nation['P16_08'] * 100) / df_nation['count']
        output_nation['CCPP_42'] = np.nan
        output_nation['CCPP_43'] = df_nation['P16_09']
        output_nation['CCPP_44'] = (df_nation['P16_09'] * 100) / df_nation['count']
        output_nation['CCPP_45'] = np.nan
        output_nation['CCPP_46'] = df_nation['P16_10']
        output_nation['CCPP_47'] = (df_nation['P16_10'] * 100) / df_nation['count']
        output_nation['CCPP_48'] = np.nan
        output_nation['CCPP_49'] = df_nation['P16_11']
        output_nation['CCPP_50'] = (df_nation['P16_11'] * 100) / df_nation['count']
        output_nation['CCPP_51'] = np.nan
        output_nation['CCPP_52'] = df_nation['P16_12']
        output_nation['CCPP_53'] = (df_nation['P16_12'] * 100) / df_nation['count']
        output_nation['CCPP_54'] = np.nan
        output_nation['CCPP_55'] = df_nation['P16_13']
        output_nation['CCPP_56'] = (df_nation['P16_13'] * 100) / df_nation['count']
        output_nation['CCPP_57'] = np.nan
        output_nation['CCPP_58'] = df_nation['P16_14']
        output_nation['CCPP_59'] = (df_nation['P16_14'] * 100) / df_nation['count']
        output_nation['CCPP_60'] = np.nan
        output_nation['CCPP_61'] = df_nation['P16_01']
        output_nation['CCPP_62'] = (df_nation['P16_01'] * 100) / df_nation['count']
        output_nation['CCPP_63'] = np.nan
        output_nation['CCPP_64'] = df_nation['P16_02']
        output_nation['CCPP_65'] = (df_nation['P16_02'] * 100) / df_nation['count']
        output_nation['CCPP_66'] = np.nan
        output_nation['CCPP_67'] = df_nation['CCPP_66_0_AUX']
        output_nation['CCPP_68'] = (df_nation['CCPP_66_0_AUX'] * 100) / df_nation['count']
        output_nation['CCPP_69'] = df_nation['CCPP_66_1_AUX']
        output_nation['CCPP_70'] = (df_nation['CCPP_66_1_AUX'] * 100) / df_nation['count']
        output_nation['CCPP_71'] = df_nation['CCPP_66_2_AUX']
        output_nation['CCPP_72'] = (df_nation['CCPP_66_2_AUX'] * 100) / df_nation['count']
        output_nation['CCPP_73'] = df_nation['CCPP_66_3_AUX']
        output_nation['CCPP_74'] = (df_nation['CCPP_66_3_AUX'] * 100) / df_nation['count']
        output_nation['CCPP_75'] = np.nan
        output_nation['CCPP_76'] = df_nation['P17']
        output_nation['CCPP_77'] = (df_nation['P17'] * 100) / df_nation['count']
        output_nation['CCPP_78'] = np.nan
        output_nation['CCPP_79'] = (df_nation['P17_01'] * 100) / df_nation['count']
        output_nation['CCPP_80'] = np.nan
        output_nation['CCPP_81'] = df_nation['P18_T'] / df_nation['count']
        output_nation['CCPP_82'] = np.nan
        output_nation['CCPP_83'] = df_nation['P18A_01'] / df_nation['count']
        output_nation['CCPP_84'] = np.nan
        output_nation['CCPP_85'] = np.nan
        output_nation['CCPP_86'] = df_nation['P18A_02'] / df_nation['count']
        output_nation['CCPP_87'] = np.nan
        output_nation['CCPP_88'] = np.nan
        output_nation['CCPP_89'] = df_nation['P18A_03'] / df_nation['count']
        output_nation['CCPP_90'] = np.nan

        # Append indicators DataFrames
        output = output_nation.append([output_department, output_province, output_district], sort=False)

        output['nation_id'].fillna(0, inplace=True)
        output['department_id'].fillna(0, inplace=True)
        output['province_id'].fillna(0, inplace=True)
        output['populated_center_id'].fillna(0, inplace=True)

        output['nation_id'] = output['nation_id'].astype(str)
        output['department_id'] = output['department_id'].astype(str)
        output['province_id'] = output['province_id'].astype(str)
        output['populated_center_id'] = output['populated_center_id'].astype(str)

        return output


class RENAMUCCPPPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        table_name = params["table_name"]
        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))

        transform_step = TransformStep()

        agg_step = AggregatorStep(table_name, measures=[])

        load_step = LoadStep(table_name, db_connector, if_exists='drop',
                             pk=PRIMARY_KEYS[params.get('level')], dtype=DTYPES[params.get('level')],
                             nullable_list=NULLABLE_LISTS[params.get('level')])

        return [transform_step, agg_step, load_step]


def run_pipeline(params):
    pp = RENAMUCCPPPipeline()
    levels = {
        "dimension_table": "dim_shared_populated_centers",
        "fact_table": "inei_renamu_populated_centers",
    }

    for k, v in levels.items():
        pp_params = {"level": k, "table_name": v}
        pp_params.update(params)
        pp.run(pp_params)


if __name__ == "__main__":
    import sys

    run_pipeline({
        "connector": "../../conns.yaml",
        "datasets": sys.argv[1]
    })
