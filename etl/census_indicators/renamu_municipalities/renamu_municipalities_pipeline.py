import os
import glob
import numpy as np
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep
from bamboo_lib.steps import LoadStep
from .static import DTYPES, SELECTED_COLUMNS, VARIABLES_DICT, COLUMNS_DICT
from etl.consistency import AggregatorStep

pd.set_option('use_inf_as_na', True)

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        path = os.path.join(
            params["datasets"],
            "02_Informacion_Censos",
            "01_RENAMU_MUNICIPALIDADES",
            "*"
        )

        # Open all RENAMU files and creates a DataFrame

        df = pd.DataFrame()
        renamu_versions = {}

        for folder in glob.glob(path):
            if int(folder[-4:]) <= 2019:
                print('Current year: {}'.format(int(folder[-4:])))
                _df = pd.DataFrame()
                for subfolder in glob.glob(os.path.join(folder, '*')):
                    for filename in glob.glob(os.path.join(subfolder, '*')):
                        if filename.endswith('.sav') and filename not in ['C115.sav', 'C116.sav']:
                            temp = pd.read_spss(filename)
                            temp = temp.replace('', np.nan).dropna(how='all')
                            temp.rename(
                                columns = {
                                    'Ubigeo': 'district_id',
                                    'idimunici': 'district_id',
                                    'idmunici': 'district_id'
                                },
                                inplace = True
                            )

                            temp = temp.loc[:, temp.columns.isin(SELECTED_COLUMNS[int(folder[-4:])])].copy()
                            temp.replace({'21403': '021403', '240302,': '240302'}, inplace=True)
                            temp['district_id'] = temp['district_id'].str.replace('\\xa0', '')
                            temp.drop_duplicates(subset=['district_id'], inplace = True)

                            temp.dropna(subset=['district_id'], inplace=True)

                            if _df.shape[0] != 0:
                        
                                unique_districts = pd.DataFrame(_df['district_id'].unique(), columns=['district_id']).append(pd.DataFrame(temp['district_id'].unique(), columns=['district_id'])).drop_duplicates().reset_index(drop=True)
                        
                                _df = pd.merge(unique_districts, _df, on = 'district_id', suffixes = ('', '_drop'), how='left')
                                _df = pd.merge(_df, temp, on = 'district_id', suffixes = ('', '_drop'), how='left')
                                _df = _df.iloc[:, ~_df.columns.str.contains('_drop')]
                            else:
                                _df = temp


                _df['year'] = int(folder[-4:])

                if int(folder[-4:]) == 2019:
                    for item in VARIABLES_DICT[int(folder[-4:])]:
                        if item != '_RENAMU_138':
                            _df[item] = _df[item].replace(VARIABLES_DICT[int(folder[-4:])][item])

                    _df[[
                        'P34A_1', 'P34A_10', 'P34A_11', 'P34A_12', 'P34A_2', 'P34A_3', 'P34A_4', 'P34A_5', 'P34A_6', 'P34A_7', 
                        'P34A_8', 'P34A_9', 'P34A_13', 'P34A_14', 'P34A_15', 'P34A_16', 'P34A_17', 'P34A_18', 'P34A_19', 'P34A_20', 
                        'P34A_21', 'P34A_22', 'P34A_23', 'P34A_24', 'P34A_25', 'P34A_26', 'P34A_27', 'P34A_28', 'P34A_29', 'P34A_30',
                        'P58_1_1', 'P58_2_1', 'P58_3_1', 'P58_4_1', 'P59_1_1', 'P59_10_1', 'P59_11_1', 'P59_2_1', 'P59_3_1', 'P59_4_1', 
                        'P59_5_1', 'P59_6_1', 'P59_7_1', 'P59_8_1', 'P59_9_1'
            
                    ]] = _df[[
                        'P34A_1', 'P34A_10', 'P34A_11', 'P34A_12', 'P34A_2', 'P34A_3', 'P34A_4', 'P34A_5', 'P34A_6', 'P34A_7', 
                        'P34A_8', 'P34A_9', 'P34A_13', 'P34A_14', 'P34A_15', 'P34A_16', 'P34A_17', 'P34A_18', 'P34A_19', 'P34A_20', 
                        'P34A_21', 'P34A_22', 'P34A_23', 'P34A_24', 'P34A_25', 'P34A_26', 'P34A_27', 'P34A_28', 'P34A_29', 'P34A_30',
                        'P58_1_1', 'P58_2_1', 'P58_3_1', 'P58_4_1', 'P59_1_1', 'P59_10_1', 'P59_11_1', 'P59_2_1', 'P59_3_1', 'P59_4_1', 
                        'P59_5_1', 'P59_6_1', 'P59_7_1', 'P59_8_1', 'P59_9_1']].fillna(0)

                    _df['_RENAMU_41'] = _df['P34A_1'] +  _df['P34A_10'] +  _df['P34A_11'] + _df['P34A_12'] + _df['P34A_2'] + _df['P34A_3'] + _df['P34A_4'] + _df['P34A_5'] + _df['P34A_6'] + _df['P34A_7'] + _df['P34A_8'] + _df['P34A_9'] + _df['P34A_13'] +  _df['P34A_14'] +  _df['P34A_15']  +  _df['P34A_16'] + _df['P34A_17'] + _df['P34A_18'] + _df['P34A_19'] + _df['P34A_20'] + _df['P34A_21'] + _df['P34A_22'] + _df['P34A_23'] + _df['P34A_24'] + _df['P34A_25'] + _df['P34A_26'] + _df['P34A_27'] + _df['P34A_28'] + _df['P34A_29'] + _df['P34A_30']
                    _df['_RENAMU_42'] = _df['P34A_1'] +  _df['P34A_10'] +  _df['P34A_11'] + _df['P34A_12'] + _df['P34A_2'] + _df['P34A_3'] + _df['P34A_4'] + _df['P34A_5'] + _df['P34A_6'] + _df['P34A_7'] + _df['P34A_8'] + _df['P34A_9']
                    _df['_RENAMU_44'] = _df['P34A_13'] +  _df['P34A_14'] +  _df['P34A_15']
                    _df['_RENAMU_46'] = _df['P34A_16'] + _df['P34A_17'] + _df['P34A_18'] + _df['P34A_19'] + _df['P34A_20'] + _df['P34A_21']
                    _df['_RENAMU_48'] = _df['P34A_22'] + _df['P34A_23'] + _df['P34A_24'] + _df['P34A_25'] + _df['P34A_26'] + _df['P34A_27'] + _df['P34A_28'] + _df['P34A_29'] + _df['P34A_30']
                    _df['_RENAMU_50'] = _df['P58_1_1'] + _df['P58_2_1'] + _df['P58_3_1'] + _df['P58_4_1']
                    _df['_RENAMU_51'] = _df['P59_1_1'] + _df['P59_10_1'] + _df['P59_11_1'] + _df['P59_2_1'] + _df['P59_3_1'] + _df['P59_4_1'] + _df['P59_5_1'] + _df['P59_6_1'] + _df['P59_7_1'] + _df['P59_8_1'] + _df['P59_9_1']
                    _df['_RENAMU_53'] = _df['P70A_1_1'] + _df['P70A_2_1'] + _df['P70A_3_1'] + _df['P70A_4_1'] + _df['P70A_5_1']
                    _df['_RENAMU_54'] = _df['P70A_6_1'] + _df['P70A_7_1'] + _df['P70A_8_1']
                    _df['_RENAMU_55'] = _df['P11_1']
                    _df['_RENAMU_56'] = _df['P11_1']
                    _df['_RENAMU_81'] = _df.apply(lambda x: 1 if x['P23_1'] == 1 or x['P23_2'] == 1 or x['P23_3'] == 1 or x['P23_4'] == 1 or x['P23_5'] == 1 or x['P23_6'] == 1 or x['P23_7'] == 1 or x['P23_8'] == 1 or x['P23_9'] == 1 or x['P23_10'] == 1 or x['P23_11'] == 1 or x['P23_12'] == 1 or x['P23_13'] == 1 or x['P23_14'] == 1 else 0, axis=1)
                    _df['_RENAMU_91'] = _df.apply(lambda x: 1 if x['P33_1'] == 1 or x['P33_2'] == 1 else 0, axis=1)
                    _df['_RENAMU_134'] = _df.apply(lambda x: 1 if x['P51A'] == 1 or x['P51B'] == 1 or x['P51C'] == 1 or x['P51D'] == 1 or x['P51E'] == 1 or x['P51F'] == 1 else 0, axis=1)
                    _df['_RENAMU_138'] = _df['P53A']
                    _df['_RENAMU_140'] = _df['P56']
                    _df['_RENAMU_144'] = _df['P60A_1']
                    _df['_RENAMU_147'] = _df['P60A_2']
                    _df['_RENAMU_150'] = _df['P60A_3']

                elif int(folder[-4:]) == 2018:

                    _df[[
                        'P36A_1', 'P36A_2', 'P36A_3', 'P36A_4', 'P36A_5', 'P36A_6', 'P36A_7', 'P36A_8', 'P36A_9', 'P36A_10', 
                        'P36A_11', 'P36A_12', 'P36A_13', 'P36A_14', 'P36A_15', 'P36A_16', 'P36A_17', 'P36A_31', 'P36A_32', 'P36A_33', 
                        'P36A_34', 'P36A_35', 'P36A_36', 'P36A_18', 'P36A_19', 'P36A_20', 'P36A_21', 'P36A_22', 'P36A_23', 
                        'P36A_24', 'P36A_25', 'P36A_26', 'P36A_27', 'P36A_28', 'P36A_29', 'P36A_30', 'P62A_1_1', 'P62A_2_1', 
                        'P62A_3_1', 'P62A_4_1', 'P62A_5_1', 'P62A_6_1', 'P61A_1', 'P61A_2', 'P61A_3', 'P61A_4', 'P61A_5', 
                        'P61A_6', 'P61A_7', 'P61A_8', 'P61A_9', 'P61A_10', 'P61A_11'
                    ]] = _df[[
                        'P36A_1', 'P36A_2', 'P36A_3', 'P36A_4', 'P36A_5', 'P36A_6', 'P36A_7', 'P36A_8', 'P36A_9', 'P36A_10', 
                        'P36A_11', 'P36A_12', 'P36A_13', 'P36A_14', 'P36A_15', 'P36A_16', 'P36A_17', 'P36A_31', 'P36A_32', 'P36A_33', 
                        'P36A_34', 'P36A_35', 'P36A_36', 'P36A_18', 'P36A_19', 'P36A_20', 'P36A_21', 'P36A_22', 'P36A_23', 
                        'P36A_24', 'P36A_25', 'P36A_26', 'P36A_27', 'P36A_28', 'P36A_29', 'P36A_30', 'P62A_1_1', 'P62A_2_1', 
                        'P62A_3_1', 'P62A_4_1', 'P62A_5_1', 'P62A_6_1', 'P61A_1', 'P61A_2', 'P61A_3', 'P61A_4', 'P61A_5', 
                        'P61A_6', 'P61A_7', 'P61A_8', 'P61A_9', 'P61A_10', 'P61A_11'
                    ]].copy()

                    _df['_RENAMU_41'] = _df['P36A_1'] + _df['P36A_2'] + _df['P36A_3'] + _df['P36A_4'] + _df['P36A_5'] + _df['P36A_6'] + _df['P36A_7'] + _df['P36A_8'] + _df['P36A_9'] + _df['P36A_10'] + _df['P36A_11'] + _df['P36A_12'] + _df['P36A_13'] + _df['P36A_14'] + _df['P36A_15'] + _df['P36A_16'] + _df['P36A_17'] + _df['P36A_31'] + _df['P36A_32'] + _df['P36A_33'] + _df['P36A_34'] + _df['P36A_35'] + _df['P36A_36'] + _df['P36A_18'] + _df['P36A_19'] + _df['P36A_20'] + _df['P36A_21'] + _df['P36A_22'] + _df['P36A_23'] + _df['P36A_24'] + _df['P36A_25'] + _df['P36A_26'] + _df['P36A_27'] + _df['P36A_28'] + _df['P36A_29'] + _df['P36A_30']
                    _df['_RENAMU_42'] = _df['P36A_1'] + _df['P36A_2'] + _df['P36A_3'] + _df['P36A_4'] + _df['P36A_5'] + _df['P36A_6'] + _df['P36A_7'] + _df['P36A_8'] + _df['P36A_9'] + _df['P36A_10'] + _df['P36A_11'] + _df['P36A_12'] + _df['P36A_13'] + _df['P36A_14'] + _df['P36A_15'] + _df['P36A_16'] + _df['P36A_17']
                    _df['_RENAMU_44'] = _df['P36A_31'] + _df['P36A_32'] + _df['P36A_33'] + _df['P36A_34'] + _df['P36A_35'] + _df['P36A_36']
                    _df['_RENAMU_46'] = _df['P36A_18'] + _df['P36A_19']
                    _df['_RENAMU_48'] = _df['P36A_20'] + _df['P36A_21'] + _df['P36A_22'] + _df['P36A_23'] + _df['P36A_24'] + _df['P36A_25'] + _df['P36A_26'] + _df['P36A_27'] + _df['P36A_28'] + _df['P36A_29'] + _df['P36A_30']
                    _df['_RENAMU_50'] = _df['P62A_1_1'] + _df['P62A_2_1'] + _df['P62A_3_1'] + _df['P62A_4_1'] + _df['P62A_5_1'] + _df['P62A_6_1']
                    _df['_RENAMU_51'] = _df['P61A_1'] + _df['P61A_2'] + _df['P61A_3'] + _df['P61A_4'] + _df['P61A_5'] + _df['P61A_6'] + _df['P61A_7'] + _df['P61A_8'] + _df['P61A_9'] + _df['P61A_10'] + _df['P61A_11']
                    _df['_RENAMU_53'] = _df['P73A_1_1'] + _df['P73A_2_1'] + _df['P73A_3_1'] + _df['P73A_4_1'] + _df['P73A_5_1']
                    _df['_RENAMU_54'] = _df['P73A_11_1'] + _df['P73A_12_1'] + _df['P73A_13_1']
                    _df['_RENAMU_55'] = np.nan
                    _df['_RENAMU_56'] = np.nan
                    _df['_RENAMU_81'] = _df.apply(lambda x: 1 if x['P23_1'] == 1 or x['P23_2'] == 1 or x['P23_3'] == 1 or x['P23_4'] == 1 or x['P23_5'] == 1 or x['P23_6'] == 1 or x['P23_7'] == 1 or x['P23_8'] == 1 or x['P23_9'] == 1 or x['P23_10'] == 1 or x['P23_11'] == 1 or x['P23_12'] == 1 or x['P23_13'] == 1 or x['P23_14'] == 1 else 0, axis=1)
                    _df['_RENAMU_91'] = _df.apply(lambda x: 1 if x['P34_1'] == 1 or x['P34_2'] == 1 else 0, axis=1)
                    _df['_RENAMU_134'] = _df.apply(lambda x: 1 if x['P53_1'] == 'Sí' or x['P53_2'] == 'Sí' or x['P53_3'] == 'Sí' or x['P53_4'] == 'Sí' or x['P53_5'] == 'Sí' or x['P53_6'] == 'Sí' else 0, axis=1)
                    _df['_RENAMU_138'] = _df['P55A']
                    _df['_RENAMU_140'] = _df.apply(lambda x: 1 if x['P59_T'] > 0 else 0 if x['P59_T'] == 0 else np.nan, axis=1)
                    _df['_RENAMU_144'] = _df.apply(lambda x: 1 if x['P63_1_1'] > 0 else 0 if x['P63_1_1'] == 0 else np.nan, axis=1)
                    _df['_RENAMU_147'] = _df.apply(lambda x: 1 if x['P63_2_1'] > 0 else 0 if x['P63_2_1'] == 0 else np.nan, axis=1)
                    _df['_RENAMU_150'] = _df.apply(lambda x: 1 if x['P63_3_1'] > 0 else 0 if x['P63_3_1'] == 0 else np.nan, axis=1)

                elif int(folder[-4:]) == 2017:

                    _df[[
                        'P39A_1', 'P39A_2', 'P39A_3', 'P39A_4', 'P39A_5', 'P39A_6', 'P39A_7', 'P39A_8', 'P39A_9', 'P39A_10', 
                        'P39A_11', 'P39A_12', 'P39A_13', 'P39A_14', 'P39A_15', 'P39A_16', 'P39A_17', 'P39A_31', 'P39A_32', 'P39A_33', 
                        'P39A_34', 'P39A_35', 'P39A_36', 'P39A_18', 'P39A_19', 'P39A_20', 'P39A_21', 'P39A_22', 'P39A_23', 
                        'P39A_24', 'P39A_25', 'P39A_26', 'P39A_27', 'P39A_28', 'P39A_29', 'P39A_30', 'P39A_31', 'P39A_32', 
                        'P39A_33', 'P39A_34', 'P39A_35', 'P39A_36', 
                        'P65A_1_1', 'P65A_2_1', 
                        'P65A_3_1', 'P65A_4_1', 'P65A_5_1', 'P65A_6_1', 'P64A_1', 'P64A_2', 'P64A_3', 'P64A_4', 'P64A_5', 
                        'P64A_6', 'P64A_7', 'P64A_8', 'P64A_9', 'P64A_10', 'P64A_11'
                    ]] = _df[[
                        'P39A_1', 'P39A_2', 'P39A_3', 'P39A_4', 'P39A_5', 'P39A_6', 'P39A_7', 'P39A_8', 'P39A_9', 'P39A_10', 
                        'P39A_11', 'P39A_12', 'P39A_13', 'P39A_14', 'P39A_15', 'P39A_16', 'P39A_17', 'P39A_31', 'P39A_32', 'P39A_33', 
                        'P39A_34', 'P39A_35', 'P39A_36', 'P39A_18', 'P39A_19', 'P39A_20', 'P39A_21', 'P39A_22', 'P39A_23', 
                        'P39A_24', 'P39A_25', 'P39A_26', 'P39A_27', 'P39A_28', 'P39A_29', 'P39A_30', 'P39A_31', 'P39A_32', 
                        'P39A_33', 'P39A_34', 'P39A_35', 'P39A_36', 
                        'P65A_1_1', 'P65A_2_1', 
                        'P65A_3_1', 'P65A_4_1', 'P65A_5_1', 'P65A_6_1', 'P64A_1', 'P64A_2', 'P64A_3', 'P64A_4', 'P64A_5', 
                        'P64A_6', 'P64A_7', 'P64A_8', 'P64A_9', 'P64A_10', 'P64A_11'
                    ]].copy()

                    _df['_RENAMU_41'] = _df['P39A_1'] + _df['P39A_2'] + _df['P39A_3'] + _df['P39A_4'] + _df['P39A_5'] + _df['P39A_6'] + _df['P39A_7'] + _df['P39A_8'] + _df['P39A_9'] + _df['P39A_10'] + _df['P39A_11'] + _df['P39A_12'] + _df['P39A_13'] + _df['P39A_14'] + _df['P39A_15'] + _df['P39A_16'] + _df['P39A_17'] + _df['P39A_31'] + _df['P39A_32'] + _df['P39A_33'] + _df['P39A_34'] + _df['P39A_35'] + _df['P39A_36'] + _df['P39A_18'] + _df['P39A_19'] + _df['P39A_20'] + _df['P39A_21'] + _df['P39A_22'] + _df['P39A_23'] + _df['P39A_24'] + _df['P39A_25'] + _df['P39A_26'] + _df['P39A_27'] + _df['P39A_28'] + _df['P39A_29'] + _df['P39A_30']
                    _df['_RENAMU_42'] = _df['P39A_1'] + _df['P39A_2'] + _df['P39A_3'] + _df['P39A_4'] + _df['P39A_5'] + _df['P39A_6'] + _df['P39A_7'] + _df['P39A_8'] + _df['P39A_9'] + _df['P39A_10'] + _df['P39A_11'] + _df['P39A_12'] + _df['P39A_13'] + _df['P39A_14'] + _df['P39A_15'] + _df['P39A_16'] + _df['P39A_17']
                    _df['_RENAMU_44'] = _df['P39A_31'] + _df['P39A_32'] + _df['P39A_33'] + _df['P39A_34'] + _df['P39A_35'] + _df['P39A_36']
                    _df['_RENAMU_46'] = _df['P39A_18'] + _df['P39A_19']
                    _df['_RENAMU_48'] = _df['P39A_20'] + _df['P39A_21'] + _df['P39A_22'] + _df['P39A_23'] + _df['P39A_24'] + _df['P39A_25'] + _df['P39A_26'] + _df['P39A_27'] + _df['P39A_28'] + _df['P39A_29'] + _df['P39A_30']
                    _df['_RENAMU_50'] = _df['P65A_1_1'] + _df['P65A_2_1'] + _df['P65A_3_1'] + _df['P65A_4_1'] + _df['P65A_5_1'] + _df['P65A_6_1']
                    _df['_RENAMU_51'] = _df['P64A_1'] + _df['P64A_2'] + _df['P64A_3'] + _df['P64A_4'] + _df['P64A_5'] + _df['P64A_6'] + _df['P64A_7'] + _df['P64A_8'] + _df['P64A_9'] + _df['P64A_10'] + _df['P64A_11']
                    _df['_RENAMU_53'] = _df['P76A_1_1'] + _df['P76A_2_1'] + _df['P76A_3_1'] + _df['P76A_4_1'] + _df['P76A_5_1'] + _df['P76_6_1']
                    _df['_RENAMU_54'] = _df['P76A_12_1'] + _df['P76A_13_1'] + _df['P76A_14_1']
                    _df['_RENAMU_55'] = np.nan
                    _df['_RENAMU_56'] = np.nan
                    _df['_RENAMU_81'] = _df.apply(lambda x: 1 if x['P25_1'] == 1 or x['P25_2'] == 1 or x['P25_3'] == 1 or x['P25_4'] == 1 or x['P25_5'] == 1 or x['P25_6'] == 1 or x['P25_7'] == 1 or x['P25_8'] == 1 or x['P25_9'] == 1 or x['P25_10'] == 1 or x['P25_11'] == 1 or x['P25_12'] == 1 or x['P25_13'] == 1 or x['P25_14'] == 1 or x['P25_15'] == 1 or x['P25_16'] == 1 else 0, axis=1)
                    _df['_RENAMU_91'] = _df.apply(lambda x: 1 if x['P37'] == 1 or x['P37'] == 2 else 0, axis=1)
                    _df['_RENAMU_134'] = _df.apply(lambda x: 1 if x['P55_1'] == 'Sí' or x['P55_2'] == 'Sí' or x['P55_3'] == 'Sí' or x['P55_4'] == 'Sí' or x['P55_5'] == 'Sí' or x['P55_6'] == 'Sí' else 0, axis=1)
                    _df['_RENAMU_138'] = _df['P59']
                    _df['_RENAMU_140'] = _df.apply(lambda x: 1 if x['P62_T'] > 0 else 0 if x['P62_T'] == 0 else np.nan, axis=1)
                    _df['_RENAMU_144'] = _df['P66_1']
                    _df['_RENAMU_147'] = _df['P66_2']
                    _df['_RENAMU_150'] = _df['P66_3']

                elif int(folder[-4:]) == 2016:

                    _df[[
                        'P39A_1', 'P39A_2', 'P39A_3', 'P39A_4', 'P39A_5', 'P36A_6', 'P39A_7', 'P39A_8', 'P39A_9', 'P39A_10', 
                        'P39A_11', 'P39A_12', 'P39A_13', 'P39A_14', 'P39A_15', 'P39A_16', 'P39A_17', 'P39A_31', 'P39A_32', 'P39A_33', 
                        'P39A_34', 'P39A_35', 'P39A_36', 'P39A_18', 'P39A_19', 'P39A_20', 'P39A_21', 'P39A_22', 'P39A_23', 
                        'P39A_24', 'P39A_25', 'P39A_26', 'P39A_27', 'P39A_28', 'P39A_29', 'P39A_30', 'P39A_31', 'P39A_32', 
                        'P39A_33', 'P39A_34', 'P39A_35', 'P39A_36', 
                        'P65A_1_1', 'P65A_2_1', 
                        'P65A_3_1', 'P65A_4_1', 'P65A_5_1', 'P65A_6_1', 'P64A_1', 'P64A_2', 'P64A_3', 'P64A_4', 'P64A_5', 
                        'P64A_6', 'P64A_7', 'P64A_8', 'P64A_9', 'P64A_10', 'P64A_11'
                    ]] = _df[[
                        'P39A_1', 'P39A_2', 'P39A_3', 'P39A_4', 'P39A_5', 'P36A_6', 'P39A_7', 'P39A_8', 'P39A_9', 'P39A_10', 
                        'P39A_11', 'P39A_12', 'P39A_13', 'P39A_14', 'P39A_15', 'P39A_16', 'P39A_17', 'P39A_31', 'P39A_32', 'P39A_33', 
                        'P39A_34', 'P39A_35', 'P39A_36', 'P39A_18', 'P39A_19', 'P39A_20', 'P39A_21', 'P39A_22', 'P39A_23', 
                        'P39A_24', 'P39A_25', 'P39A_26', 'P39A_27', 'P39A_28', 'P39A_29', 'P39A_30', 'P39A_31', 'P39A_32', 
                        'P39A_33', 'P39A_34', 'P39A_35', 'P39A_36', 
                        'P65A_1_1', 'P65A_2_1', 
                        'P65A_3_1', 'P65A_4_1', 'P65A_5_1', 'P65A_6_1', 'P64A_1', 'P64A_2', 'P64A_3', 'P64A_4', 'P64A_5', 
                        'P64A_6', 'P64A_7', 'P64A_8', 'P64A_9', 'P64A_10', 'P64A_11'
                    ]].copy()

                    _df['_RENAMU_41'] = _df['P39A_1'] + _df['P39A_2'] + _df['P39A_3'] + _df['P39A_4'] + _df['P39A_5'] + _df['P36A_6'] + _df['P39A_7'] + _df['P39A_8'] + _df['P39A_9'] + _df['P39A_10'] + _df['P39A_11'] + _df['P39A_12'] + _df['P39A_13'] + _df['P39A_14'] + _df['P39A_15'] + _df['P39A_16'] + _df['P39A_17'] + _df['P39A_31'] + _df['P39A_32'] + _df['P39A_33'] + _df['P39A_34'] + _df['P39A_35'] + _df['P39A_36'] + _df['P39A_18'] + _df['P39A_19'] + _df['P39A_20'] + _df['P39A_21'] + _df['P39A_22'] + _df['P39A_23'] + _df['P39A_24'] + _df['P39A_25'] + _df['P39A_26'] + _df['P39A_27'] + _df['P39A_28'] + _df['P39A_29'] + _df['P39A_30']
                    _df['_RENAMU_42'] = _df['P39A_1'] + _df['P39A_2'] + _df['P39A_3'] + _df['P39A_4'] + _df['P39A_5'] + _df['P36A_6'] + _df['P39A_7'] + _df['P39A_8'] + _df['P39A_9'] + _df['P39A_10'] + _df['P39A_11'] + _df['P39A_12'] + _df['P39A_13'] + _df['P39A_14'] + _df['P39A_15'] + _df['P39A_16'] + _df['P39A_17']
                    _df['_RENAMU_44'] = _df['P39A_31'] + _df['P39A_32'] + _df['P39A_33'] + _df['P39A_34'] + _df['P39A_35'] + _df['P39A_36']
                    _df['_RENAMU_46'] = _df['P39A_18'] + _df['P39A_19']
                    _df['_RENAMU_48'] = _df['P39A_20'] + _df['P39A_21'] + _df['P39A_22'] + _df['P39A_23'] + _df['P39A_24'] + _df['P39A_25'] + _df['P39A_26'] + _df['P39A_27'] + _df['P39A_28'] + _df['P39A_29'] + _df['P39A_30']
                    _df['_RENAMU_50'] = _df['P65A_1_1'] + _df['P65A_2_1'] + _df['P65A_3_1'] + _df['P65A_4_1'] + _df['P65A_5_1'] + _df['P65A_6_1']
                    _df['_RENAMU_51'] = _df['P64A_1'] + _df['P64A_2'] + _df['P64A_3'] + _df['P64A_4'] + _df['P64A_5'] + _df['P64A_6'] + _df['P64A_7'] + _df['P64A_8'] + _df['P64A_9'] + _df['P64A_10'] + _df['P64A_11']
                    _df['_RENAMU_53'] = _df['P76A_1_1'] + _df['P76A_2_1'] + _df['P76A_3_1']
                    _df['_RENAMU_54'] = _df['P76A_4_1'] + _df['P76A_5_1'] + _df['P76A_6_1'] + _df['P76A_7_1'] + _df['P76_8_1']
                    _df['_RENAMU_55'] = np.nan
                    _df['_RENAMU_56'] = np.nan
                    _df['_RENAMU_81'] = _df.apply(lambda x: 1 if x['P25_1'] == 1 or x['P25_2'] == 1 or x['P25_3'] == 1 or x['P25_4'] == 1 or x['P25_5'] == 1 or x['P25_6'] == 1 or x['P25_7'] == 1 or x['P25_8'] == 1 or x['P25_9'] == 1 or x['P25_10'] == 1 or x['P25_11'] == 1 or x['P25_12'] == 1 or x['P25_13'] == 1 or x['P25_14'] == 1 or x['P25_15'] == 1 or x['P25_16'] == 1 or x['P25_17'] == 1 else 0, axis=1)
                    _df['_RENAMU_91'] = _df.apply(lambda x: 1 if x['P37'] == 1 or x['P37'] == 2 else 0, axis=1)
                    _df['_RENAMU_134'] = _df.apply(lambda x: 1 if x['P55_1'] == 'Si' or x['P55_2'] == 'Si' or x['P55_3'] == 'Si' or x['P55_4'] == 'Si' or x['P55_5'] == 'Si' or x['P55_6'] == 'Si' else 0, axis=1)
                    _df['_RENAMU_138'] = _df.apply(lambda x: 1 if x['P59_1'] == 1 or x['P59_2'] == 1 or x['P59_3'] == 1 or x['P59_4'] == 1 or x['P59_5'] == 1 or x['P59_6'] == 1 or x['P59_7'] == 1 or x['P59_8'] == 1 or x['P59_9'] == 1 or x['P59_10'] == 1 or x['P59_11'] == 1 or x['P59_12'] == 1 or x['P59_13'] == 1 or x['P59_14'] == 1 else 0, axis=1)
                    _df['_RENAMU_140'] = _df.apply(lambda x: 1 if x['P62'] == 'Tiene' else 0 if x['P62'] == 'No tiene' else np.nan, axis=1)
                    _df['_RENAMU_144'] = _df['P66_1']
                    _df['_RENAMU_147'] = _df['P66_2']
                    _df['_RENAMU_150'] = _df['P66_3']

                elif int(folder[-4:]) == 2015:

                    _df[[
                        'P39A_1', 'P39A_2', 'P39A_3', 'P39A_4', 'P39A_5', 'P36A_6', 'P39A_7', 'P39A_8', 'P39A_9', 'P39A_10', 
                        'P39A_11', 'P39A_12', 'P39A_13', 'P39A_14', 'P39A_15', 'P39A_16', 'P39A_17', 'P39A_31', 'P39A_32', 'P39A_33', 
                        'P39A_34', 'P39A_35', 'P39A_36', 'P39A_18', 'P39A_19', 'P39A_20', 'P39A_21', 'P39A_22', 'P39A_23', 
                        'P39A_24', 'P39A_25', 'P39A_26', 'P39A_27', 'P39A_28', 'P39A_29', 'P39A_30', 'P39A_31', 'P39A_32', 
                        'P39A_33', 'P39A_34', 'P39A_35', 'P39A_36', 
                        'P65A_1_1', 'P65A_2_1', 
                        'P65A_3_1', 'P65A_4_1', 'P65A_5_1', 'P65A_6_1', 'P64A_1', 'P64A_2', 'P64A_3', 'P64A_4', 'P64A_5', 
                        'P64A_6', 'P64A_7', 'P64A_8', 'P64A_9', 'P64A_10', 'P64A_11'
                    ]] = _df[[
                        'P39A_1', 'P39A_2', 'P39A_3', 'P39A_4', 'P39A_5', 'P36A_6', 'P39A_7', 'P39A_8', 'P39A_9', 'P39A_10', 
                        'P39A_11', 'P39A_12', 'P39A_13', 'P39A_14', 'P39A_15', 'P39A_16', 'P39A_17', 'P39A_31', 'P39A_32', 'P39A_33', 
                        'P39A_34', 'P39A_35', 'P39A_36', 'P39A_18', 'P39A_19', 'P39A_20', 'P39A_21', 'P39A_22', 'P39A_23', 
                        'P39A_24', 'P39A_25', 'P39A_26', 'P39A_27', 'P39A_28', 'P39A_29', 'P39A_30', 'P39A_31', 'P39A_32', 
                        'P39A_33', 'P39A_34', 'P39A_35', 'P39A_36', 
                        'P65A_1_1', 'P65A_2_1', 
                        'P65A_3_1', 'P65A_4_1', 'P65A_5_1', 'P65A_6_1', 'P64A_1', 'P64A_2', 'P64A_3', 'P64A_4', 'P64A_5', 
                        'P64A_6', 'P64A_7', 'P64A_8', 'P64A_9', 'P64A_10', 'P64A_11'
                    ]].copy()

                    _df['_RENAMU_41'] = _df['P39A_1'] + _df['P39A_2'] + _df['P39A_3'] + _df['P39A_4'] + _df['P39A_5'] + _df['P36A_6'] + _df['P39A_7'] + _df['P39A_8'] + _df['P39A_9'] + _df['P39A_10'] + _df['P39A_11'] + _df['P39A_12'] + _df['P39A_13'] + _df['P39A_14'] + _df['P39A_15'] + _df['P39A_16'] + _df['P39A_17'] + _df['P39A_31'] + _df['P39A_32'] + _df['P39A_33'] + _df['P39A_34'] + _df['P39A_35'] + _df['P39A_36'] + _df['P39A_18'] + _df['P39A_19'] + _df['P39A_20'] + _df['P39A_21'] + _df['P39A_22'] + _df['P39A_23'] + _df['P39A_24'] + _df['P39A_25'] + _df['P39A_26'] + _df['P39A_27'] + _df['P39A_28'] + _df['P39A_29'] + _df['P39A_30']
                    _df['_RENAMU_42'] = _df['P39A_1'] + _df['P39A_2'] + _df['P39A_3'] + _df['P39A_4'] + _df['P39A_5'] + _df['P36A_6'] + _df['P39A_7'] + _df['P39A_8'] + _df['P39A_9'] + _df['P39A_10'] + _df['P39A_11'] + _df['P39A_12'] + _df['P39A_13'] + _df['P39A_14'] + _df['P39A_15'] + _df['P39A_16'] + _df['P39A_17']
                    _df['_RENAMU_44'] = _df['P39A_31'] + _df['P39A_32'] + _df['P39A_33'] + _df['P39A_34'] + _df['P39A_35'] + _df['P39A_36']
                    _df['_RENAMU_46'] = _df['P39A_18'] + _df['P39A_19']
                    _df['_RENAMU_48'] = _df['P39A_20'] + _df['P39A_21'] + _df['P39A_22'] + _df['P39A_23'] + _df['P39A_24'] + _df['P39A_25'] + _df['P39A_26'] + _df['P39A_27'] + _df['P39A_28'] + _df['P39A_29'] + _df['P39A_30']
                    _df['_RENAMU_50'] = _df['P65A_1_1'] + _df['P65A_2_1'] + _df['P65A_3_1'] + _df['P65A_4_1'] + _df['P65A_5_1'] + _df['P65A_6_1']
                    _df['_RENAMU_51'] = _df['P64A_1'] + _df['P64A_2'] + _df['P64A_3'] + _df['P64A_4'] + _df['P64A_5'] + _df['P64A_6'] + _df['P64A_7'] + _df['P64A_8'] + _df['P64A_9'] + _df['P64A_10'] + _df['P64A_11']
                    _df['_RENAMU_53'] = _df['P82A_1_1'] + _df['P82A_2_1'] + _df['P82A_3_1']
                    _df['_RENAMU_54'] = _df['P82A_4_1'] + _df['P82A_5_1'] + _df['P82A_6_1'] + _df['P82A_7_1'] + _df['P82A_O_1']
                    _df['_RENAMU_55'] = np.nan
                    _df['_RENAMU_56'] = np.nan
                    _df['_RENAMU_81'] = _df.apply(lambda x: 1 if x['P25_1'] == 1 or x['P25_2'] == 1 or x['P25_3'] == 1 or x['P25_4'] == 1 or x['P25_5'] == 1 or x['P25_6'] == 1 or x['P25_7'] == 1 or x['P25_8'] == 1 or x['P25_9'] == 1 or x['P25_10'] == 1 or x['P25_11'] == 1 or x['P25_12'] == 1 or x['P25_13'] == 1 or x['P25_14'] == 1 or x['P25_15'] == 1 or x['P25_16'] == 1 or x['P25_17'] == 1 else 0, axis=1)
                    _df['_RENAMU_91'] = _df.apply(lambda x: 1 if x['P37'] == 1 else 0, axis=1)
                    _df['_RENAMU_134'] = _df.apply(lambda x: 1 if x['P55_1'] == 'Si' or x['P55_2'] == 'Si' or x['P55_3'] == 'Si' or x['P55_4'] == 'Si' or x['P55_5'] == 'Si' else 0, axis=1)
                    _df['_RENAMU_138'] = _df.apply(lambda x: 1 if x['P59_1'] == 1 or x['P59_2'] == 1 or x['P59_3'] == 1 or x['P59_4'] == 1 or x['P59_5'] == 1 or x['P59_6'] == 1 or x['P59_7'] == 1 or x['P59_8'] == 1 or x['P59_9'] == 1 or x['P59_10'] == 1 or x['P59_11'] == 1 or x['P59_12'] == 1 or x['P59_13'] == 1 or x['P59_14'] == 1 else 0, axis=1)
                    _df['_RENAMU_140'] = _df.apply(lambda x: 1 if x['P62'] == 'Si dispone' else 0 if x['P62'] == 'No dispone' else np.nan, axis=1)
                    _df['_RENAMU_144'] = _df['P66_1']
                    _df['_RENAMU_147'] = _df['P66_2']
                    _df['_RENAMU_150'] = _df['P66_3']

                if int(folder[-4:]) < 2019:
                    for item in VARIABLES_DICT[int(folder[-4:])]:
                        _df[item] = _df[item].replace(VARIABLES_DICT[int(folder[-4:])][item])
                
                    _df.rename(columns=COLUMNS_DICT[int(folder[-4:])], inplace=True)

                renamu_versions[int(folder[-4:])] = _df

        for item in renamu_versions:
            df = df.append(renamu_versions[item], sort=True)


        # Creates aditional geo levels

        df['nation_id'] = 'per'
        df['department_id'] = df['district_id'].str[0:2]
        df['province_id'] = df['district_id'].str[0:4]

        # Generates count column to aggregate values

        df['count'] = 1
        df = df.fillna(0)

        print(df['P52'].unique())

        # Creates auxiliar columns to aggregate special values

        df['_P41_1'] = df.apply(lambda x: x['P41_1'] if x['year'] != 2018 else x['_P41_1'], axis=1) 
        df['_P43_1'] = df.apply(lambda x: x['P43_1'] if x['year'] != 2018 else x['_P43_1'], axis=1) 
        df['_P62'] = df.apply(lambda x: x['P62'] if x['year'] not in [2015, 2016] else x['_P62'], axis=1) 
        # df['P33_AUX'] = df.apply(lambda x: 1 if x['P33_1'] == 1 or x['P33_2'] == 1 else 0, axis=1)
        df['P41_1_1_AUX'] = df.apply(lambda x: 1 if x['_P41_1'] == 1 else 0, axis=1)
        df['P41_1_2_AUX'] = df.apply(lambda x: 1 if x['_P41_1'] == 2 else 0, axis=1)
        df['P41_1_3_AUX'] = df.apply(lambda x: 1 if x['_P41_1'] == 3 else 0, axis=1)
        df['P41_1_4_AUX'] = df.apply(lambda x: 1 if x['_P41_1'] == 4 else 0, axis=1)
        df['P43_1_1_AUX'] = df.apply(lambda x: 1 if x['_P43_1'] <= 1 else 0, axis=1)
        df['P43_1_2_AUX'] = df.apply(lambda x: 1 if x['_P43_1'] == 2 else 0, axis=1)
        df['P43_1_3_AUX'] = df.apply(lambda x: 1 if x['_P43_1'] == 3 else 0, axis=1)
        df['P43_1_4_AUX'] = df.apply(lambda x: 1 if x['_P43_1'] == 4 else 0, axis=1)
        # df['P51_AUX'] = df.apply(lambda x: 1 if x['P51A'] == 1 or x['P51B'] == 1 or x['P51C'] == 1 or x['P51D'] == 1 or x['P51E'] == 1 or x['P51F'] == 1 else 0, axis=1)
        # df['P23_AUX'] = df.apply(lambda x: 1 if x['P23_1'] == 1 or x['P23_2'] == 1 or x['P23_3'] == 1 or x['P23_4'] == 1 or x['P23_5'] == 1 or x['P23_6'] == 1 or x['P23_7'] == 1 or x['P23_8'] == 1 or x['P23_9'] == 1 or x['P23_10'] == 1 or x['P23_11'] == 1 or x['P23_12'] == 1 or x['P23_13'] == 1 or x['P23_14'] == 1 else 0, axis=1)
        df['P19_AUX'] = df.apply(lambda x: 1 if x['P19'] == 1 else 0, axis=1)
        df['P13_1_AUX'] = df.apply(lambda x: 1 if x['P13_1'] == 1 else 0, axis=1)

        # Drops departments rows at district level

        df = df[~df['district_id'].isin(['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '1,', '|', 'Fuente: Instituto', 'Fuente: Inst', 'Fuente', 'Fuente: Instituto Nacional de Estadística e Informática - Registro Nacional de Municipalidades 2015.'])]

        # Creates aggregated DataFrames for provinces, departments and nation levels

        df_province = df.drop(columns=['district_id']).groupby(by=['province_id', 'year']).sum().reset_index()
        df_department = df.drop(columns=['district_id']).groupby(by=['department_id', 'year']).sum().reset_index()
        df_nation = df.drop(columns=['district_id']).groupby(by=['nation_id', 'year']).sum().reset_index()

        # Calculates district level indicators

        output_district = df[['district_id', 'year']].copy()
        output_district['RENAMU_1'] = df['P14A_1'] + df['P14A_2'] + df['P14A_3'] + df['P14A_4'] + df['P14A_5'] + df['P14A_6'] + df['P14A_7'] + df['P14A_8'] + df['P14A_9']
        output_district['RENAMU_2'] = df.apply(lambda x: 1 if x['P18_1'] == 1 else 0 if x['P18_1'] == 0 else np.nan, axis=1)
        output_district['RENAMU_3'] = np.nan
        output_district['RENAMU_4'] = df.apply(lambda x: 1 if x['P18_10'] == 1 else 0 if x['P18_10'] == 0 else np.nan, axis=1)
        output_district['RENAMU_5'] = np.nan
        output_district['RENAMU_6'] = df.apply(lambda x: 1 if x['P18_11'] == 1 else 0 if x['P18_11'] == 0 else np.nan, axis=1)
        output_district['RENAMU_7'] = np.nan
        output_district['RENAMU_8'] = df.apply(lambda x: 1 if x['P18_12'] == 1 else 0 if x['P18_12'] == 0 else np.nan, axis=1)
        output_district['RENAMU_9'] = np.nan
        output_district['RENAMU_10'] = df.apply(lambda x: 1 if x['P18_2'] == 1 else 0 if x['P18_2'] == 0 else np.nan, axis=1)
        output_district['RENAMU_11'] = np.nan
        output_district['RENAMU_12'] = df.apply(lambda x: 1 if x['P18_3'] == 1 else 0 if x['P18_3'] == 0 else np.nan, axis=1)
        output_district['RENAMU_13'] = np.nan
        output_district['RENAMU_14'] = df.apply(lambda x: 1 if x['P18_4'] == 1 else 0 if x['P18_4'] == 0 else np.nan, axis=1)
        output_district['RENAMU_15'] = np.nan
        output_district['RENAMU_16'] = df.apply(lambda x: 1 if x['P18_5'] == 1 else 0 if x['P18_5'] == 0 else np.nan, axis=1)
        output_district['RENAMU_17'] = np.nan
        output_district['RENAMU_18'] = df.apply(lambda x: 1 if x['P18_6'] == 1 else 0 if x['P18_6'] == 0 else np.nan, axis=1)
        output_district['RENAMU_19'] = np.nan
        output_district['RENAMU_20'] = df.apply(lambda x: 1 if x['P18_7'] == 1 else 0 if x['P18_7'] == 0 else np.nan, axis=1)
        output_district['RENAMU_21'] = np.nan
        output_district['RENAMU_22'] = df.apply(lambda x: 1 if x['P18_8'] == 1 else 0 if x['P18_8'] == 0 else np.nan, axis=1)
        output_district['RENAMU_23'] = np.nan
        output_district['RENAMU_24'] = df.apply(lambda x: 1 if x['P18_9'] == 1 else 0 if x['P18_9'] == 0 else np.nan, axis=1)
        output_district['RENAMU_25'] = np.nan
        output_district['RENAMU_26'] = df.apply(lambda x: 1 if x['P19'] == 1 else 0 if x['P19'] in [2, 3, 4] else np.nan, axis=1)
        output_district['RENAMU_27'] = np.nan
        output_district['RENAMU_28'] = df['P20_1_T']
        output_district['RENAMU_29'] = df['P20_1_T'] * 100 / (df['P20_1_T'] + df['P20_2_T'] + df['P20_3_T'] + df['P20_4_T'] + df['P20_5_T'] + df['P20_6_T'])
        output_district['RENAMU_30'] = df['P20_2_T']
        output_district['RENAMU_31'] = df['P20_2_T'] * 100 / (df['P20_1_T'] + df['P20_2_T'] + df['P20_3_T'] + df['P20_4_T'] + df['P20_5_T'] + df['P20_6_T'])
        output_district['RENAMU_32'] = df['P20_3_T']
        output_district['RENAMU_33'] = df['P20_3_T'] * 100 / (df['P20_1_T'] + df['P20_2_T'] + df['P20_3_T'] + df['P20_4_T'] + df['P20_5_T'] + df['P20_6_T'])
        output_district['RENAMU_34'] = df['P20_4_T']
        output_district['RENAMU_35'] = df['P20_4_T'] * 100 / (df['P20_1_T'] + df['P20_2_T'] + df['P20_3_T'] + df['P20_4_T'] + df['P20_5_T'] + df['P20_6_T'])
        output_district['RENAMU_36'] = df['P20_5_T'] + df['P20_6_T']
        output_district['RENAMU_37'] = (df['P20_5_T'] + df['P20_6_T']) * 100 / (df['P20_1_T'] + df['P20_2_T'] + df['P20_3_T'] + df['P20_4_T'] + df['P20_5_T'] + df['P20_6_T'])
        output_district['RENAMU_38'] = df['P20_1_T'] + df['P20_2_T'] + df['P20_3_T'] + df['P20_4_T'] + df['P20_5_T'] + df['P20_6_T']
        output_district['RENAMU_39'] = df['P32_2_T']
        output_district['RENAMU_40'] = df['P32_2_T'] * 100 / (df['P20_1_T'] + df['P20_2_T'] + df['P20_3_T'] + df['P20_4_T'] + df['P20_5_T'] + df['P20_6_T'])
        output_district['RENAMU_41'] = df['_RENAMU_41']
        output_district['RENAMU_42'] = df['_RENAMU_42']
        output_district['RENAMU_43'] = df['_RENAMU_42'] * 100 / df['_RENAMU_41']
        output_district['RENAMU_44'] = df['_RENAMU_44']
        output_district['RENAMU_45'] = df['_RENAMU_44'] * 100 / df['_RENAMU_41']
        output_district['RENAMU_46'] = df['_RENAMU_46']
        output_district['RENAMU_47'] = df['_RENAMU_46'] * 100 / df['_RENAMU_41']
        output_district['RENAMU_48'] = df['_RENAMU_48']
        output_district['RENAMU_49'] = df['_RENAMU_48'] * 100 / df['_RENAMU_41']
        output_district['RENAMU_50'] = df['_RENAMU_50']
        output_district['RENAMU_51'] = df['_RENAMU_51']
        output_district['RENAMU_52'] = df['P66_1_1'] +  df['P66_10_1'] +  df['P66_2_1'] +  df['P66_3_1'] +  df['P66_4_1']  +  df['P66_5_1'] +  df['P66_6_1'] +  df['P66_7_1'] +  df['P66_8_1'] +  df['P66_9_1'] 
        output_district['RENAMU_53'] = df['_RENAMU_53']
        output_district['RENAMU_54'] = df['_RENAMU_54']
        output_district['RENAMU_55'] = df['_RENAMU_55']
        output_district['RENAMU_56'] = np.nan
        output_district['RENAMU_57'] = df.apply(lambda x: 1 if x['P10_4'] == 1 else 0 if x['P10_4'] == 0 else np.nan, axis=1)
        output_district['RENAMU_58'] = np.nan
        output_district['RENAMU_59'] = df.apply(lambda x: 1 if x['P12A_3'] == 1 else 0 if x['P12A_3'] == 0 else np.nan, axis=1)
        output_district['RENAMU_60'] = np.nan
        output_district['RENAMU_61'] = df.apply(lambda x: 1 if x['P12A_6'] == 1 else 0 if x['P12A_6'] == 0 else np.nan, axis=1)
        output_district['RENAMU_62'] = np.nan
        output_district['RENAMU_63'] = df.apply(lambda x: 1 if x['P12A_9'] == 1 else 0 if x['P12A_9'] == 0 else np.nan, axis=1)
        output_district['RENAMU_64'] = np.nan
        output_district['RENAMU_65'] = df.apply(lambda x: 1 if x['P13_1'] == 1 else 0 if x['P13_1'] == 0 else np.nan, axis=1)
        output_district['RENAMU_66'] = np.nan
        output_district['RENAMU_67'] = df.apply(lambda x: 1 if x['P13_2'] == 1 else 0 if x['P13_2'] == 0 else np.nan, axis=1)
        output_district['RENAMU_68'] = np.nan
        output_district['RENAMU_69'] = df.apply(lambda x: 1 if x['P15'] == 1 else 0 if x['P15'] == 0 else np.nan, axis=1)
        output_district['RENAMU_70'] = np.nan
        output_district['RENAMU_71'] = df.apply(lambda x: 1 if x['P16_1'] == 1 else 0 if x['P16_1'] == 0 else np.nan, axis=1)
        output_district['RENAMU_72'] = np.nan
        output_district['RENAMU_73'] = df.apply(lambda x: 1 if x['P16_2'] == 1 else 0 if x['P16_2'] == 0 else np.nan, axis=1)
        output_district['RENAMU_74'] = np.nan
        output_district['RENAMU_75'] = df.apply(lambda x: 1 if x['P16_3'] == 1 else 0 if x['P16_3'] == 0 else np.nan, axis=1)
        output_district['RENAMU_76'] = np.nan
        output_district['RENAMU_77'] = df.apply(lambda x: 1 if x['P16_4'] == 1 else 0 if x['P16_4'] == 0 else np.nan, axis=1)
        output_district['RENAMU_78'] = np.nan
        output_district['RENAMU_79'] = df.apply(lambda x: 1 if x['P16_5'] == 1 else 0 if x['P16_5'] == 0 else np.nan, axis=1)
        output_district['RENAMU_80'] = np.nan
        output_district['RENAMU_81'] = df['_RENAMU_81']
        output_district['RENAMU_82'] = np.nan
        output_district['RENAMU_83'] = df.apply(lambda x: 1 if x['P31_1'] == 1 else 0 if x['P31_1'] == 0 else np.nan, axis=1)
        output_district['RENAMU_84'] = np.nan
        output_district['RENAMU_85'] = df.apply(lambda x: 1 if x['P31_2'] == 1 else 0 if x['P31_2'] == 0 else np.nan, axis=1)
        output_district['RENAMU_86'] = np.nan
        output_district['RENAMU_87'] = df.apply(lambda x: 1 if x['P31_3'] == 1 else 0 if x['P31_3'] == 0 else np.nan, axis=1)
        output_district['RENAMU_88'] = np.nan
        output_district['RENAMU_89'] = df.apply(lambda x: 1 if x['P31_4'] == 1 else 0 if x['P31_4'] == 0 else np.nan, axis=1)
        output_district['RENAMU_90'] = np.nan
        output_district['RENAMU_91'] = df['_RENAMU_91']
        output_district['RENAMU_92'] = np.nan
        output_district['RENAMU_93'] = df.apply(lambda x: 1 if x['P39_1'] == 1 else 0 if x['P39_1'] == 0 else np.nan, axis=1)
        output_district['RENAMU_94'] = np.nan
        output_district['RENAMU_95'] = df.apply(lambda x: 1 if x['P39_2'] == 1 else 0 if x['P39_2'] == 0 else np.nan, axis=1)
        output_district['RENAMU_96'] = np.nan
        output_district['RENAMU_97'] = df.apply(lambda x: 1 if x['P40A_1'] == 1 else 0 if x['P40A_1'] == 0 else np.nan, axis=1)
        output_district['RENAMU_98'] = np.nan
        output_district['RENAMU_99'] = df.apply(lambda x: 1 if x['P40A_2'] == 1 else 0 if x['P40A_2'] == 0 else np.nan, axis=1)
        output_district['RENAMU_100'] = np.nan
        output_district['RENAMU_101'] = df.apply(lambda x: 1 if x['P40A_4'] == 1 else 0 if x['P40A_4'] == 0 else np.nan, axis=1)
        output_district['RENAMU_102'] = np.nan
        output_district['RENAMU_103'] = df.apply(lambda x: 1 if x['P40A_3'] == 1 else 0 if x['P40A_3'] == 0 else np.nan, axis=1)
        output_district['RENAMU_104'] = np.nan
        output_district['RENAMU_105'] = df['_P41_1']
        output_district['RENAMU_106'] = np.nan
        output_district['RENAMU_107'] = np.nan
        output_district['RENAMU_108'] = np.nan
        output_district['RENAMU_109'] = np.nan
        output_district['RENAMU_110'] = df['_P43_1']
        output_district['RENAMU_111'] = np.nan
        output_district['RENAMU_112'] = np.nan
        output_district['RENAMU_113'] = np.nan
        output_district['RENAMU_114'] = np.nan
        output_district['RENAMU_115'] = df['P44_1']
        output_district['RENAMU_116'] = np.nan
        output_district['RENAMU_117'] = df['P48_T']
        output_district['RENAMU_118'] = df.apply(lambda x: 1 if x['P49_1'] == 1 else 0 if x['P49_1'] == 0 else np.nan, axis=1)
        output_district['RENAMU_119'] = np.nan
        output_district['RENAMU_120'] = df.apply(lambda x: 1 if x['P49_2'] == 1 else 0 if x['P49_2'] == 0 else np.nan, axis=1)
        output_district['RENAMU_121'] = np.nan
        output_district['RENAMU_122'] = df.apply(lambda x: 1 if x['P49_3'] == 1 else 0 if x['P49_3'] == 0 else np.nan, axis=1)
        output_district['RENAMU_123'] = np.nan
        output_district['RENAMU_124'] = df.apply(lambda x: 1 if x['P49_4'] == 1 else 0 if x['P49_4'] == 0 else np.nan, axis=1)
        output_district['RENAMU_125'] = np.nan
        output_district['RENAMU_126'] = df.apply(lambda x: 1 if x['P49_5'] == 1 else 0 if x['P49_5'] == 0 else np.nan, axis=1)
        output_district['RENAMU_127'] = np.nan
        output_district['RENAMU_128'] = df.apply(lambda x: 1 if x['P49_6'] == 1 else 0 if x['P49_6'] == 0 else np.nan, axis=1)
        output_district['RENAMU_129'] = np.nan
        output_district['RENAMU_130'] = df.apply(lambda x: 1 if x['P49_7'] == 1 else 0 if x['P49_7'] == 0 else np.nan, axis=1)
        output_district['RENAMU_131'] = np.nan
        output_district['RENAMU_132'] = df.apply(lambda x: 1 if x['P49_8'] == 1 else 0 if x['P49_8'] == 0 else np.nan, axis=1)
        output_district['RENAMU_133'] = np.nan
        output_district['RENAMU_134'] = df['_RENAMU_134']
        output_district['RENAMU_135'] = np.nan
        output_district['RENAMU_136'] = df.apply(lambda x: 1 if x['P52'] == 1 else 0 if x['P52'] == 0 else np.nan, axis=1)
        output_district['RENAMU_137'] = np.nan
        output_district['RENAMU_138'] = df['_RENAMU_138']
        output_district['RENAMU_139'] = np.nan
        output_district['RENAMU_140'] = df['_RENAMU_140']
        output_district['RENAMU_141'] = np.nan
        output_district['RENAMU_142'] = df['P52_1']
        output_district['RENAMU_143'] = df['P53A_1']
        output_district['RENAMU_144'] = df['_RENAMU_144']
        output_district['RENAMU_145'] = np.nan
        output_district['RENAMU_146'] = df['P60A_1_2']
        output_district['RENAMU_147'] = df['_RENAMU_147']
        output_district['RENAMU_148'] = np.nan
        output_district['RENAMU_149'] = df['P60A_2_1']
        output_district['RENAMU_150'] = df['_RENAMU_150']
        output_district['RENAMU_151'] = np.nan
        output_district['RENAMU_152'] = df['P60A_3_1']
        output_district['RENAMU_153'] = df.apply(lambda x: 1 if x['P61'] == 1 else 0 if x['P61'] == 0 else np.nan, axis=1)
        output_district['RENAMU_154'] = np.nan
        output_district['RENAMU_155'] = df['P61_1']
        output_district['RENAMU_156'] = df.apply(lambda x: 1 if x['_P62'] == 1 else 0 if x['_P62'] == 0 else np.nan, axis=1)
        output_district['RENAMU_157'] = np.nan
        output_district['RENAMU_158'] = df['P62_1']
        output_district['RENAMU_159'] = df.apply(lambda x: 1 if x['P64'] == 1 else 0 if x['P64'] == 0 else np.nan, axis=1)
        output_district['RENAMU_160'] = np.nan
        output_district['RENAMU_161'] = df.apply(lambda x: 1 if x['P66'] == 1 else 0 if x['P66'] == 0 else np.nan, axis=1)
        output_district['RENAMU_162'] = np.nan
        output_district['RENAMU_163'] = df.apply(lambda x: 1 if x['P69_2'] == 1 else 0 if x['P69_2'] == 0 else np.nan, axis=1)
        output_district['RENAMU_164'] = np.nan
        output_district['RENAMU_165'] = df['P69_2_T']
        output_district['RENAMU_166'] = df['P75']
        output_district['RENAMU_167'] = np.nan
        output_district['RENAMU_168'] = df['P78_1']
        output_district['RENAMU_169'] = np.nan
        output_district['RENAMU_170'] = df['P78_2']
        output_district['RENAMU_171'] = np.nan
        output_district['RENAMU_172'] = df['P78_3']
        output_district['RENAMU_173'] = np.nan
        output_district['RENAMU_174'] = df['P97']
        output_district['RENAMU_175'] = np.nan
        output_district['RENAMU_176'] = np.nan
        output_district['RENAMU_177'] = df['P98_1_1']
        output_district['RENAMU_178'] = df['P98_1_2']
        output_district['RENAMU_179'] = np.nan
        output_district['RENAMU_180'] = df['P98_2_1']
        output_district['RENAMU_181'] = df['P98_2_2']
        output_district['RENAMU_182'] = np.nan
        output_district['RENAMU_183'] = df['P98_3_1']
        output_district['RENAMU_184'] = df['P98_3_2']
        output_district['RENAMU_185'] = np.nan
        output_district['RENAMU_186'] = df['P98_4_1']
        output_district['RENAMU_187'] = df['P98_4_2']
        output_district['RENAMU_188'] = np.nan
        output_district['RENAMU_189'] = df['P98_5_1']
        output_district['RENAMU_190'] = df['P98_5_2']

        # Calculates province level indicators

        output_province = df_province[['province_id', 'year']].copy()
        output_province['RENAMU_1'] = df_province['P14A_1'] + df_province['P14A_2'] + df_province['P14A_3'] + df_province['P14A_4'] + df_province['P14A_5'] + df_province['P14A_6'] + df_province['P14A_7'] + df_province['P14A_8'] + df_province['P14A_9']
        output_province['RENAMU_2'] = np.nan
        output_province['RENAMU_3'] = df_province['P18_1'] * 100 / df_province['count']
        output_province['RENAMU_4'] = np.nan
        output_province['RENAMU_5'] = df_province['P18_10'] * 100 / df_province['count']
        output_province['RENAMU_6'] = np.nan
        output_province['RENAMU_7'] = df_province['P18_11'] * 100 / df_province['count']
        output_province['RENAMU_8'] = np.nan
        output_province['RENAMU_9'] = df_province['P18_12'] * 100 / df_province['count']
        output_province['RENAMU_10'] = np.nan
        output_province['RENAMU_11'] = df_province['P18_2'] * 100 / df_province['count']
        output_province['RENAMU_12'] = np.nan
        output_province['RENAMU_13'] = df_province['P18_3'] * 100 / df_province['count']
        output_province['RENAMU_14'] = np.nan
        output_province['RENAMU_15'] = df_province['P18_4'] * 100 / df_province['count']
        output_province['RENAMU_16'] = np.nan
        output_province['RENAMU_17'] = df_province['P18_5'] * 100 / df_province['count']
        output_province['RENAMU_18'] = np.nan
        output_province['RENAMU_19'] = df_province['P18_6'] * 100 / df_province['count']
        output_province['RENAMU_20'] = np.nan
        output_province['RENAMU_21'] = df_province['P18_7'] * 100 / df_province['count']
        output_province['RENAMU_22'] = np.nan
        output_province['RENAMU_23'] = df_province['P18_8'] * 100 / df_province['count']
        output_province['RENAMU_24'] = np.nan
        output_province['RENAMU_25'] = df_province['P18_9'] * 100 / df_province['count']
        output_province['RENAMU_26'] = np.nan
        output_province['RENAMU_27'] = df_province['P19_AUX'] * 100 / df_province['count']
        output_province['RENAMU_28'] = df_province['P20_1_T']
        output_province['RENAMU_29'] = df_province['P20_1_T'] * 100 / (df_province['P20_1_T'] + df_province['P20_2_T'] + df_province['P20_3_T'] + df_province['P20_4_T'] + df_province['P20_5_T'] + df_province['P20_6_T'])
        output_province['RENAMU_30'] = df_province['P20_2_T']
        output_province['RENAMU_31'] = df_province['P20_2_T'] * 100 / (df_province['P20_1_T'] + df_province['P20_2_T'] + df_province['P20_3_T'] + df_province['P20_4_T'] + df_province['P20_5_T'] + df_province['P20_6_T'])
        output_province['RENAMU_32'] = df_province['P20_3_T']
        output_province['RENAMU_33'] = df_province['P20_3_T'] * 100 / (df_province['P20_1_T'] + df_province['P20_2_T'] + df_province['P20_3_T'] + df_province['P20_4_T'] + df_province['P20_5_T'] + df_province['P20_6_T'])
        output_province['RENAMU_34'] = df_province['P20_4_T']
        output_province['RENAMU_35'] = df_province['P20_4_T'] * 100 / (df_province['P20_1_T'] + df_province['P20_2_T'] + df_province['P20_3_T'] + df_province['P20_4_T'] + df_province['P20_5_T'] + df_province['P20_6_T'])
        output_province['RENAMU_36'] = df_province['P20_5_T'] + df_province['P20_6_T']
        output_province['RENAMU_37'] = (df_province['P20_5_T'] + df_province['P20_6_T']) * 100 / (df_province['P20_1_T'] + df_province['P20_2_T'] + df_province['P20_3_T'] + df_province['P20_4_T'] + df_province['P20_5_T'] + df_province['P20_6_T'])
        output_province['RENAMU_38'] = df_province['P20_1_T'] + df_province['P20_2_T'] + df_province['P20_3_T'] + df_province['P20_4_T'] + df_province['P20_5_T'] + df_province['P20_6_T']
        output_province['RENAMU_39'] = df_province['P32_2_T']
        output_province['RENAMU_40'] = df_province['P32_2_T'] * 100 / (df_province['P20_1_T'] + df_province['P20_2_T'] + df_province['P20_3_T'] + df_province['P20_4_T'] + df_province['P20_5_T'] + df_province['P20_6_T'])
        output_province['RENAMU_41'] = df_province['_RENAMU_41']
        output_province['RENAMU_42'] = df_province['_RENAMU_42']
        output_province['RENAMU_43'] = df_province['_RENAMU_42'] * 100 / df_province['_RENAMU_41']
        output_province['RENAMU_44'] = df_province['_RENAMU_44']
        output_province['RENAMU_45'] = df_province['_RENAMU_44'] * 100 / df_province['_RENAMU_41']
        output_province['RENAMU_46'] = df_province['_RENAMU_46']
        output_province['RENAMU_47'] = df_province['_RENAMU_46'] * 100 / df_province['_RENAMU_41']
        output_province['RENAMU_48'] = df_province['_RENAMU_48']
        output_province['RENAMU_49'] = df_province['_RENAMU_48'] * 100 / df_province['_RENAMU_41']
        output_province['RENAMU_50'] = df_province['_RENAMU_50']
        output_province['RENAMU_51'] = df_province['_RENAMU_51']
        output_province['RENAMU_52'] = df_province['P66_1_1'] +  df_province['P66_10_1'] +  df_province['P66_2_1'] +  df_province['P66_3_1'] +  df_province['P66_4_1']  +  df_province['P66_5_1'] +  df_province['P66_6_1'] +  df_province['P66_7_1'] +  df_province['P66_8_1'] +  df_province['P66_9_1'] 
        output_province['RENAMU_53'] = df_province['_RENAMU_53']
        output_province['RENAMU_54'] = df_province['_RENAMU_54']
        output_province['RENAMU_55'] = np.nan
        output_province['RENAMU_56'] = df_province['_RENAMU_56'] * 100 / df_province['count']
        output_province['RENAMU_57'] = np.nan
        output_province['RENAMU_58'] = df_province['P10_4'] * 100 / df_province['count']
        output_province['RENAMU_59'] = np.nan
        output_province['RENAMU_60'] = df_province['P12A_3'] * 100 / df_province['count']
        output_province['RENAMU_61'] = np.nan
        output_province['RENAMU_62'] = df_province['P12A_6'] * 100 / df_province['count']
        output_province['RENAMU_63'] = np.nan
        output_province['RENAMU_64'] = df_province['P12A_9'] * 100 / df_province['count']
        output_province['RENAMU_65'] = np.nan
        output_province['RENAMU_66'] = df_province['P13_1_AUX'] * 100 / df_province['count']
        output_province['RENAMU_67'] = np.nan
        output_province['RENAMU_68'] = df_province['P13_2'] * 100 / df_province['count']
        output_province['RENAMU_69'] = np.nan
        output_province['RENAMU_70'] = df_province['P15'] * 100 / df_province['count']
        output_province['RENAMU_71'] = np.nan
        output_province['RENAMU_72'] = df_province['P16_1'] * 100 / df_province['count']
        output_province['RENAMU_73'] = np.nan
        output_province['RENAMU_74'] = df_province['P16_2'] * 100 / df_province['count']
        output_province['RENAMU_75'] = np.nan
        output_province['RENAMU_76'] = df_province['P16_3'] * 100 / df_province['count']
        output_province['RENAMU_77'] = np.nan
        output_province['RENAMU_78'] = df_province['P16_4'] * 100 / df_province['count']
        output_province['RENAMU_79'] = np.nan
        output_province['RENAMU_80'] = df_province['P16_5'] * 100 / df_province['count']
        output_province['RENAMU_81'] = np.nan
        output_province['RENAMU_82'] = df_province['_RENAMU_81'] * 100 / df_province['count']
        output_province['RENAMU_83'] = np.nan
        output_province['RENAMU_84'] = df_province['P31_1'] * 100 / df_province['count']
        output_province['RENAMU_85'] = np.nan
        output_province['RENAMU_86'] = df_province['P31_2'] * 100 / df_province['count']
        output_province['RENAMU_87'] = np.nan
        output_province['RENAMU_88'] = df_province['P31_3'] * 100 / df_province['count']
        output_province['RENAMU_89'] = np.nan
        output_province['RENAMU_90'] = df_province['P31_4'] * 100 / df_province['count']
        output_province['RENAMU_91'] = np.nan
        output_province['RENAMU_92'] = df_province['_RENAMU_91'] * 100 / df_province['count']
        output_province['RENAMU_93'] = np.nan
        output_province['RENAMU_94'] = df_province['P39_1'] * 100 / df_province['count']
        output_province['RENAMU_95'] = np.nan
        output_province['RENAMU_96'] = df_province['P39_2'] * 100 / df_province['count']
        output_province['RENAMU_97'] = np.nan
        output_province['RENAMU_98'] = df_province['P40A_1'] * 100 / df_province['count']
        output_province['RENAMU_99'] = np.nan
        output_province['RENAMU_100'] = df_province['P40A_2'] * 100 / df_province['count']
        output_province['RENAMU_101'] = np.nan
        output_province['RENAMU_102'] = df_province['P40A_4'] * 100 / df_province['count']
        output_province['RENAMU_103'] = np.nan
        output_province['RENAMU_104'] = df_province['P40A_3'] * 100 / df_province['count']
        output_province['RENAMU_105'] = np.nan
        output_province['RENAMU_106'] = df_province['P41_1_1_AUX'] * 100 / df_province['count']
        output_province['RENAMU_107'] = df_province['P41_1_2_AUX'] * 100 / df_province['count']
        output_province['RENAMU_108'] = df_province['P41_1_3_AUX'] * 100 / df_province['count']
        output_province['RENAMU_109'] = df_province['P41_1_4_AUX'] * 100 / df_province['count']
        output_province['RENAMU_110'] = np.nan
        output_province['RENAMU_111'] = df_province['P43_1_1_AUX'] * 100 / df_province['count']
        output_province['RENAMU_112'] = df_province['P43_1_2_AUX'] * 100 / df_province['count']
        output_province['RENAMU_113'] = df_province['P43_1_3_AUX'] * 100 / df_province['count']
        output_province['RENAMU_114'] = df_province['P43_1_4_AUX'] * 100 / df_province['count']
        output_province['RENAMU_115'] = np.nan
        output_province['RENAMU_116'] = df_province['P44_1'] * 100 / df_province['count']
        output_province['RENAMU_117'] = df_province['P48_T']
        output_province['RENAMU_118'] = np.nan
        output_province['RENAMU_119'] = df_province['P49_1'] * 100 / df_province['count']
        output_province['RENAMU_120'] = np.nan
        output_province['RENAMU_121'] = df_province['P49_2'] * 100 / df_province['count']
        output_province['RENAMU_122'] = np.nan
        output_province['RENAMU_123'] = df_province['P49_3'] * 100 / df_province['count']
        output_province['RENAMU_124'] = np.nan
        output_province['RENAMU_125'] = df_province['P49_4'] * 100 / df_province['count']
        output_province['RENAMU_126'] = np.nan
        output_province['RENAMU_127'] = df_province['P49_5'] * 100 / df_province['count']
        output_province['RENAMU_128'] = np.nan
        output_province['RENAMU_129'] = df_province['P49_6'] * 100 / df_province['count']
        output_province['RENAMU_130'] = np.nan
        output_province['RENAMU_131'] = df_province['P49_7'] * 100 / df_province['count']
        output_province['RENAMU_132'] = np.nan
        output_province['RENAMU_133'] = df_province['P49_8'] * 100 / df_province['count']
        output_province['RENAMU_134'] = np.nan
        output_province['RENAMU_135'] = df_province['_RENAMU_134'] * 100 / df_province['count']
        output_province['RENAMU_136'] = np.nan
        output_province['RENAMU_137'] = df_province['P52'] * 100 / df_province['count']
        output_province['RENAMU_138'] = np.nan
        output_province['RENAMU_139'] = df_province['_RENAMU_138'] * 100 / df_province['count']
        output_province['RENAMU_140'] = np.nan
        output_province['RENAMU_141'] = df_province['_RENAMU_140'] * 100 / df_province['count']
        output_province['RENAMU_142'] = df_province['P52_1']
        output_province['RENAMU_143'] = df_province['P53A_1']
        output_province['RENAMU_144'] = np.nan
        output_province['RENAMU_145'] = df_province['_RENAMU_144'] * 100 / df_province['count']
        output_province['RENAMU_146'] = df_province['P60A_1_2']
        output_province['RENAMU_147'] = np.nan
        output_province['RENAMU_148'] = df_province['_RENAMU_147'] * 100 / df_province['count']
        output_province['RENAMU_149'] = df_province['P60A_2_1']
        output_province['RENAMU_150'] = np.nan
        output_province['RENAMU_151'] = df_province['_RENAMU_150'] * 100 / df_province['count']
        output_province['RENAMU_152'] = df_province['P60A_3_1']
        output_province['RENAMU_153'] = np.nan
        output_province['RENAMU_154'] = df_province['P61'] * 100 / df_province['count']
        output_province['RENAMU_155'] = df_province['P61_1']
        output_province['RENAMU_156'] = np.nan
        output_province['RENAMU_157'] = df_province['_P62'] * 100 / df_province['count']
        output_province['RENAMU_158'] = df_province['P62_1']
        output_province['RENAMU_159'] = np.nan
        output_province['RENAMU_160'] = df_province['P64'] * 100 / df_province['count']
        output_province['RENAMU_161'] = np.nan
        output_province['RENAMU_162'] = df_province['P66'] * 100 / df_province['count']
        output_province['RENAMU_163'] = np.nan
        output_province['RENAMU_164'] = df_province['P69_2'] * 100 / df_province['count']
        output_province['RENAMU_165'] = df_province['P69_2_T']
        output_province['RENAMU_166'] = np.nan
        output_province['RENAMU_167'] = df_province['P75'] * 100 / df_province['count']
        output_province['RENAMU_168'] = np.nan
        output_province['RENAMU_169'] = df_province['P78_1'] * 100 / df_province['count']
        output_province['RENAMU_170'] = np.nan
        output_province['RENAMU_171'] = df_province['P78_2'] * 100 / df_province['count']
        output_province['RENAMU_172'] = np.nan
        output_province['RENAMU_173'] = df_province['P78_3'] * 100 / df_province['count']
        output_province['RENAMU_174'] = np.nan
        output_province['RENAMU_175'] = df_province['P97'] * 100 / df_province['count']
        output_province['RENAMU_176'] = df_province['P98_1'] * 100 / df_province['count']
        output_province['RENAMU_177'] = df_province['P98_1_1']
        output_province['RENAMU_178'] = df_province['P98_1_2']
        output_province['RENAMU_179'] = df_province['P98_2'] * 100 / df_province['count']
        output_province['RENAMU_180'] = df_province['P98_2_1']
        output_province['RENAMU_181'] = df_province['P98_2_2']
        output_province['RENAMU_182'] = df_province['P98_3'] * 100 / df_province['count']
        output_province['RENAMU_183'] = df_province['P98_3_1']
        output_province['RENAMU_184'] = df_province['P98_3_2']
        output_province['RENAMU_185'] = df_province['P98_4'] * 100 / df_province['count']
        output_province['RENAMU_186'] = df_province['P98_4_1']
        output_province['RENAMU_187'] = df_province['P98_4_2']
        output_province['RENAMU_188'] = df_province['P98_5'] * 100 / df_province['count']
        output_province['RENAMU_189'] = df_province['P98_5_1']
        output_province['RENAMU_190'] = df_province['P98_5_2']

        # Calculates department level indicators

        output_department = df_department[['department_id', 'year']].copy()
        output_department['RENAMU_1'] = df_department['P14A_1'] + df_department['P14A_2'] + df_department['P14A_3'] + df_department['P14A_4'] + df_department['P14A_5'] + df_department['P14A_6'] + df_department['P14A_7'] + df_department['P14A_8'] + df_department['P14A_9']
        output_department['RENAMU_2'] = np.nan
        output_department['RENAMU_3'] = df_department['P18_1'] * 100 / df_department['count']
        output_department['RENAMU_4'] = np.nan
        output_department['RENAMU_5'] = df_department['P18_10'] * 100 / df_department['count']
        output_department['RENAMU_6'] = np.nan
        output_department['RENAMU_7'] = df_department['P18_11'] * 100 / df_department['count']
        output_department['RENAMU_8'] = np.nan
        output_department['RENAMU_9'] = df_department['P18_12'] * 100 / df_department['count']
        output_department['RENAMU_10'] = np.nan
        output_department['RENAMU_11'] = df_department['P18_2'] * 100 / df_department['count']
        output_department['RENAMU_12'] = np.nan
        output_department['RENAMU_13'] = df_department['P18_3'] * 100 / df_department['count']
        output_department['RENAMU_14'] = np.nan
        output_department['RENAMU_15'] = df_department['P18_4'] * 100 / df_department['count']
        output_department['RENAMU_16'] = np.nan
        output_department['RENAMU_17'] = df_department['P18_5'] * 100 / df_department['count']
        output_department['RENAMU_18'] = np.nan
        output_department['RENAMU_19'] = df_department['P18_6'] * 100 / df_department['count']
        output_department['RENAMU_20'] = np.nan
        output_department['RENAMU_21'] = df_department['P18_7'] * 100 / df_department['count']
        output_department['RENAMU_22'] = np.nan
        output_department['RENAMU_23'] = df_department['P18_8'] * 100 / df_department['count']
        output_department['RENAMU_24'] = np.nan
        output_department['RENAMU_25'] = df_department['P18_9'] * 100 / df_department['count']
        output_department['RENAMU_26'] = np.nan
        output_department['RENAMU_27'] = df_department['P19_AUX'] * 100 / df_department['count']
        output_department['RENAMU_28'] = df_department['P20_1_T']
        output_department['RENAMU_29'] = df_department['P20_1_T'] * 100 / (df_department['P20_1_T'] + df_department['P20_2_T'] + df_department['P20_3_T'] + df_department['P20_4_T'] + df_department['P20_5_T'] + df_department['P20_6_T'])
        output_department['RENAMU_30'] = df_department['P20_2_T']
        output_department['RENAMU_31'] = df_department['P20_2_T'] * 100 / (df_department['P20_1_T'] + df_department['P20_2_T'] + df_department['P20_3_T'] + df_department['P20_4_T'] + df_department['P20_5_T'] + df_department['P20_6_T'])
        output_department['RENAMU_32'] = df_department['P20_3_T']
        output_department['RENAMU_33'] = df_department['P20_3_T'] * 100 / (df_department['P20_1_T'] + df_department['P20_2_T'] + df_department['P20_3_T'] + df_department['P20_4_T'] + df_department['P20_5_T'] + df_department['P20_6_T'])
        output_department['RENAMU_34'] = df_department['P20_4_T']
        output_department['RENAMU_35'] = df_department['P20_4_T'] * 100 / (df_department['P20_1_T'] + df_department['P20_2_T'] + df_department['P20_3_T'] + df_department['P20_4_T'] + df_department['P20_5_T'] + df_department['P20_6_T'])
        output_department['RENAMU_36'] = df_department['P20_5_T'] + df_department['P20_6_T']
        output_department['RENAMU_37'] = (df_department['P20_5_T'] + df_department['P20_6_T']) * 100 / (df_department['P20_1_T'] + df_department['P20_2_T'] + df_department['P20_3_T'] + df_department['P20_4_T'] + df_department['P20_5_T'] + df_department['P20_6_T'])
        output_department['RENAMU_38'] = df_department['P20_1_T'] + df_department['P20_2_T'] + df_department['P20_3_T'] + df_department['P20_4_T'] + df_department['P20_5_T'] + df_department['P20_6_T']
        output_department['RENAMU_39'] = df_department['P32_2_T']
        output_department['RENAMU_40'] = df_department['P32_2_T'] * 100 / (df_department['P20_1_T'] + df_department['P20_2_T'] + df_department['P20_3_T'] + df_department['P20_4_T'] + df_department['P20_5_T'] + df_department['P20_6_T'])
        output_department['RENAMU_41'] = df_department['_RENAMU_41']
        output_department['RENAMU_42'] = df_department['_RENAMU_42']
        output_department['RENAMU_43'] = df_department['_RENAMU_42'] * 100 / df_department['_RENAMU_41']
        output_department['RENAMU_44'] = df_department['_RENAMU_44']
        output_department['RENAMU_45'] = df_department['_RENAMU_44'] * 100 / df_department['_RENAMU_41']
        output_department['RENAMU_46'] = df_department['_RENAMU_46']
        output_department['RENAMU_47'] = df_department['_RENAMU_46'] * 100 / df_department['_RENAMU_41']
        output_department['RENAMU_48'] = df_department['_RENAMU_48']
        output_department['RENAMU_49'] = df_department['_RENAMU_48'] * 100 / df_department['_RENAMU_41']
        output_department['RENAMU_50'] = df_department['_RENAMU_50']
        output_department['RENAMU_51'] = df_department['_RENAMU_51']
        output_department['RENAMU_52'] = df_department['P66_1_1'] +  df_department['P66_10_1'] +  df_department['P66_2_1'] +  df_department['P66_3_1'] +  df_department['P66_4_1']  +  df_department['P66_5_1'] +  df_department['P66_6_1'] +  df_department['P66_7_1'] +  df_department['P66_8_1'] +  df_department['P66_9_1'] 
        output_department['RENAMU_53'] = df_department['_RENAMU_53']
        output_department['RENAMU_54'] = df_department['_RENAMU_54']
        output_department['RENAMU_55'] = np.nan
        output_department['RENAMU_56'] = df_department['_RENAMU_56'] * 100 / df_department['count']
        output_department['RENAMU_57'] = np.nan
        output_department['RENAMU_58'] = df_department['P10_4'] * 100 / df_department['count']
        output_department['RENAMU_59'] = np.nan
        output_department['RENAMU_60'] = df_department['P12A_3'] * 100 / df_department['count']
        output_department['RENAMU_61'] = np.nan
        output_department['RENAMU_62'] = df_department['P12A_6'] * 100 / df_department['count']
        output_department['RENAMU_63'] = np.nan
        output_department['RENAMU_64'] = df_department['P12A_9'] * 100 / df_department['count']
        output_department['RENAMU_65'] = np.nan
        output_department['RENAMU_66'] = df_department['P13_1_AUX'] * 100 / df_department['count']
        output_department['RENAMU_67'] = np.nan
        output_department['RENAMU_68'] = df_department['P13_2'] * 100 / df_department['count']
        output_department['RENAMU_69'] = np.nan
        output_department['RENAMU_70'] = df_department['P15'] * 100 / df_department['count']
        output_department['RENAMU_71'] = np.nan
        output_department['RENAMU_72'] = df_department['P16_1'] * 100 / df_department['count']
        output_department['RENAMU_73'] = np.nan
        output_department['RENAMU_74'] = df_department['P16_2'] * 100 / df_department['count']
        output_department['RENAMU_75'] = np.nan
        output_department['RENAMU_76'] = df_department['P16_3'] * 100 / df_department['count']
        output_department['RENAMU_77'] = np.nan
        output_department['RENAMU_78'] = df_department['P16_4'] * 100 / df_department['count']
        output_department['RENAMU_79'] = np.nan
        output_department['RENAMU_80'] = df_department['P16_5'] * 100 / df_department['count']
        output_department['RENAMU_81'] = np.nan
        output_department['RENAMU_82'] = df_department['_RENAMU_81'] * 100 / df_department['count']
        output_department['RENAMU_83'] = np.nan
        output_department['RENAMU_84'] = df_department['P31_1'] * 100 / df_department['count']
        output_department['RENAMU_85'] = np.nan
        output_department['RENAMU_86'] = df_department['P31_2'] * 100 / df_department['count']
        output_department['RENAMU_87'] = np.nan
        output_department['RENAMU_88'] = df_department['P31_3'] * 100 / df_department['count']
        output_department['RENAMU_89'] = np.nan
        output_department['RENAMU_90'] = df_department['P31_4'] * 100 / df_department['count']
        output_department['RENAMU_91'] = np.nan
        output_department['RENAMU_92'] = df_department['_RENAMU_91'] * 100 / df_department['count']
        output_department['RENAMU_93'] = np.nan
        output_department['RENAMU_94'] = df_department['P39_1'] * 100 / df_department['count']
        output_department['RENAMU_95'] = np.nan
        output_department['RENAMU_96'] = df_department['P39_2'] * 100 / df_department['count']
        output_department['RENAMU_97'] = np.nan
        output_department['RENAMU_98'] = df_department['P40A_1'] * 100 / df_department['count']
        output_department['RENAMU_99'] = np.nan
        output_department['RENAMU_100'] = df_department['P40A_2'] * 100 / df_department['count']
        output_department['RENAMU_101'] = np.nan
        output_department['RENAMU_102'] = df_department['P40A_4'] * 100 / df_department['count']
        output_department['RENAMU_103'] = np.nan
        output_department['RENAMU_104'] = df_department['P40A_3'] * 100 / df_department['count']
        output_department['RENAMU_105'] = np.nan
        output_department['RENAMU_106'] = df_department['P41_1_1_AUX'] * 100 / df_department['count']
        output_department['RENAMU_107'] = df_department['P41_1_2_AUX'] * 100 / df_department['count']
        output_department['RENAMU_108'] = df_department['P41_1_3_AUX'] * 100 / df_department['count']
        output_department['RENAMU_109'] = df_department['P41_1_4_AUX'] * 100 / df_department['count']
        output_department['RENAMU_110'] = np.nan
        output_department['RENAMU_111'] = df_department['P43_1_1_AUX'] * 100 / df_department['count']
        output_department['RENAMU_112'] = df_department['P43_1_2_AUX'] * 100 / df_department['count']
        output_department['RENAMU_113'] = df_department['P43_1_3_AUX'] * 100 / df_department['count']
        output_department['RENAMU_114'] = df_department['P43_1_4_AUX'] * 100 / df_department['count']
        output_department['RENAMU_115'] = np.nan
        output_department['RENAMU_116'] = df_department['P44_1'] * 100 / df_department['count']
        output_department['RENAMU_117'] = df_department['P48_T']
        output_department['RENAMU_118'] = np.nan
        output_department['RENAMU_119'] = df_department['P49_1'] * 100 / df_department['count']
        output_department['RENAMU_120'] = np.nan
        output_department['RENAMU_121'] = df_department['P49_2'] * 100 / df_department['count']
        output_department['RENAMU_122'] = np.nan
        output_department['RENAMU_123'] = df_department['P49_3'] * 100 / df_department['count']
        output_department['RENAMU_124'] = np.nan
        output_department['RENAMU_125'] = df_department['P49_4'] * 100 / df_department['count']
        output_department['RENAMU_126'] = np.nan
        output_department['RENAMU_127'] = df_department['P49_5'] * 100 / df_department['count']
        output_department['RENAMU_128'] = np.nan
        output_department['RENAMU_129'] = df_department['P49_6'] * 100 / df_department['count']
        output_department['RENAMU_130'] = np.nan
        output_department['RENAMU_131'] = df_department['P49_7'] * 100 / df_department['count']
        output_department['RENAMU_132'] = np.nan
        output_department['RENAMU_133'] = df_department['P49_8'] * 100 / df_department['count']
        output_department['RENAMU_134'] = np.nan
        output_department['RENAMU_135'] = df_department['_RENAMU_134'] * 100 / df_department['count']
        output_department['RENAMU_136'] = np.nan
        output_department['RENAMU_137'] = df_department['P52'] * 100 / df_department['count']
        output_department['RENAMU_138'] = np.nan
        output_department['RENAMU_139'] = df_department['_RENAMU_138'] * 100 / df_department['count']
        output_department['RENAMU_140'] = np.nan
        output_department['RENAMU_141'] = df_department['_RENAMU_140'] * 100 / df_department['count']
        output_department['RENAMU_142'] = df_department['P52_1']
        output_department['RENAMU_143'] = df_department['P53A_1']
        output_department['RENAMU_144'] = np.nan
        output_department['RENAMU_145'] = df_department['_RENAMU_144'] * 100 / df_department['count']
        output_department['RENAMU_146'] = df_department['P60A_1_2']
        output_department['RENAMU_147'] = np.nan
        output_department['RENAMU_148'] = df_department['_RENAMU_147'] * 100 / df_department['count']
        output_department['RENAMU_149'] = df_department['P60A_2_1']
        output_department['RENAMU_150'] = np.nan
        output_department['RENAMU_151'] = df_department['_RENAMU_150'] * 100 / df_department['count']
        output_department['RENAMU_152'] = df_department['P60A_3_1']
        output_department['RENAMU_153'] = np.nan
        output_department['RENAMU_154'] = df_department['P61'] * 100 / df_department['count']
        output_department['RENAMU_155'] = df_department['P61_1']
        output_department['RENAMU_156'] = np.nan
        output_department['RENAMU_157'] = df_department['_P62'] * 100 / df_department['count']
        output_department['RENAMU_158'] = df_department['P62_1']
        output_department['RENAMU_159'] = np.nan
        output_department['RENAMU_160'] = df_department['P64'] * 100 / df_department['count']
        output_department['RENAMU_161'] = np.nan
        output_department['RENAMU_162'] = df_department['P66'] * 100 / df_department['count']
        output_department['RENAMU_163'] = np.nan
        output_department['RENAMU_164'] = df_department['P69_2'] * 100 / df_department['count']
        output_department['RENAMU_165'] = df_department['P69_2_T']
        output_department['RENAMU_166'] = np.nan
        output_department['RENAMU_167'] = df_department['P75'] * 100 / df_department['count']
        output_department['RENAMU_168'] = np.nan
        output_department['RENAMU_169'] = df_department['P78_1'] * 100 / df_department['count']
        output_department['RENAMU_170'] = np.nan
        output_department['RENAMU_171'] = df_department['P78_2'] * 100 / df_department['count']
        output_department['RENAMU_172'] = np.nan
        output_department['RENAMU_173'] = df_department['P78_3'] * 100 / df_department['count']
        output_department['RENAMU_174'] = np.nan
        output_department['RENAMU_175'] = df_department['P97'] * 100 / df_department['count']
        output_department['RENAMU_176'] = df_department['P98_1'] * 100 / df_department['count']
        output_department['RENAMU_177'] = df_department['P98_1_1']
        output_department['RENAMU_178'] = df_department['P98_1_2']
        output_department['RENAMU_179'] = df_department['P98_2'] * 100 / df_department['count']
        output_department['RENAMU_180'] = df_department['P98_2_1']
        output_department['RENAMU_181'] = df_department['P98_2_2']
        output_department['RENAMU_182'] = df_department['P98_3'] * 100 / df_department['count']
        output_department['RENAMU_183'] = df_department['P98_3_1']
        output_department['RENAMU_184'] = df_department['P98_3_2']
        output_department['RENAMU_185'] = df_department['P98_4'] * 100 / df_department['count']
        output_department['RENAMU_186'] = df_department['P98_4_1']
        output_department['RENAMU_187'] = df_department['P98_4_2']
        output_department['RENAMU_188'] = df_department['P98_5'] * 100 / df_department['count']
        output_department['RENAMU_189'] = df_department['P98_5_1']
        output_department['RENAMU_190'] = df_department['P98_5_2']

        # Calculates nation level indicators

        output_nation = df_nation[['nation_id', 'year']].copy()
        output_nation['RENAMU_1'] = df_nation['P14A_1'] + df_nation['P14A_2'] + df_nation['P14A_3'] + df_nation['P14A_4'] + df_nation['P14A_5'] + df_nation['P14A_6'] + df_nation['P14A_7'] + df_nation['P14A_8'] + df_nation['P14A_9']
        output_nation['RENAMU_2'] = np.nan
        output_nation['RENAMU_3'] = df_nation['P18_1'] * 100 / df_nation['count']
        output_nation['RENAMU_4'] = np.nan
        output_nation['RENAMU_5'] = df_nation['P18_10'] * 100 / df_nation['count']
        output_nation['RENAMU_6'] = np.nan
        output_nation['RENAMU_7'] = df_nation['P18_11'] * 100 / df_nation['count']
        output_nation['RENAMU_8'] = np.nan
        output_nation['RENAMU_9'] = df_nation['P18_12'] * 100 / df_nation['count']
        output_nation['RENAMU_10'] = np.nan
        output_nation['RENAMU_11'] = df_nation['P18_2'] * 100 / df_nation['count']
        output_nation['RENAMU_12'] = np.nan
        output_nation['RENAMU_13'] = df_nation['P18_3'] * 100 / df_nation['count']
        output_nation['RENAMU_14'] = np.nan
        output_nation['RENAMU_15'] = df_nation['P18_4'] * 100 / df_nation['count']
        output_nation['RENAMU_16'] = np.nan
        output_nation['RENAMU_17'] = df_nation['P18_5'] * 100 / df_nation['count']
        output_nation['RENAMU_18'] = np.nan
        output_nation['RENAMU_19'] = df_nation['P18_6'] * 100 / df_nation['count']
        output_nation['RENAMU_20'] = np.nan
        output_nation['RENAMU_21'] = df_nation['P18_7'] * 100 / df_nation['count']
        output_nation['RENAMU_22'] = np.nan
        output_nation['RENAMU_23'] = df_nation['P18_8'] * 100 / df_nation['count']
        output_nation['RENAMU_24'] = np.nan
        output_nation['RENAMU_25'] = df_nation['P18_9'] * 100 / df_nation['count']
        output_nation['RENAMU_26'] = np.nan
        output_nation['RENAMU_27'] = df_nation['P19_AUX'] * 100 / df_nation['count']
        output_nation['RENAMU_28'] = df_nation['P20_1_T']
        output_nation['RENAMU_29'] = df_nation['P20_1_T'] * 100 / (df_nation['P20_1_T'] + df_nation['P20_2_T'] + df_nation['P20_3_T'] + df_nation['P20_4_T'] + df_nation['P20_5_T'] + df_nation['P20_6_T'])
        output_nation['RENAMU_30'] = df_nation['P20_2_T']
        output_nation['RENAMU_31'] = df_nation['P20_2_T'] * 100 / (df_nation['P20_1_T'] + df_nation['P20_2_T'] + df_nation['P20_3_T'] + df_nation['P20_4_T'] + df_nation['P20_5_T'] + df_nation['P20_6_T'])
        output_nation['RENAMU_32'] = df_nation['P20_3_T']
        output_nation['RENAMU_33'] = df_nation['P20_3_T'] * 100 / (df_nation['P20_1_T'] + df_nation['P20_2_T'] + df_nation['P20_3_T'] + df_nation['P20_4_T'] + df_nation['P20_5_T'] + df_nation['P20_6_T'])
        output_nation['RENAMU_34'] = df_nation['P20_4_T']
        output_nation['RENAMU_35'] = df_nation['P20_4_T'] * 100 / (df_nation['P20_1_T'] + df_nation['P20_2_T'] + df_nation['P20_3_T'] + df_nation['P20_4_T'] + df_nation['P20_5_T'] + df_nation['P20_6_T'])
        output_nation['RENAMU_36'] = df_nation['P20_5_T'] + df_nation['P20_6_T']
        output_nation['RENAMU_37'] = (df_nation['P20_5_T'] + df_nation['P20_6_T']) * 100 / (df_nation['P20_1_T'] + df_nation['P20_2_T'] + df_nation['P20_3_T'] + df_nation['P20_4_T'] + df_nation['P20_5_T'] + df_nation['P20_6_T'])
        output_nation['RENAMU_38'] = df_nation['P20_1_T'] + df_nation['P20_2_T'] + df_nation['P20_3_T'] + df_nation['P20_4_T'] + df_nation['P20_5_T'] + df_nation['P20_6_T']
        output_nation['RENAMU_39'] = df_nation['P32_2_T']
        output_nation['RENAMU_40'] = df_nation['P32_2_T'] * 100 / (df_nation['P20_1_T'] + df_nation['P20_2_T'] + df_nation['P20_3_T'] + df_nation['P20_4_T'] + df_nation['P20_5_T'] + df_nation['P20_6_T'])
        output_nation['RENAMU_41'] = df_nation['_RENAMU_41']
        output_nation['RENAMU_42'] = df_nation['_RENAMU_42']
        output_nation['RENAMU_43'] = df_nation['_RENAMU_42'] * 100 / df_nation['_RENAMU_41']
        output_nation['RENAMU_44'] = df_nation['_RENAMU_44']
        output_nation['RENAMU_45'] = df_nation['_RENAMU_44'] * 100 / df_nation['_RENAMU_41']
        output_nation['RENAMU_46'] = df_nation['_RENAMU_46']
        output_nation['RENAMU_47'] = df_nation['_RENAMU_46'] * 100 / df_nation['_RENAMU_41']
        output_nation['RENAMU_48'] = df_nation['_RENAMU_48']
        output_nation['RENAMU_49'] = df_nation['_RENAMU_48'] * 100 / df_nation['_RENAMU_41']
        output_nation['RENAMU_50'] = df_nation['_RENAMU_50']
        output_nation['RENAMU_51'] = df_nation['_RENAMU_51']
        output_nation['RENAMU_52'] = df_nation['P66_1_1'] +  df_nation['P66_10_1'] +  df_nation['P66_2_1'] +  df_nation['P66_3_1'] +  df_nation['P66_4_1']  +  df_nation['P66_5_1'] +  df_nation['P66_6_1'] +  df_nation['P66_7_1'] +  df_nation['P66_8_1'] +  df_nation['P66_9_1'] 
        output_nation['RENAMU_53'] = df_nation['_RENAMU_53']
        output_nation['RENAMU_54'] = df_nation['_RENAMU_54']
        output_nation['RENAMU_55'] = np.nan
        output_nation['RENAMU_56'] = df_nation['_RENAMU_56'] * 100 / df_nation['count']
        output_nation['RENAMU_57'] = np.nan
        output_nation['RENAMU_58'] = df_nation['P10_4'] * 100 / df_nation['count']
        output_nation['RENAMU_59'] = np.nan
        output_nation['RENAMU_60'] = df_nation['P12A_3'] * 100 / df_nation['count']
        output_nation['RENAMU_61'] = np.nan
        output_nation['RENAMU_62'] = df_nation['P12A_6'] * 100 / df_nation['count']
        output_nation['RENAMU_63'] = np.nan
        output_nation['RENAMU_64'] = df_nation['P12A_9'] * 100 / df_nation['count']
        output_nation['RENAMU_65'] = np.nan
        output_nation['RENAMU_66'] = df_nation['P13_1_AUX'] * 100 / df_nation['count']
        output_nation['RENAMU_67'] = np.nan
        output_nation['RENAMU_68'] = df_nation['P13_2'] * 100 / df_nation['count']
        output_nation['RENAMU_69'] = np.nan
        output_nation['RENAMU_70'] = df_nation['P15'] * 100 / df_nation['count']
        output_nation['RENAMU_71'] = np.nan
        output_nation['RENAMU_72'] = df_nation['P16_1'] * 100 / df_nation['count']
        output_nation['RENAMU_73'] = np.nan
        output_nation['RENAMU_74'] = df_nation['P16_2'] * 100 / df_nation['count']
        output_nation['RENAMU_75'] = np.nan
        output_nation['RENAMU_76'] = df_nation['P16_3'] * 100 / df_nation['count']
        output_nation['RENAMU_77'] = np.nan
        output_nation['RENAMU_78'] = df_nation['P16_4'] * 100 / df_nation['count']
        output_nation['RENAMU_79'] = np.nan
        output_nation['RENAMU_80'] = df_nation['P16_5'] * 100 / df_nation['count']
        output_nation['RENAMU_81'] = np.nan
        output_nation['RENAMU_82'] = df_nation['_RENAMU_81'] * 100 / df_nation['count']
        output_nation['RENAMU_83'] = np.nan
        output_nation['RENAMU_84'] = df_nation['P31_1'] * 100 / df_nation['count']
        output_nation['RENAMU_85'] = np.nan
        output_nation['RENAMU_86'] = df_nation['P31_2'] * 100 / df_nation['count']
        output_nation['RENAMU_87'] = np.nan
        output_nation['RENAMU_88'] = df_nation['P31_3'] * 100 / df_nation['count']
        output_nation['RENAMU_89'] = np.nan
        output_nation['RENAMU_90'] = df_nation['P31_4'] * 100 / df_nation['count']
        output_nation['RENAMU_91'] = np.nan
        output_nation['RENAMU_92'] = df_nation['_RENAMU_91'] * 100 / df_nation['count']
        output_nation['RENAMU_93'] = np.nan
        output_nation['RENAMU_94'] = df_nation['P39_1'] * 100 / df_nation['count']
        output_nation['RENAMU_95'] = np.nan
        output_nation['RENAMU_96'] = df_nation['P39_2'] * 100 / df_nation['count']
        output_nation['RENAMU_97'] = np.nan
        output_nation['RENAMU_98'] = df_nation['P40A_1'] * 100 / df_nation['count']
        output_nation['RENAMU_99'] = np.nan
        output_nation['RENAMU_100'] = df_nation['P40A_2'] * 100 / df_nation['count']
        output_nation['RENAMU_101'] = np.nan
        output_nation['RENAMU_102'] = df_nation['P40A_4'] * 100 / df_nation['count']
        output_nation['RENAMU_103'] = np.nan
        output_nation['RENAMU_104'] = df_nation['P40A_3'] * 100 / df_nation['count']
        output_nation['RENAMU_105'] = np.nan
        output_nation['RENAMU_106'] = df_nation['P41_1_1_AUX'] * 100 / df_nation['count']
        output_nation['RENAMU_107'] = df_nation['P41_1_2_AUX'] * 100 / df_nation['count']
        output_nation['RENAMU_108'] = df_nation['P41_1_3_AUX'] * 100 / df_nation['count']
        output_nation['RENAMU_109'] = df_nation['P41_1_4_AUX'] * 100 / df_nation['count']
        output_nation['RENAMU_110'] = np.nan
        output_nation['RENAMU_111'] = df_nation['P43_1_1_AUX'] * 100 / df_nation['count']
        output_nation['RENAMU_112'] = df_nation['P43_1_2_AUX'] * 100 / df_nation['count']
        output_nation['RENAMU_113'] = df_nation['P43_1_3_AUX'] * 100 / df_nation['count']
        output_nation['RENAMU_114'] = df_nation['P43_1_4_AUX'] * 100 / df_nation['count']
        output_nation['RENAMU_115'] = np.nan
        output_nation['RENAMU_116'] = df_nation['P44_1'] * 100 / df_nation['count']
        output_nation['RENAMU_117'] = df_nation['P48_T']
        output_nation['RENAMU_118'] = np.nan
        output_nation['RENAMU_119'] = df_nation['P49_1'] * 100 / df_nation['count']
        output_nation['RENAMU_120'] = np.nan
        output_nation['RENAMU_121'] = df_nation['P49_2'] * 100 / df_nation['count']
        output_nation['RENAMU_122'] = np.nan
        output_nation['RENAMU_123'] = df_nation['P49_3'] * 100 / df_nation['count']
        output_nation['RENAMU_124'] = np.nan
        output_nation['RENAMU_125'] = df_nation['P49_4'] * 100 / df_nation['count']
        output_nation['RENAMU_126'] = np.nan
        output_nation['RENAMU_127'] = df_nation['P49_5'] * 100 / df_nation['count']
        output_nation['RENAMU_128'] = np.nan
        output_nation['RENAMU_129'] = df_nation['P49_6'] * 100 / df_nation['count']
        output_nation['RENAMU_130'] = np.nan
        output_nation['RENAMU_131'] = df_nation['P49_7'] * 100 / df_nation['count']
        output_nation['RENAMU_132'] = np.nan
        output_nation['RENAMU_133'] = df_nation['P49_8'] * 100 / df_nation['count']
        output_nation['RENAMU_134'] = np.nan
        output_nation['RENAMU_135'] = df_nation['_RENAMU_134'] * 100 / df_nation['count']
        output_nation['RENAMU_136'] = np.nan
        output_nation['RENAMU_137'] = df_nation['P52'] * 100 / df_nation['count']
        output_nation['RENAMU_138'] = np.nan
        output_nation['RENAMU_139'] = df_nation['_RENAMU_138'] * 100 / df_nation['count']
        output_nation['RENAMU_140'] = np.nan
        output_nation['RENAMU_141'] = df_nation['_RENAMU_140'] * 100 / df_nation['count']
        output_nation['RENAMU_142'] = df_nation['P52_1']
        output_nation['RENAMU_143'] = df_nation['P53A_1']
        output_nation['RENAMU_144'] = np.nan
        output_nation['RENAMU_145'] = df_nation['_RENAMU_144'] * 100 / df_nation['count']
        output_nation['RENAMU_146'] = df_nation['P60A_1_2']
        output_nation['RENAMU_147'] = np.nan
        output_nation['RENAMU_148'] = df_nation['_RENAMU_147'] * 100 / df_nation['count']
        output_nation['RENAMU_149'] = df_nation['P60A_2_1']
        output_nation['RENAMU_150'] = np.nan
        output_nation['RENAMU_151'] = df_nation['_RENAMU_150'] * 100 / df_nation['count']
        output_nation['RENAMU_152'] = df_nation['P60A_3_1']
        output_nation['RENAMU_153'] = np.nan
        output_nation['RENAMU_154'] = df_nation['P61'] * 100 / df_nation['count']
        output_nation['RENAMU_155'] = df_nation['P61_1']
        output_nation['RENAMU_156'] = np.nan
        output_nation['RENAMU_157'] = df_nation['_P62'] * 100 / df_nation['count']
        output_nation['RENAMU_158'] = df_nation['P62_1']
        output_nation['RENAMU_159'] = np.nan
        output_nation['RENAMU_160'] = df_nation['P64'] * 100 / df_nation['count']
        output_nation['RENAMU_161'] = np.nan
        output_nation['RENAMU_162'] = df_nation['P66'] * 100 / df_nation['count']
        output_nation['RENAMU_163'] = np.nan
        output_nation['RENAMU_164'] = df_nation['P69_2'] * 100 / df_nation['count']
        output_nation['RENAMU_165'] = df_nation['P69_2_T']
        output_nation['RENAMU_166'] = np.nan
        output_nation['RENAMU_167'] = df_nation['P75'] * 100 / df_nation['count']
        output_nation['RENAMU_168'] = np.nan
        output_nation['RENAMU_169'] = df_nation['P78_1'] * 100 / df_nation['count']
        output_nation['RENAMU_170'] = np.nan
        output_nation['RENAMU_171'] = df_nation['P78_2'] * 100 / df_nation['count']
        output_nation['RENAMU_172'] = np.nan
        output_nation['RENAMU_173'] = df_nation['P78_3'] * 100 / df_nation['count']
        output_nation['RENAMU_174'] = np.nan
        output_nation['RENAMU_175'] = df_nation['P97'] * 100 / df_nation['count']
        output_nation['RENAMU_176'] = df_nation['P98_1'] * 100 / df_nation['count']
        output_nation['RENAMU_177'] = df_nation['P98_1_1']
        output_nation['RENAMU_178'] = df_nation['P98_1_2']
        output_nation['RENAMU_179'] = df_nation['P98_2'] * 100 / df_nation['count']
        output_nation['RENAMU_180'] = df_nation['P98_2_1']
        output_nation['RENAMU_181'] = df_nation['P98_2_2']
        output_nation['RENAMU_182'] = df_nation['P98_3'] * 100 / df_nation['count']
        output_nation['RENAMU_183'] = df_nation['P98_3_1']
        output_nation['RENAMU_184'] = df_nation['P98_3_2']
        output_nation['RENAMU_185'] = df_nation['P98_4'] * 100 / df_nation['count']
        output_nation['RENAMU_186'] = df_nation['P98_4_1']
        output_nation['RENAMU_187'] = df_nation['P98_4_2']
        output_nation['RENAMU_188'] = df_nation['P98_5'] * 100 / df_nation['count']
        output_nation['RENAMU_189'] = df_nation['P98_5_1']
        output_nation['RENAMU_190'] = df_nation['P98_5_2']

        # Append indicators DataFrames
        output = output_nation.append([output_department, output_province, output_district], sort=False)

        output['nation_id'].fillna(0, inplace=True)
        output['department_id'].fillna(0, inplace=True)
        output['province_id'].fillna(0, inplace=True)
        output['district_id'].fillna(0, inplace=True)

        output['nation_id'] = output['nation_id'].astype(str)
        output['department_id'] = output['department_id'].astype(str)
        output['province_id'] = output['province_id'].astype(str)
        output['district_id'] = output['district_id'].astype(str)

        return output

class RENAMUPipeline(EasyPipeline):
    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open(params['connector']))

        transform_step = TransformStep()

        agg_step = AggregatorStep('inei_renamu_municipalities', measures=[
            'RENAMU_1', 'RENAMU_2', 'RENAMU_3', 'RENAMU_4', 'RENAMU_5', 'RENAMU_6', 'RENAMU_7', 'RENAMU_8', 'RENAMU_9', 'RENAMU_10', 'RENAMU_11',
            'RENAMU_12', 'RENAMU_13', 'RENAMU_14', 'RENAMU_15', 'RENAMU_16', 'RENAMU_17', 'RENAMU_18', 'RENAMU_19', 'RENAMU_20', 'RENAMU_21', 
            'RENAMU_22', 'RENAMU_23', 'RENAMU_24', 'RENAMU_25', 'RENAMU_26', 'RENAMU_27', 'RENAMU_28', 'RENAMU_29', 'RENAMU_30', 'RENAMU_31', 
            'RENAMU_32', 'RENAMU_33', 'RENAMU_34', 'RENAMU_35', 'RENAMU_36', 'RENAMU_37', 'RENAMU_38', 'RENAMU_39', 'RENAMU_40', 'RENAMU_41', 
            'RENAMU_42', 'RENAMU_43', 'RENAMU_44', 'RENAMU_45', 'RENAMU_46', 'RENAMU_47', 'RENAMU_48', 'RENAMU_49', 'RENAMU_50', 'RENAMU_51',
            'RENAMU_52', 'RENAMU_53', 'RENAMU_54', 'RENAMU_55', 'RENAMU_56', 'RENAMU_57', 'RENAMU_58', 'RENAMU_59', 'RENAMU_60', 'RENAMU_61', 
            'RENAMU_62', 'RENAMU_63', 'RENAMU_64', 'RENAMU_65', 'RENAMU_66', 'RENAMU_67', 'RENAMU_68', 'RENAMU_69', 'RENAMU_70', 'RENAMU_71',
            'RENAMU_72', 'RENAMU_73', 'RENAMU_74', 'RENAMU_75', 'RENAMU_76', 'RENAMU_77', 'RENAMU_78', 'RENAMU_79', 'RENAMU_80', 'RENAMU_81',
            'RENAMU_82', 'RENAMU_83', 'RENAMU_84', 'RENAMU_85', 'RENAMU_86', 'RENAMU_87', 'RENAMU_88', 'RENAMU_89', 'RENAMU_90', 'RENAMU_91',
            'RENAMU_92', 'RENAMU_93', 'RENAMU_94', 'RENAMU_95', 'RENAMU_96', 'RENAMU_97', 'RENAMU_98', 'RENAMU_99', 'RENAMU_100', 'RENAMU_101',
            'RENAMU_102', 'RENAMU_103', 'RENAMU_104', 'RENAMU_105', 'RENAMU_106', 'RENAMU_107', 'RENAMU_108', 'RENAMU_109', 'RENAMU_110', 'RENAMU_111',
            'RENAMU_112', 'RENAMU_113', 'RENAMU_114', 'RENAMU_115', 'RENAMU_116', 'RENAMU_117', 'RENAMU_118', 'RENAMU_119', 'RENAMU_120', 'RENAMU_121',
            'RENAMU_122', 'RENAMU_123', 'RENAMU_124', 'RENAMU_125', 'RENAMU_126', 'RENAMU_127', 'RENAMU_128', 'RENAMU_129', 'RENAMU_130', 'RENAMU_131',
            'RENAMU_132', 'RENAMU_133', 'RENAMU_134', 'RENAMU_135', 'RENAMU_136', 'RENAMU_137', 'RENAMU_138', 'RENAMU_139', 'RENAMU_140', 'RENAMU_141',
            'RENAMU_142', 'RENAMU_143', 'RENAMU_144', 'RENAMU_145', 'RENAMU_146', 'RENAMU_147', 'RENAMU_148', 'RENAMU_149', 'RENAMU_150', 'RENAMU_151',
            'RENAMU_152', 'RENAMU_153', 'RENAMU_154', 'RENAMU_155', 'RENAMU_156', 'RENAMU_157', 'RENAMU_158', 'RENAMU_159', 'RENAMU_160', 'RENAMU_161', 
            'RENAMU_162', 'RENAMU_163', 'RENAMU_164', 'RENAMU_165', 'RENAMU_166', 'RENAMU_167', 'RENAMU_168', 'RENAMU_169', 'RENAMU_170', 'RENAMU_171', 
            'RENAMU_172', 'RENAMU_173', 'RENAMU_174', 'RENAMU_175', 'RENAMU_176', 'RENAMU_177', 'RENAMU_178', 'RENAMU_179', 'RENAMU_180', 'RENAMU_181', 
            'RENAMU_182', 'RENAMU_183', 'RENAMU_184', 'RENAMU_185', 'RENAMU_186', 'RENAMU_187', 'RENAMU_188', 'RENAMU_189', 'RENAMU_190'
        ])

        load_step = LoadStep('inei_renamu_municipalities', db_connector, if_exists='drop', 
                             pk=['nation_id', 'department_id', 'province_id', 'district_id', 'year'], dtype=DTYPES,
                             nullable_list=[
                                'RENAMU_1', 'RENAMU_2', 'RENAMU_3', 'RENAMU_4', 'RENAMU_5', 'RENAMU_6', 'RENAMU_7', 'RENAMU_8', 'RENAMU_9', 'RENAMU_10', 'RENAMU_11',
                                'RENAMU_12', 'RENAMU_13', 'RENAMU_14', 'RENAMU_15', 'RENAMU_16', 'RENAMU_17', 'RENAMU_18', 'RENAMU_19', 'RENAMU_20', 'RENAMU_21', 
                                'RENAMU_22', 'RENAMU_23', 'RENAMU_24', 'RENAMU_25', 'RENAMU_26', 'RENAMU_27', 'RENAMU_28', 'RENAMU_29', 'RENAMU_30', 'RENAMU_31', 
                                'RENAMU_32', 'RENAMU_33', 'RENAMU_34', 'RENAMU_35', 'RENAMU_36', 'RENAMU_37', 'RENAMU_38', 'RENAMU_39', 'RENAMU_40', 'RENAMU_41', 
                                'RENAMU_42', 'RENAMU_43', 'RENAMU_44', 'RENAMU_45', 'RENAMU_46', 'RENAMU_47', 'RENAMU_48', 'RENAMU_49', 'RENAMU_50', 'RENAMU_51',
                                'RENAMU_52', 'RENAMU_53', 'RENAMU_54', 'RENAMU_55', 'RENAMU_56', 'RENAMU_57', 'RENAMU_58', 'RENAMU_59', 'RENAMU_60', 'RENAMU_61', 
                                'RENAMU_62', 'RENAMU_63', 'RENAMU_64', 'RENAMU_65', 'RENAMU_66', 'RENAMU_67', 'RENAMU_68', 'RENAMU_69', 'RENAMU_70', 'RENAMU_71',
                                'RENAMU_72', 'RENAMU_73', 'RENAMU_74', 'RENAMU_75', 'RENAMU_76', 'RENAMU_77', 'RENAMU_78', 'RENAMU_79', 'RENAMU_80', 'RENAMU_81',
                                'RENAMU_82', 'RENAMU_83', 'RENAMU_84', 'RENAMU_85', 'RENAMU_86', 'RENAMU_87', 'RENAMU_88', 'RENAMU_89', 'RENAMU_90', 'RENAMU_91',
                                'RENAMU_92', 'RENAMU_93', 'RENAMU_94', 'RENAMU_95', 'RENAMU_96', 'RENAMU_97', 'RENAMU_98', 'RENAMU_99', 'RENAMU_100', 'RENAMU_101',
                                'RENAMU_102', 'RENAMU_103', 'RENAMU_104', 'RENAMU_105', 'RENAMU_106', 'RENAMU_107', 'RENAMU_108', 'RENAMU_109', 'RENAMU_110', 'RENAMU_111',
                                'RENAMU_112', 'RENAMU_113', 'RENAMU_114', 'RENAMU_115', 'RENAMU_116', 'RENAMU_117', 'RENAMU_118', 'RENAMU_119', 'RENAMU_120', 'RENAMU_121',
                                'RENAMU_122', 'RENAMU_123', 'RENAMU_124', 'RENAMU_125', 'RENAMU_126', 'RENAMU_127', 'RENAMU_128', 'RENAMU_129', 'RENAMU_130', 'RENAMU_131',
                                'RENAMU_132', 'RENAMU_133', 'RENAMU_134', 'RENAMU_135', 'RENAMU_136', 'RENAMU_137', 'RENAMU_138', 'RENAMU_139', 'RENAMU_140', 'RENAMU_141',
                                'RENAMU_142', 'RENAMU_143', 'RENAMU_144', 'RENAMU_145', 'RENAMU_146', 'RENAMU_147', 'RENAMU_148', 'RENAMU_149', 'RENAMU_150', 'RENAMU_151',
                                'RENAMU_152', 'RENAMU_153', 'RENAMU_154', 'RENAMU_155', 'RENAMU_156', 'RENAMU_157', 'RENAMU_158', 'RENAMU_159', 'RENAMU_160', 'RENAMU_161', 
                                'RENAMU_162', 'RENAMU_163', 'RENAMU_164', 'RENAMU_165', 'RENAMU_166', 'RENAMU_167', 'RENAMU_168', 'RENAMU_169', 'RENAMU_170', 'RENAMU_171', 
                                'RENAMU_172', 'RENAMU_173', 'RENAMU_174', 'RENAMU_175', 'RENAMU_176', 'RENAMU_177', 'RENAMU_178', 'RENAMU_179', 'RENAMU_180', 'RENAMU_181', 
                                'RENAMU_182', 'RENAMU_183', 'RENAMU_184', 'RENAMU_185', 'RENAMU_186', 'RENAMU_187', 'RENAMU_188', 'RENAMU_189', 'RENAMU_190'])

        return [transform_step, load_step]


def run_pipeline(params: dict):
    pp = RENAMUPipeline()
    pp.run(params)


if __name__ == "__main__":
    import sys
    from os import path
    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline({
        "connector": path.join(__dirname, "..", "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
