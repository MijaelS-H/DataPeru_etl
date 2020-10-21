COLUMNS_DICT = {
    'P10_01_M': 'P11_01_M',
    'P10_01_H': 'P11_01_H',
    'P10_02_M': 'P11_02_M',
    'P10_02_H': 'P11_02_H',
    'P11': 'P13',
    'P12_01': 'P14_01',
    'P15': 'P17',
    'P15_01': 'P17_01',
    'P16A_01': 'P18A_01',
    'P16A_02': 'P18A_02',
    'P16A_03': 'P18A_03',
    'P16A_04': 'P18A_04',
}

DTYPES = {
    'nation_id':                'String',
    'department_id':            'String',
    'province_id':              'String',
    'populated_center_id':      'String',
    'year':                     'UInt16',
    'CCPP_1':                   'Float32',
    'CCPP_2':                   'Float32',
    'CCPP_3':                   'Float32',
    'CCPP_4':                   'Float32',
    'CCPP_5':                   'Float32',
    'CCPP_6':                   'Float32',
    'CCPP_7':                   'Float32',
    'CCPP_8':                   'Float32',
    'CCPP_9':                   'Float32',
    'CCPP_10':                  'Float32',
    'CCPP_11':                  'Float32',
    'CCPP_12':                  'Float32',
    'CCPP_13':                  'Float32',
    'CCPP_14':                  'Float32',
    'CCPP_15':                  'Float32',
    'CCPP_16':                  'Float32',
    'CCPP_17':                  'Float32',
    'CCPP_18':                  'Float32',
    'CCPP_19':                  'Float32',
    'CCPP_20':                  'Float32',
    'CCPP_21':                  'Float32',
    'CCPP_22':                  'Float32',
    'CCPP_23':                  'Float32',
    'CCPP_24':                  'Float32',
    'CCPP_25':                  'Float32',
    'CCPP_26':                  'Float32',
    'CCPP_27':                  'Float32',
    'CCPP_28':                  'Float32',
    'CCPP_29':                  'Float32',
    'CCPP_30':                  'Float32',
    'CCPP_31':                  'Float32',
    'CCPP_32':                  'Float32',
    'CCPP_33':                  'Float32',
    'CCPP_34':                  'Float32',
    'CCPP_35':                  'Float32',
    'CCPP_36':                  'Float32',
    'CCPP_37':                  'Float32',
    'CCPP_38':                  'Float32',
    'CCPP_39':                  'Float32',
    'CCPP_40':                  'Float32',
    'CCPP_41':                  'Float32',
    'CCPP_42':                  'Float32',
    'CCPP_43':                  'Float32',
    'CCPP_44':                  'Float32',
    'CCPP_45':                  'Float32',
    'CCPP_46':                  'Float32',
    'CCPP_47':                  'Float32',
    'CCPP_48':                  'Float32',
    'CCPP_49':                  'Float32',
    'CCPP_50':                  'Float32',
    'CCPP_51':                  'Float32',
    'CCPP_52':                  'Float32',
    'CCPP_53':                  'Float32',
    'CCPP_54':                  'Float32',
    'CCPP_55':                  'Float32',
    'CCPP_56':                  'Float32',
    'CCPP_57':                  'Float32',
    'CCPP_58':                  'Float32',
    'CCPP_59':                  'Float32',
    'CCPP_60':                  'Float32',
    'CCPP_61':                  'Float32',
    'CCPP_62':                  'Float32',
    'CCPP_63':                  'Float32',
    'CCPP_64':                  'Float32',
    'CCPP_65':                  'Float32',
    'CCPP_66':                  'Float32',
    'CCPP_67':                  'Float32',
    'CCPP_68':                  'Float32',
    'CCPP_69':                  'Float32',
    'CCPP_70':                  'Float32',
    'CCPP_71':                  'Float32',
    'CCPP_72':                  'Float32',
    'CCPP_73':                  'Float32',
    'CCPP_74':                  'Float32',
    'CCPP_75':                  'Float32',
    'CCPP_76':                  'Float32',
    'CCPP_77':                  'Float32',
    'CCPP_78':                  'Float32',
    'CCPP_79':                  'Float32',
    'CCPP_80':                  'Float32',
    'CCPP_81':                  'Float32',
    'CCPP_82':                  'Float32',
    'CCPP_83':                  'Float32',
    'CCPP_84':                  'Float32',
    'CCPP_85':                  'Float32',
    'CCPP_86':                  'Float32',
    'CCPP_87':                  'Float32',
    'CCPP_88':                  'Float32',
    'CCPP_89':                  'Float32',
    'CCPP_90':                  'Float32'
}

SELECTED_COLUMNS = { 
    2019: [
        'populated_center_id', 'P11_01_M', 'P11_01_H', 'P11_02_M', 'P11_02_H', 'P12', 'P13', 'P14', 'P14_01',
        'P16_03', 'P16_04', 'P16_05', 'P16_06', 'P16_07', 'P16_08', 'P16_09', 'P16_10', 'P16_11',
        'P16_12', 'P16_13', 'P16_14', 'P16_01', 'P16_02', 'P17', 'P17_01', 'P18_T', 'P18A_01', 'P18A_02',
        'P18A_03'  
    ],
    2018: [
        'populated_center_id', 'P11_01_M', 'P11_01_H', 'P11_02_M', 'P11_02_H', 'P12', 'P13', 'P14', 'P14_01',
        'P16_03', 'P16_04', 'P16_05', 'P16_06', 'P16_07', 'P16_08', 'P16_09', 'P16_10', 'P16_11',
        'P16_12', 'P16_13', 'P16_14', 'P16_01', 'P16_02', 'P17', 'P17_01', 'P18_T', 'P18A_01', 'P18A_02',
        'P18A_03' 
    ],
    2017: [
        'populated_center_id', 'P10_01_M', 'P10_01_H', 'P10_02_M', 'P10_02_H', 'P11', 'P12_01', 'P15',
        'P15_01', 'P16A_01', 'P16A_02', 'P16A_03', 'P16A_04'
    ],
    2016: [
        'populated_center_id', 'P10_01_M', 'P10_01_H', 'P10_02_M', 'P10_02_H', 'P11', 'P12_01', 'P15',
        'P15_01', 'P16A_01', 'P16A_02', 'P16A_03', 'P16A_04'
    ],
    2015: [
        'populated_center_id', 'P10_01_M', 'P10_01_H', 'P10_02_M', 'P10_02_H', 'P11', 'P12_01', 'P15',
        'P15_01', 'P16A_01', 'P16A_02', 'P16A_03', 'P16A_04'
    ] 
}

VARIABLES_DICT = {
    2019: {
        'P12': {
            'Sí': 1,
            'No': 0
        },
        'P13': {
            'Realiza': 1,
            'No realiza': 0
        },
        'P14': {
            'Sí': 1,
            'No realiza': 0
        },
        'P17': {
            'Sí': 1,
            'No recibió': 0
        },
    },
    2018: {
        'P12': {
            'Sí': 1,
            'No cuenta': 0
        },
        'P13': {
            'Realiza': 1,
            'No realiza': 0
        },
        'P14': {
            'Sí': 1,
            'No realiza': 0
        },
        'P17': {
            'Sí': 1,
            'No recibió': 0
        },
    },
    2017: {
        'P13': {
            'Realiza': 1,
            'No realiza': 0
        },
        'P17': {
            'Recibió': 1,
            'No recibió': 0
        },
    },
    2016: {
        'P13': {
            'Realiza': 1,
            'No realiza': 0
        },
        'P17': {
            'Recibió': 1,
            'No recibió': 0
        },
    },
    2015: {
        'P13': {
            'Tiene': 1,
            'No tiene': 0
        },
        'P17': {
            'Si recibió': 1,
            'No recibió': 0
        },
    }
}