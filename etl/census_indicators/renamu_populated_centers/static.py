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
    'dimension_table': {
        'nation_id':                'String',
        'nation_name':              'String',
        'department_id':            'String',
        'department_name':          'String',
        'province_id':              'String',
        'province_name':            'String',
        'populated_center_id':      'String',
        'populated_center_name':    'String'
    },
    'fact_table': {
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
}

NULLABLE_LISTS = {
    'dimension_table': [],
    'fact_table': {
        'CCPP_1', 'CCPP_2', 'CCPP_3', 'CCPP_4', 'CCPP_5',
        'CCPP_6', 'CCPP_7', 'CCPP_8', 'CCPP_9', 'CCPP_10', 'CCPP_11', 'CCPP_12',
        'CCPP_13', 'CCPP_14', 'CCPP_15', 'CCPP_16', 'CCPP_17', 'CCPP_18',
        'CCPP_19', 'CCPP_20', 'CCPP_21', 'CCPP_22', 'CCPP_23', 'CCPP_24',
        'CCPP_25', 'CCPP_26', 'CCPP_27', 'CCPP_28', 'CCPP_29', 'CCPP_30',
        'CCPP_31', 'CCPP_32', 'CCPP_33', 'CCPP_34', 'CCPP_35', 'CCPP_36',
        'CCPP_37', 'CCPP_38', 'CCPP_39', 'CCPP_40', 'CCPP_41', 'CCPP_42',
        'CCPP_43', 'CCPP_44', 'CCPP_45', 'CCPP_46', 'CCPP_47', 'CCPP_48',
        'CCPP_49', 'CCPP_50', 'CCPP_51', 'CCPP_52', 'CCPP_53', 'CCPP_54',
        'CCPP_55', 'CCPP_56', 'CCPP_57', 'CCPP_58', 'CCPP_59', 'CCPP_60',
        'CCPP_61', 'CCPP_62', 'CCPP_63', 'CCPP_64', 'CCPP_65', 'CCPP_66',
        'CCPP_67', 'CCPP_68', 'CCPP_69', 'CCPP_70', 'CCPP_71', 'CCPP_72',
        'CCPP_73', 'CCPP_74', 'CCPP_75', 'CCPP_76', 'CCPP_77', 'CCPP_78',
        'CCPP_79', 'CCPP_80', 'CCPP_81', 'CCPP_82', 'CCPP_83', 'CCPP_84',
        'CCPP_85', 'CCPP_86', 'CCPP_87', 'CCPP_88', 'CCPP_89', 'CCPP_90'
    }
}

PRIMARY_KEYS = {
    'dimension_table': ['province_id'],
    'fact_table': ['nation_id', 'department_id', 'province_id', 'populated_center_id', 'year']
}

SELECTED_COLUMNS = { 
    2019: [
        'populated_center_id', 'populated_center_name', 'P11_01_M', 'P11_01_H', 'P11_02_M', 'P11_02_H', 'P12', 'P13', 'P14', 'P14_01',
        'P16_03', 'P16_04', 'P16_05', 'P16_06', 'P16_07', 'P16_08', 'P16_09', 'P16_10', 'P16_11',
        'P16_12', 'P16_13', 'P16_14', 'P16_01', 'P16_02', 'P17', 'P17_01', 'P18_T', 'P18A_01', 'P18A_02',
        'P18A_03'  
    ],
    2018: [
        'populated_center_id', 'populated_center_name', 'P11_01_M', 'P11_01_H', 'P11_02_M', 'P11_02_H', 'P12', 'P13', 'P14', 'P14_01',
        'P16_03', 'P16_04', 'P16_05', 'P16_06', 'P16_07', 'P16_08', 'P16_09', 'P16_10', 'P16_11',
        'P16_12', 'P16_13', 'P16_14', 'P16_01', 'P16_02', 'P17', 'P17_01', 'P18_T', 'P18A_01', 'P18A_02',
        'P18A_03' 
    ],
    2017: [
        'populated_center_id', 'populated_center_name', 'P10_01_M', 'P10_01_H', 'P10_02_M', 'P10_02_H', 'P11', 'P12_01', 'P15',
        'P15_01', 'P16A_01', 'P16A_02', 'P16A_03', 'P16A_04'
    ],
    2016: [
        'populated_center_id', 'populated_center_name', 'P10_01_M', 'P10_01_H', 'P10_02_M', 'P10_02_H', 'P11', 'P12_01', 'P15',
        'P15_01', 'P16A_01', 'P16A_02', 'P16A_03', 'P16A_04'
    ],
    2015: [
        'populated_center_id', 'populated_center_name', 'P10_01_M', 'P10_01_H', 'P10_02_M', 'P10_02_H', 'P11', 'P12_01', 'P15',
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
        'populated_center_name': {
            'Humanhuaco': 'Humanhuauco',
            'San Miguel de Yuncay': 'San Miguel de Juncay',
            'Ramal de Azpuzana': 'Ramal de Aspuzana',
            'Yacango - Juli': 'Yacango Kanccora',
            'Turistico Umayo Sillustani': 'Turístico Umayo de Sillustani',
            'Turistico Umayo de Sillustani': 'Turístico Umayo de Sillustani',
            'Uros - Chulluni': 'Turística de Uros Chulluni',
            'Locuto y Anexos - Margen Izquierda del Rio Piura': 'Locuto',
            'Santa Ana - Margen Izquierda del Río Piura': 'Santa Ana',
            'San Martín Cp.3': 'San Martín CP3',
            'Tsirotsi Mayni - San Francisco de Cahuapanas': 'San Francisco de Cahuapanas',
            'Rocchacc': 'Union Quispimarca',
            'Flor de K Antu': "Flor de K'Antu",
            'Vista Alegre de Ccarhuacco': 'Vista Alegre de Ccarhuaccocco',
            'Porcon La Esperanza': 'Porcón La Esperanza',
            'Las Lagunas': 'Lagunas',
            'Cadmalca': 'Cadmalca Alto',
            'Quengo Río Bajo': 'Quengorío',
            'Pampa de la Calzada': 'Pampa La Calzada',
            'Naranjal-Paltaybamba': 'Paltaybamba Tablada',
            'Andayaque': 'Markjopata',
            'Jerusalen': 'Jerusalén',
            'Barrio Tayacaja': 'Tayacaja',
            'Poccyac': 'Poccyacc',
            'Villa Juancito': 'Juancito'
        }
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
        'populated_center_name': {
            'Humanhuaco': 'Humanhuauco',
            'San Miguel de Yuncay': 'San Miguel de Juncay',
            'Ramal de Azpuzana': 'Ramal de Aspuzana',
            'Yacango - Juli': 'Yacango Kanccora',
            'Turistico Umayo Sillustani': 'Turístico Umayo de Sillustani',
            'Turistico Umayo de Sillustani': 'Turístico Umayo de Sillustani',
            'Uros - Chulluni': 'Turística de Uros Chulluni',
            'Locuto y Anexos - Margen Izquierda del Rio Piura': 'Locuto',
            'Santa Ana - Margen Izquierda del Río Piura': 'Santa Ana',
            'San Martín Cp.3': 'San Martín CP3',
            'Tsirotsi Mayni - San Francisco de Cahuapanas': 'San Francisco de Cahuapanas',
            'Rocchacc': 'Union Quispimarca',
            'Flor de K Antu': "Flor de K'Antu",
            'Vista Alegre de Ccarhuacco': 'Vista Alegre de Ccarhuaccocco',
            'Porcon La Esperanza': 'Porcón La Esperanza',
            'Las Lagunas': 'Lagunas',
            'Cadmalca': 'Cadmalca Alto',
            'Quengo Río Bajo': 'Quengorío',
            'Pampa de la Calzada': 'Pampa La Calzada',
            'Naranjal-Paltaybamba': 'Paltaybamba Tablada',
            'Andayaque': 'Markjopata',
            'Jerusalen': 'Jerusalén',
            'Barrio Tayacaja': 'Tayacaja',
            'Poccyac': 'Poccyacc',
            'Villa Juancito': 'Juancito'
        }
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
        'populated_center_name': {
            'Humanhuaco': 'Humanhuauco',
            'San Miguel de Yuncay': 'San Miguel de Juncay',
            'Ramal de Azpuzana': 'Ramal de Aspuzana',
            'Yacango - Juli': 'Yacango Kanccora',
            'Turistico Umayo Sillustani': 'Turístico Umayo de Sillustani',
            'Turistico Umayo de Sillustani': 'Turístico Umayo de Sillustani',
            'Uros - Chulluni': 'Turística de Uros Chulluni',
            'Locuto y Anexos - Margen Izquierda del Rio Piura': 'Locuto',
            'Santa Ana - Margen Izquierda del Río Piura': 'Santa Ana',
            'San Martín Cp.3': 'San Martín CP3',
            'Tsirotsi Mayni - San Francisco de Cahuapanas': 'San Francisco de Cahuapanas',
            'Rocchacc': 'Union Quispimarca',
            'Flor de K Antu': "Flor de K'Antu",
            'Vista Alegre de Ccarhuacco': 'Vista Alegre de Ccarhuaccocco',
            'Porcon La Esperanza': 'Porcón La Esperanza',
            'Las Lagunas': 'Lagunas',
            'Cadmalca': 'Cadmalca Alto',
            'Quengo Río Bajo': 'Quengorío',
            'Pampa de la Calzada': 'Pampa La Calzada',
            'Naranjal-Paltaybamba': 'Paltaybamba Tablada',
            'Andayaque': 'Markjopata',
            'Jerusalen': 'Jerusalén',
            'Barrio Tayacaja': 'Tayacaja',
            'Poccyac': 'Poccyacc',
            'Villa Juancito': 'Juancito'
        }
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
        'populated_center_name': {
            'Humanhuaco': 'Humanhuauco',
            'San Miguel de Yuncay': 'San Miguel de Juncay',
            'Ramal de Azpuzana': 'Ramal de Aspuzana',
            'Yacango - Juli': 'Yacango Kanccora',
            'Turistico Umayo Sillustani': 'Turístico Umayo de Sillustani',
            'Turistico Umayo de Sillustani': 'Turístico Umayo de Sillustani',
            'Uros - Chulluni': 'Turística de Uros Chulluni',
            'Locuto y Anexos - Margen Izquierda del Rio Piura': 'Locuto',
            'Santa Ana - Margen Izquierda del Río Piura': 'Santa Ana',
            'San Martín Cp.3': 'San Martín CP3',
            'Tsirotsi Mayni - San Francisco de Cahuapanas': 'San Francisco de Cahuapanas',
            'Rocchacc': 'Union Quispimarca',
            'Flor de K Antu': "Flor de K'Antu",
            'Vista Alegre de Ccarhuacco': 'Vista Alegre de Ccarhuaccocco',
            'Porcon La Esperanza': 'Porcón La Esperanza',
            'Las Lagunas': 'Lagunas',
            'Cadmalca': 'Cadmalca Alto',
            'Quengo Río Bajo': 'Quengorío',
            'Pampa de la Calzada': 'Pampa La Calzada',
            'Naranjal-Paltaybamba': 'Paltaybamba Tablada',
            'Andayaque': 'Markjopata',
            'Jerusalen': 'Jerusalén',
            'Barrio Tayacaja': 'Tayacaja',
            'Poccyac': 'Poccyacc',
            'Villa Juancito': 'Juancito'
        }
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
        'populated_center_name': {
            'Humanhuaco': 'Humanhuauco',
            'San Miguel de Yuncay': 'San Miguel de Juncay',
            'Ramal de Azpuzana': 'Ramal de Aspuzana',
            'Yacango - Juli': 'Yacango Kanccora',
            'Turistico Umayo Sillustani': 'Turístico Umayo de Sillustani',
            'Turistico Umayo de Sillustani': 'Turístico Umayo de Sillustani',
            'Uros - Chulluni': 'Turística de Uros Chulluni',
            'Locuto y Anexos - Margen Izquierda del Rio Piura': 'Locuto',
            'Santa Ana - Margen Izquierda del Río Piura': 'Santa Ana',
            'San Martín Cp.3': 'San Martín CP3',
            'Tsirotsi Mayni - San Francisco de Cahuapanas': 'San Francisco de Cahuapanas',
            'Rocchacc': 'Union Quispimarca',
            'Flor de K Antu': "Flor de K'Antu",
            'Vista Alegre de Ccarhuacco': 'Vista Alegre de Ccarhuaccocco',
            'Porcon La Esperanza': 'Porcón La Esperanza',
            'Las Lagunas': 'Lagunas',
            'Cadmalca': 'Cadmalca Alto',
            'Quengo Río Bajo': 'Quengorío',
            'Pampa de la Calzada': 'Pampa La Calzada',
            'Naranjal-Paltaybamba': 'Paltaybamba Tablada',
            'Andayaque': 'Markjopata',
            'Jerusalen': 'Jerusalén',
            'Barrio Tayacaja': 'Tayacaja',
            'Poccyac': 'Poccyacc',
            'Villa Juancito': 'Juancito'
        }
    }
}