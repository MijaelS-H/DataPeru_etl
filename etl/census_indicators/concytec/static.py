COLUMNS_RENAME = {
    'IDCI': 'research_center_id',
    'ID': 'institute_id',
    'CCDD': 'department_id',
    'CCPP':'province_id',
    'CCDI': 'district_id'
}

LIST_DICT = [
    {'instituto público de investigación': 1, #P213
     'educación superior (universidad)': 2,
     'salud': 3, 
     'institución privada sin fines de lucro': 4,
     'otro': 5  
    },
    {'sociedad de responsabilidad limitada': 1, #P214
     'sociedad colectiva': 2,
     'sociedad anónima cerrada': 3,
     'sociedad anónima abierta': 4,
     'pública': 5,
     'asociación': 6,
     'otra': 7
    },
    {'sí': 1, #Bin
     'no': 0,
     'nan': 0,
     '9.0': 0
    }
]

DTYPE = {
    'nation_id':        'String',
    'department_id':    'String',
    'province_id':      'String',
    'district_id':      'String',
    'year':             'UInt16',
    'CONCYTEC_1':       'UInt32', 
    'CONCYTEC_2':       'Float32', 
    'CONCYTEC_3':       'UInt32', 
    'CONCYTEC_4':       'Float32', 
    'CONCYTEC_5':       'UInt32', 
    'CONCYTEC_6':       'Float32', 
    'CONCYTEC_7':       'UInt32', 
    'CONCYTEC_8':       'Float32', 
    'CONCYTEC_9':       'UInt32', 
    'CONCYTEC_10':      'Float32', 
    'CONCYTEC_11':      'UInt32', 
    'CONCYTEC_12':      'Float32', 
    'CONCYTEC_13':      'UInt32', 
    'CONCYTEC_14':      'Float32', 
    'CONCYTEC_15':      'UInt32', 
    'CONCYTEC_16':      'Float32', 
    'CONCYTEC_17':      'UInt32', 
    'CONCYTEC_18':      'Float32', 
    'CONCYTEC_19':      'UInt32', 
    'CONCYTEC_20':      'Float32', 
    'CONCYTEC_21':      'UInt32', 
    'CONCYTEC_22':      'Float32', 
    'CONCYTEC_23':      'UInt32', 
    'CONCYTEC_24':      'Float32', 
    'CONCYTEC_25':      'Float32', 
    'CONCYTEC_26':      'Float32', 
    'CONCYTEC_27':      'Float32', 
    'CONCYTEC_28':      'Float32', 
    'CONCYTEC_29':      'Float32', 
    'CONCYTEC_30':      'Float32', 
    'CONCYTEC_31':      'Float32', 
    'CONCYTEC_32':      'Float32', 
    'CONCYTEC_33':      'Float32', 
    'CONCYTEC_34':      'Float32', 
    'CONCYTEC_35':      'Float32', 
    'CONCYTEC_36':      'Float32', 
    'CONCYTEC_37':      'Float32', 
    'CONCYTEC_38':      'Float32', 
    'CONCYTEC_39':      'Float32', 
    'CONCYTEC_40':      'Float32', 
    'CONCYTEC_41':      'Float32', 
    'CONCYTEC_42':      'Float32', 
    'CONCYTEC_43':      'Float32', 
    'CONCYTEC_44':      'Float32', 
    'CONCYTEC_45':      'Float32', 
    'CONCYTEC_46':      'Float32', 
    'CONCYTEC_47':      'Float32', 
    'CONCYTEC_48':      'Float32', 
    'CONCYTEC_49':      'Float32', 
    'CONCYTEC_50':      'Float32', 
    'CONCYTEC_51':      'Float32', 
    'CONCYTEC_52':      'Float32', 
    'CONCYTEC_53':      'Float32', 
    'CONCYTEC_54':      'Float32', 
    'CONCYTEC_55':      'Float32', 
    'CONCYTEC_56':      'Float32', 
    'CONCYTEC_57':      'Float32', 
    'CONCYTEC_58':      'Float32', 
    'CONCYTEC_59':      'Float32',
    'CONCYTEC_60':      'Float32', 
    'CONCYTEC_61':      'Float32', 
    'CONCYTEC_62':      'Float32', 
    'CONCYTEC_63':      'Float32', 
    'CONCYTEC_64':      'Float32', 
    'CONCYTEC_65':      'Float32',
    'CONCYTEC_66':      'Float32',
    'CONCYTEC_67':      'Float32',
    'CONCYTEC_68':      'Float32', 
    'CONCYTEC_69':      'Float32', 
    'CONCYTEC_70':      'Float32', 
    'CONCYTEC_71':      'Float32', 
    'CONCYTEC_72':      'Float32', 
    'CONCYTEC_73':      'Float32', 
    'CONCYTEC_74':      'Float32', 
    'CONCYTEC_75':      'Float32', 
    'CONCYTEC_76':      'Float32', 
    'CONCYTEC_77':      'Float32', 
    'CONCYTEC_78':      'Float32',
    'CONCYTEC_79':      'Float32', 
    'CONCYTEC_80':      'Float32', 
    'CONCYTEC_81':      'Float32', 
    'CONCYTEC_82':      'Float32'
}

LIST_NULL = [
    'CONCYTEC_25', 'CONCYTEC_26', 'CONCYTEC_27', 'CONCYTEC_28', 'CONCYTEC_29', 'CONCYTEC_30', 'CONCYTEC_31', 'CONCYTEC_32', 
    'CONCYTEC_33', 'CONCYTEC_34', 'CONCYTEC_35', 'CONCYTEC_36', 'CONCYTEC_37', 'CONCYTEC_38', 'CONCYTEC_39', 'CONCYTEC_40', 
    'CONCYTEC_41', 'CONCYTEC_42', 'CONCYTEC_43', 'CONCYTEC_44', 'CONCYTEC_45', 'CONCYTEC_46', 'CONCYTEC_47', 'CONCYTEC_48', 
    'CONCYTEC_49', 'CONCYTEC_50', 'CONCYTEC_51', 'CONCYTEC_52', 'CONCYTEC_53', 'CONCYTEC_54', 'CONCYTEC_55', 'CONCYTEC_56', 
    'CONCYTEC_57', 'CONCYTEC_58', 'CONCYTEC_59', 'CONCYTEC_60', 'CONCYTEC_61', 'CONCYTEC_62', 'CONCYTEC_63', 'CONCYTEC_64', 
    'CONCYTEC_65', 'CONCYTEC_66', 'CONCYTEC_67', 'CONCYTEC_68', 'CONCYTEC_69', 'CONCYTEC_70', 'CONCYTEC_71', 'CONCYTEC_72', 
    'CONCYTEC_73', 'CONCYTEC_74', 'CONCYTEC_75', 'CONCYTEC_76', 'CONCYTEC_77', 'CONCYTEC_78', 'CONCYTEC_79', 'CONCYTEC_80', 
    'CONCYTEC_81', 'CONCYTEC_82' 
]