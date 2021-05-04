DTYPES = {
    'dimension_table': {
        'cultivo_id':           'String',
        'cultivo_name':         'String'
    },
    'fact_table': {
        'cultivo_id':                       'String', 
        'district_id':                      'String', 
        'month_id':                         'UInt32', 
        'tipo':                             'UInt8', 
        'superficie_sembrada':              'Float32',
        'superficie_sembrada_unidad':       'UInt8', 
        'superficie_cosechada':             'Float32',
        'superficie_cosechada_unidad':      'UInt8', 
        'produccion':                       'Float32', 
        'produccion_unidad':                'UInt8',
        'rendimiento':                      'Float32', 
        'rendimiento_unidad':               'UInt8', 
        'precio':                           'Float32', 
        'precio_unidad':                    'UInt8'
    }
}

PRIMARY_KEYS = {
    'dimension_table': ['cultivo_id'],
    'fact_table': ['cultivo_id', 'district_id', 'month_id', 'tipo']
}

RENAME_COLUMNS = {
    'COD_CULTIVO': 'cultivo_id',
    'CULTIVO': 'cultivo_name',
    'TIPO': 'tipo',
    'ANIO': 'year',
    'MES': 'month',
    'UBIGEO': 'district_id',
    'SUPERFICIE_SEMBRADA': 'superficie_sembrada',
    'UNIDAD': 'superficie_sembrada_unidad',
    'SUPERFICIE_COSECHADA': 'superficie_cosechada',
    'UNIDAD.1': 'superficie_cosechada_unidad',
    'PRODUCCION': 'produccion',
    'UNIDAD.2': 'produccion_unidad',
    'RENDIMIENTO': 'rendimiento',
    'UNIDAD_4': 'rendimiento_unidad',
    'PRECIO_CHACRA': 'precio',
    'UNIDAD.3': 'precio_unidad'
}

REPLACE_VALUES = {
    'tipo': {
        'TRANSITORIOS': 1,
        'PERMANENTES': 2,
        'CULTIVOS DE MANEJO ESPECIAL': 3,
        'PASTOS': 4
    },
    'superficie_sembrada_unidad': {
        'ha': 1
    },
    'superficie_cosechada_unidad': {
        'ha': 1
    },
    'produccion_unidad': {
        't': 1
    },
    'rendimiento_unidad': {
        'kg/ha': 1
    },
    'precio_unidad': {
        'soles/kg': 1
    }
}