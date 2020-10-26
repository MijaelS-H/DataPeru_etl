DTYPES = {
    'dimension_table': {
        'producto_id':           'UInt8',
        'producto_name':         'String'
    },
    'fact_table': {
        'producto_id':                      'UInt8', 
        'department_id':                    'String',
        'month_id':                         'UInt32',
        'produccion':                       'Float32',
        'produccion_unidad':                'UInt8'
    }
}

PRIMARY_KEYS = {
    'dimension_table': ['producto_id'],
    'fact_table': ['producto_id', 'department_id', 'month_id']
}

RENAME_COLUMNS = {
    'PRODUCTOS': 'producto_name',
    'UBIGEO': 'department_id',
    'AÑO': 'year',
    'MES': 'month',
    'PRODUCCION MENSUAL': 'produccion',
    'UNIDAD': 'produccion_unidad'
}

REPLACE_VALUES = {
    'produccion_unidad': {
        't': 1
    }
}