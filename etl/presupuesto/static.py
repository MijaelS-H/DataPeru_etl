
FOLDER = 'dimensions'

DATA_FOLDER = '../../../datasets/download/'

TIPO_GOBIERNO = {
    'GOBIERNO NACIONAL': 1,
    'GOBIERNOS REGIONALES': 2,
    'GOBIERNOS LOCALES': 3
}

BASE = {
    'GN': ['tipo_gobierno', 'sector', 'pliego', 'ejecutora', 
           'sec_ejec', 'programa_ppto', 'producto_proyecto', 
           'funcion', 'division_funcional', 'departamento_meta'],
    'GR': ['tipo_gobierno', 'pliego', 'ejecutora', 
           'sec_ejec', 'programa_ppto', 'producto_proyecto', 
           'funcion', 'division_funcional', 'departamento_meta'],
    'GL': ['ubigeo', 'tipo_gobierno', 'ejecutora', 
           'sec_ejec', 'programa_ppto', 'producto_proyecto', 
           'funcion', 'division_funcional', 'departamento_meta']
}

DIMENSIONS = {
    'GN': {
        'dim_sector.csv': 'sector',
        'dim_pliego.csv': 'pliego',
        'dim_ejecutora.csv': 'ejecutora',
        'dim_funcion.csv': 'funcion',
        'dim_division_funcional.csv': 'division_funcional',
        'dim_programa_ppto.csv': 'programa_ppto',
        'dim_producto_proyecto.csv': 'producto_proyecto'
    },
    'GR': {
        'dim_pliego.csv': 'pliego',
        'dim_ejecutora.csv': 'ejecutora',
        'dim_funcion.csv': 'funcion',
        'dim_division_funcional.csv': 'division_funcional',
        'dim_programa_ppto.csv': 'programa_ppto',
        'dim_producto_proyecto.csv': 'producto_proyecto'
    },
    'GL': {
        'dim_ejecutora.csv': 'ejecutora',
        'dim_funcion.csv': 'funcion',
        'dim_division_funcional.csv': 'division_funcional',
        'dim_programa_ppto.csv': 'programa_ppto',
        'dim_producto_proyecto.csv': 'producto_proyecto'
    }
}

DTYPE = {
    'GN': {
        'sector':               'UInt8',
        'pliego':               'UInt8',
        'tipo_gobierno':        'UInt8',
        'ejecutora':            'UInt16',
        'sec_ejec':             'UInt32',
        'programa_ppto':        'UInt8',
        'producto_proyecto':    'UInt32',
        'funcion':              'UInt8',
        'division_funcional':   'UInt8',
        'departamento_meta':    'String',
        'pia':                  'Float32',
        'pim':                  'Float32',
        'devengado':            'Float32',
        'year':                 'UInt16'
    },
    'GR': {
        'pliego':               'UInt8',
        'tipo_gobierno':        'UInt8',
        'ejecutora':            'UInt16',
        'sec_ejec':             'UInt32',
        'programa_ppto':        'UInt8',
        'producto_proyecto':    'UInt32',
        'funcion':              'UInt8',
        'division_funcional':   'UInt8',
        'departamento_meta':    'String',
        'pia':                  'Float32',
        'pim':                  'Float32',
        'devengado':            'Float32',
        'year':                 'UInt16'
        },
    'GL': {
        'tipo_gobierno':        'UInt8',
        'ubigeo':               'String',
        'ejecutora':            'UInt16',
        'sec_ejec':             'UInt32',
        'programa_ppto':        'UInt8',
        'producto_proyecto':    'UInt32',
        'funcion':              'UInt8',
        'division_funcional':   'UInt8',
        'departamento_meta':    'String',
        'pia':                  'Float32',
        'pim':                  'Float32',
        'devengado':            'Float32',
        'year':                 'UInt16'
        }
}
