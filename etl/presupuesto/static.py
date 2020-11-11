import os


FOLDER = 'dimensions'

DATA_FOLDER = os.path.join("..", "datasets", "download")

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
           'funcion', 'division_funcional', 'departamento_meta'],
    'gn_sectores': ['nivel_gobierno', 'sector_nombre', 'pliego_nombre', 'ubigeo', 'fuente_financiamiento_nombre', 'rubro_nombre'],
    'gob_regionales': ['nivel_gobierno', 'pliego_nombre', 'fuente_financ_nombre', 'rubro_nombre'],
    'gob_locales': ['nivel_gobierno', 'fuente_financiamiento_nombre', 'rubro_nombre', 'ubigeo']
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
        'pia':                  'Float64',
        'pim':                  'Float64',
        'devengado':            'Float64',
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
        'pia':                  'Float64',
        'pim':                  'Float64',
        'devengado':            'Float64',
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
        'pia':                  'Float64',
        'pim':                  'Float64',
        'devengado':            'Float64',
        'year':                 'UInt16'
    },
    'gn_sectores': {
        'nivel_gobierno':       'UInt8',
        'sector':               'UInt8',
        'pliego':               'UInt16',
        'rubro':                'UInt8',
        'ubigeo':               'String',
        'fuente_financiamiento':'UInt8',
        'pia':                  'Float64',
        'pim':                  'Float64',
        'monto_recaudado':      'Float64',
        'year':                 'UInt16'
    },
    'gob_regionales': {
        'nivel_gobierno':       'UInt8',
        'sector':               'UInt8',
        'pliego':               'UInt16',
        'rubro':                'UInt8',
        'ubigeo':               'String',
        'fuente_financiamiento':'UInt8',
        'pia':                  'Float64',
        'pim':                  'Float64',
        'monto_recaudado':      'Float64',
        'year':                 'UInt16'
    },
    'gob_locales': {
        'nivel_gobierno':       'UInt8',
        'sector':               'UInt8',
        'pliego':               'UInt16',
        'rubro':                'UInt8',
        'ubigeo':               'String',
        'fuente_financiamiento':'UInt8',
        'pia':                  'Float64',
        'pim':                  'Float64',
        'monto_recaudado':      'Float64',
        'year':                 'UInt16'
    }
}
