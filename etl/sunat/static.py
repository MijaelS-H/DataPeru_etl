COLUMNS_RENAME = {
    'regimen': 'trade_flow_id',
    'cadu': 'aduana_id',
    'cpaides': 'country_id',
    'femb': 'shipment_date_id',
    'freg': 'registry_date_id',
    'cnan': 'hs6_id',
    'vfobserdol': 'trade_value',
    'vpesnet': 'net_weight_value',
    'tunifis': 'unit',
    'qunifis': 'quantity'
}

COUNTRIES_DICT = {
    'fx': 'fr',
    'gf': 'fr',
    'gp': 'fr',
    'l': 'pa',
    'mq': 'fr',
    'sx': 'nl',
    '1g': 'gb',
    '1m': 'de',
    '99': '1w',
    'an': 'nl',
    'um': 'us',
    '  ': 'xx',  
    '1c': 'xx',
    '1p': 'bz',
    'yu': 'xx'
}

HS_DICT = {
    '80450': '080450'
}

REGIMEN_DICT = {
    'Importación Definitiva': 1,
    'Exportación Definitiva': 2
}

UBIGEO_DICT = {
    '      ': '999999'
}

UNIT_DICT = {
    'u': 'U',
    'kg': 'KG',
    'm2': 'M2',
    'KIG': 'KI6',
    'UU6': 'U6',
    'LT': 'L',
    'UNI': 'U'
}