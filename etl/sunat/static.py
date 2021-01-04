COLUMNS_RENAME = {
    'regimen': 'trade_flow_id',
    'cadu': 'aduana_id',
    'cpaides': 'country_id',
    'femb': 'shipment_date_id',
    'freg': 'report_date_id',
    'cnan': 'hs6_id',
    'vfobserdol': 'trade_value',
    'vpesnet': 'net_weight_value',
    'tunifis': 'unit',
    'qunifis': 'quantity',
    'codi_aduan': 'aduana_id',
    'pais_orige': 'country_id',
    'fech_embar': 'shipment_date_id',
    'fech_ingsi': 'report_date_id',
    'part_nandi': 'hs6_id',
    'fob_dolpol': 'trade_value',
    'peso_neto': 'net_weight_value',
    'tunicom': 'unit',
    'qunicom': 'quantity'
}

COUNTRIES_DICT = {
    'fx': 'fr',
    'gf': 'fr',
    'gp': 'fr',
    'yt': 'fr',
    'l': 'pa',
    'mq': 'fr',
    'sx': 'nl',
    '1g': 'gb',
    '1m': 'de',
    '99': '1w',
    'an': 'nl',
    'um': 'us',
    ' ': 'xx', 
    '  ': 'xx',  
    '1c': 'xx',
    '1p': 'bz',
    'yu': 'xx',
    'nan': 'xx',
    '1k': 'ci',
    '1e': 'xx',
    'sj': 'no',
    'bv': 'no',
    'eh': 'ma',
    'gs': 'gb',
    'ax': 'fi',
    'hm': 'au'
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
    'UNI': 'U',
    '}U': 'U'
}