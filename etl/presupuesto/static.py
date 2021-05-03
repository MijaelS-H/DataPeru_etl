
URL_INGRESO = [
    "2014-Ingreso.zip",
    "2015-Ingreso.zip",
    "2016-Ingreso.zip",
    "2017-Ingreso.zip",
    "2018-Ingreso.zip",
    "2019-Ingreso.zip",
    "2020-Ingreso.zip",
    "2021-Ingreso.zip"
]

URL_GASTO = [
    "2014-Gasto.zip",
    "2015-Gasto.zip",
    "2016-Gasto.zip",
    "2017-Gasto.zip",
    "2018-Gasto.zip",
    "2019-Gasto.zip",
    "2020.zip",
    "2021.zip"
]

INGRESO_DIMENSIONS_COLS = ["tipo_gobierno", "sector", "pliego", "fuente_financ", "rubro", "ejecutora"]

INGRESO_DTYPES_COLS = {
    "tipo_gobierno":                 "String",
    "tipo_gobierno_nombre":          "String",
    "sector":                        "UInt8",
    "sector_nombre":                 "String",
    "pliego":                        "String",
    "pliego_nombre":                 "String",
    "sec_ejec":                      "UInt32",
    "ejecutora":                     "UInt32",
    "ejecutora_nombre":              "String",
    "fuente_financ":                 "UInt8",
    "fuente_financ_nombre":          "String",
    "rubro":                         "UInt8",
    "rubro_nombre":                  "String",
    "monto_pia":                     "Float64",
    "monto_pim":                     "Float64",
    "monto_recaudado":               "Float64",
    "district_id":                   "String",
    "month_id":                      "UInt32",
    "version":                       "String"
}

GASTO_DIMENSIONS_COLS = ["tipo_gobierno", "sector", "pliego", "ejecutora", "division_funcional", "programa_ppto", "producto_proyecto", "funcion"]

GASTO_DTYPES_COLS = {
    "tipo_gobierno":                 "String",
    "tipo_gobierno_nombre":          "String",
    "sector":                        "UInt8",
    "sector_nombre":                 "String",
    "division_funcional":            "UInt8",
    "division_funcional_nombre":     "String",
    "pliego":                        "String",
    "pliego_nombre":                 "String",
    "sec_ejec":                      "UInt32",
    "ejecutora":                     "UInt32",
    "ejecutora_nombre":              "String",
    "programa_ppto":                 "UInt16",
    "programa_ppto_nombre":          "String",
    "producto_proyecto":             "UInt32",
    "producto_proyecto_nombre":      "String",
    "funcion":                       "UInt8",
    "funcion_nombre":                "String",
    "departamento_meta":             "String",
    "departamento_meta_nombre":      "String",
    "monto_pia":                     "Float64",
    "monto_pim":                     "Float64",
    "monto_devengado":               "Float64",
    "district_id":                   "String",
    "month_id":                      "UInt32",
    "version":                       "String"
}
