import pandas as pd
from bamboo_lib.helpers import grab_parent_dir
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
path = grab_parent_dir('../../') + "/datasets/20200318"

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        # Loading data
        df1 =  pd.read_excel(io = '{}/{}/{}'.format(path, 'A. Economía', "A.24.xlsx"),   skiprows = (0,1,2,4,17,18), usecols = "A:I", reset_index = True)
        df2 =  pd.read_excel(io = '{}/{}/{}'.format(path, 'A. Economía', "A.165.xlsx"),   skiprows = (0,1,2,3,5,6,7), usecols = "A:K", reset_index = True)[0:19]
        df3 =  pd.read_excel(io = '{}/{}/{}'.format(path, 'A. Economía', "A.175.xlsx"),   skiprows = (0,1,2,3,5,6,7,8), usecols = "A:G")[0:36]
        df4 =  pd.read_excel(io = '{}/{}/{}'.format(path, 'A. Economía', "A.186.xlsx"),   skiprows = (0,1,2), usecols = "A:R")[0:17]
        df5 =  pd.read_excel(io = '{}/{}/{}'.format(path, 'B. Población y Vivienda', "B.1.xls"),   skiprows = (0,1,2,3,4,6,7), usecols = "A:F")[0:8]
        df6 =  pd.read_excel(io = '{}/{}/{}'.format(path, 'C. Empleo', "C.13.xlsx"),   skiprows = (0,1,2,4,5), usecols = "A:M")[0:41]
        df7 =  pd.read_excel(io = '{}/{}/{}'.format(path, 'C. Empleo', "C.14.xlsx"),   skiprows = (0,1,2,3), usecols = "A:M")[3:5]
        df8 =  pd.read_excel(io = '{}/{}/{}'.format(path, 'D. Sociales', "D.5.xlsx"),   skiprows = (0,1), usecols = "A:J")[3:12]
        df9 =  pd.read_excel(io = '{}/{}/{}'.format(path, 'D. Sociales', "D.6.xlsx"),   skiprows = (0,1,2,4,6), usecols = "A:K")[0:7]
        df10 = pd.read_excel(io = '{}/{}/{}'.format(path, 'D. Sociales', "D.36.xlsx"), skiprows = range(0,7), usecols = "A:D")[0:25]
        df11 = pd.read_excel(io = '{}/{}/{}'.format(path, 'D. Sociales', "D.57.xlsx"), skiprows = (0,1,2,3,5,6,7,8,9), usecols = "A:O")[0:32]
        df12 = pd.read_excel(io = '{}/{}/{}'.format(path, 'E. Medio Ambiente', "E.39.xlsx"), skiprows = (0,1,2), usecols = "A:B")[0:21]
        df13 = pd.read_excel(io = '{}/{}/{}'.format(path, 'G. Seguridad Ciudadana', "G.3.xlsx"), skiprows = (0,1,2,3,4,6,7,8,9), usecols = "A,C:I")[0:20]

        # Starts transforming step for each table, given their unique "formats"
        # df1
        df1.rename(columns= {"Años": "year", "Producto Bruto Interno ": "producto_interno_bruto", "Remune-\nraciones": "remuneraciones", "Derechos \nde \nImportación": "derechos_importacion", "Impuestos \na los \nProductos": "impuestos_productos", "Otros Impuestos": "otros_impuestos", " Ingreso de explotación": "ingreso_explotacion", " Excedente de explotación bruto": "excedente_explotacion_bruto", " Ingreso mixto": "ingreso_mixto"}, inplace = True)
        df1["year"].replace({"2016P/": 2016, "2017P/": 2017, "2018E/": 2018}, inplace = True)

        #df2
        df2 = df2.T
        new_header = df2.iloc[0]
        df2 = df2[1:]
        df2.columns = new_header
        df2.drop("  Hogares privados con servicio doméstico y", axis = 1, inplace = True)
        df2['year'] = df2.index
        df2.rename(columns= {'  Agricultura, ganadería, caza y silvicultura': 'agricultura_ganaderia_caza_silvicultura', '  Pesca': 'pesca', '  Minería': 'mineria', '  Industria manufacturera': 'industria_manufacturera', '  Electricidad, gas y agua': 'electricidad_gas_agua', '  Construcción': 'construccion', '  Comercio': 'comercio', '  Hoteles y restaurantes': 'hoteles_restaurantes', '  Transporte, almacenamiento y comunicaciones': 'transporte_almacenamiento_comunicaciones', '  Intermediación financiera': 'intermediacion_financiera', '  Actividad inmobiliarias, empresariales y de alquiler': 'actividad_inmobiliarias_empresariales_alquiler', '  Administración pública y defensa': 'administracion_publica_defensa', '  Enseñanza': 'ensenianza', '  Servicios sociales y salud': 'servicios_sociales_salud', '  Otras actividades de servicios comunitarios': 'otras_actividades_servicios_comunitarios', '  Hogares privados con servicio doméstico y': 'droped', '  organizaciones extraterritoriales': 'hogares_privados_organizaciones_extraterritoriales', 'Créditos Hipotecarios para Vivienda': 'creditos_hipotecariosvivienda', 'Créditos de Consumo': 'creditos_consumo'}, inplace = True)

        # df3
        df3.rename(columns= {"Unnamed: 0": "year", "Expor-": "exportaciones", "Impor-": "importaciones", "Balanza": "balanza_comercial", "Balanza.1": "balanza_pagos", "Activos": "activos_externos_netos_corto_plazo", "Deuda": "deuda_publica_externa"}, inplace = True)

        # df4
        df4.rename(columns= {"2016 P/": 2016, "2017 P/": 2017, "2018 P/": 2018}, inplace = True)
        df4 = df4.T
        new_header = df4.iloc[0]
        df4 = df4[1:]
        df4.columns = new_header
        df4.drop(["I.  Activos", "II. Pasivos", "        Privada y Pública  1/", "    1. Bonos y Deuda Externa Total", "                  BCRP", "          a.    Mediano y Largo Plazo ", "           b.   Corto Plazo"], axis = 1, inplace = True)
        df4['year'] = df4.index
        df4.rename(columns= {"    1.  Activos de Reserva del BCRP": "act_reserva_BCRP", "    2.  Sistema Financiero (sin BCRP)": "act_sist_financiero_sin_BCRP", "    3.  Otros Activos": "act_otros_activos", "                  Sector Privado    2/": "pas_med_lar_sector_privado", "                  Sector Público    3/": "pas_med_lar_sector_publico", "                  Sistema Financiero (sin BCRP)": "pas_cort_sist_financiero_sin_BCRP", "                  BCRP   4/": "pas_cort_BCRP", "                  Otros    5/": "pas_cort_otros", "     2. Inversión Directa": "pas_inversion_directa", "     3. Participación del Capital": "pas_participacion_capital" }, inplace = True)

        # df5
        df5.drop(["Unnamed: 1", "Unnamed: 4"], axis = 1, inplace = True)
        df5.rename(columns= {"Año": "year", "Población": "poblacion_total", "Unnamed: 3": "poblacion_censada", "Unnamed: 5": "poblacion_omitida"}, inplace = True)

        # df6
        df6 = df6.T
        new_header = df6.iloc[0]
        df6 = df6[1:]
        df6.columns = new_header
        df6['year'] = df6.index
        df6 = df6[['Hombre', 'Mujer', '14 a 24 años', '25 a 44 años', '45  a 64 años', '65 y más años', 'Primaria o menos 1/', 'Secundaria', 'Superior no univeritaria', 'Superior universitaria',  'De 1 a  10 personas', 'De 11 a 50 personas', 'Más de 50 personas', 'Agricultura/Pesca/Minería', 'Manufactura', 'Construcción','Comercio', 'Transp. y Comunicaciones ', 'Otros Servicios 3/', 'year']]
        df6.rename(columns= {"Hombre": "pea_hombres", "Mujer": "pea_mujeres", "14 a 24 años": "pea_14_24_yrs", "25 a 44 años": "pea_25_44_yrs", "45  a 64 años": "pea_45_64_yrs", "65 y más años": "pea_65_o_mas_yrs", "Primaria o menos 1/": "pea_primaria_o_inferior", "Secundaria": "pea_secundaria", "Superior no univeritaria": "pea_superior_no_universitaria", "Superior universitaria": "pea_universitaria", "De 1 a  10 personas": "pea_empresa_1_10_empleados", "De 11 a 50 personas": "pea_empresa_11_50_empleados", "Más de 50 personas": "pea_empresa_50_o_mas_empleados", "Agricultura/Pesca/Minería": "pea_agricultura_pesca_mineria", "Manufactura": "pea_manufactura", "Construcción": "pea_construccion", "Comercio": "pea_comercio", "Transp. y Comunicaciones ": "pea_transporte_comunicaciones", "Otros Servicios 3/": "pea_otros_servicios" }, inplace = True)

        # df7
        df7 = df7.T
        new_header = df7.iloc[0]
        df7 = df7[1:]
        df7.columns = new_header
        df7['year'] = df7.index
        df7.rename(columns= {"Sin Seguro": "pea_sin_seguro_medico", "Con Seguro": "pea_con_seguro_medico"}, inplace = True)

        # df8
        df8 = df8.T
        new_header = df8.iloc[0]
        df8 = df8[1:]
        df8.columns = new_header
        df8['year'] = df8.index
        df8.rename(columns= {"Con 1 NBI": "perc_poblacion_con_1_nbi", "Con 2 a 5 NBI": "perc_poblacion_con_2_a_5_nbi", "Población en viviendas con características físicas inadecuadas": "perc_poblacion_nbi_vivienda_inadecuada", "Población en viviendas con hacinamiento": "perc_poblacion_nbi_vivienda_hacinada", "Población en viviendas sin servicios higiénicos": "perc_poblacion_nbi_servicios_higienicos", "Población en hogares con niños que no asisten a la escuela": "perc_poblacion_nbi_menores_sin_escuela", "Población en hogares con alta dependencia económica": "perc_poblacion_nbi_alta_dependencia_economica"}, inplace = True)
        df8 = df8[["year", "perc_poblacion_con_1_nbi", "perc_poblacion_con_2_a_5_nbi", "perc_poblacion_nbi_vivienda_inadecuada", "perc_poblacion_nbi_vivienda_hacinada", "perc_poblacion_nbi_servicios_higienicos", "perc_poblacion_nbi_menores_sin_escuela", "perc_poblacion_nbi_alta_dependencia_economica"]]

        # df9
        df9.rename(columns= {'2018 P/': 2018}, inplace = True)
        df9 = df9.T
        new_header = df9.iloc[0]
        df9 = df9[1:]
        df9.columns = new_header
        df9['year'] = df9.index
        df9.rename(columns= {'Educación Inicial': 'gasto_social_educacion_inicial', 'Educación Primaria': 'gasto_social_educacion_primaria', 'Educación Secundaria': 'gasto_social_educacion_secundaria', 'Asistencia Social': 'gasto_social_asistencia_social', 'Salud Colectiva': 'gasto_social_salud_colectiva', 'Salud Individual': 'gasto_social_salud_individual' }, inplace = True)

        # df10
        df10.drop("Unnamed: 1", axis = 1, inplace = True)
        df10.rename(columns= {"Unnamed: 0": "year", "Unnamed: 2": "gasto_gobierno_sector_publico", "Unnamed: 3": "gasto_gobierno_sector_privado"}, inplace = True)
        df10["year"].replace({"2015  P/": 2015, "2016  P/": 2016, "2017  E/": 2017, "2018  E/": 2018}, inplace = True)

        # df11
        df11.rename(columns= {"Unnamed: 9": 2013, "Unnamed: 10": 2014, "Unnamed: 11": 2015, "Unnamed: 12": 2016, "Unnamed: 13": 2017, "Unnamed: 14": 2018}, inplace = True)
        df11.drop([6,7,8,11,12,15,16,19,20,23,24,27,28,31], axis = 0, inplace = True)
        df11 = df11.T
        new_header = ["analfabetismo_total_15_19", "analfabetismo_total_20_29", "analfabetismo_total_30_39", "analfabetismo_total_40_49", "analfabetismo_total_50_59", "analfabetismo_total_60_y_mas", "analfabetismo_h_15_19", "analfabetismo_f_15_19", "analfabetismo_h_20_29", "analfabetismo_f_20_29", "analfabetismo_h_30_39", "analfabetismo_f_30_39", "analfabetismo_h_40_49", "analfabetismo_f_40_49", "analfabetismo_h_50_59", "analfabetismo_f_50_59", "analfabetismo_h_60_y_mas", "analfabetismo_f_60_y_mas"]
        df11.columns = new_header
        df11.drop(["Grupos de edad / \nSexo"], axis = 0, inplace = True)
        df11['year'] = df11.index

        # df12
        df12.rename(columns= {"Año": "year", "Emisiones": "millones_toneladas_co2_equivalente"}, inplace = True)

        # df13
        df13 = df13.T
        new_header = df13.iloc[0]
        df13 = df13[1:]
        df13.columns = new_header
        df13['year'] = df13.index
        df13.rename(columns= {"Delitos contra la vida, el cuerpo y la salud": "delitos_vida_cuerpo_salud", "Delitos contra el honor": "delitos_honor", "Delitos contra la familia": "delitos_familia", "Delitos contra la libertad": "delitos_libertad", "Delitos contra el patrimonio": "delitos_patrimonio", "Delito contra la confianza y la buena fe en los negocios": "delitos_confianza_buena_fe_negocios", "Delitos contra los derechos intelectuales": "delitos_derechos_intelectuales", "Delitos contra el patrimonio cultural": "delitos_patrimonio_cultural", "Delitos contra el orden económico": "delitos_orden_economico", "Delitos contra el orden financiero y monetario": "delitos_orden_financiero_monetario", "Delitos tributarios": "delitos_tributarios", "Delitos contra la seguridad pública": "delitos_seguridad_publica", "Delitos ambientales": "delitos_ambientales", "Delitos contra la tranquilidad pública": "delitos_tranquilidad_publica", "Delitos contra la humanidad": "delitos_humanidad", "Delitos contra el estado y la defensa nacional": "delitos_estado_defensa_nacional", "Delitos contra los poderes del estado y el orden constitucional": "delitos_poderes_estado_orden_const", "Delito contra la voluntad popular": "delitos_voluntad_popular", "Delitos contra la administración pública": "delitos_administracion_publica", "Delitos contra la fe pública": "delitos_fe_publica"}, inplace = True)

        # Merge of the 13 datasets
        df = pd.merge(df1, df2[["year", "agricultura_ganaderia_caza_silvicultura", "pesca", "mineria", "industria_manufacturera", "electricidad_gas_agua", "construccion", "comercio", "hoteles_restaurantes", "transporte_almacenamiento_comunicaciones", "intermediacion_financiera", "actividad_inmobiliarias_empresariales_alquiler", "administracion_publica_defensa", "ensenianza", "servicios_sociales_salud", "otras_actividades_servicios_comunitarios", "hogares_privados_organizaciones_extraterritoriales", "creditos_hipotecariosvivienda", "creditos_consumo"]], on = "year", how = "left")
        df = pd.merge(df, df3[["year", "exportaciones", "importaciones", "balanza_comercial", "balanza_pagos", "activos_externos_netos_corto_plazo", "deuda_publica_externa"]], on = "year", how = "left")
        df = pd.merge(df, df4[["year", "act_reserva_BCRP", "act_sist_financiero_sin_BCRP", "act_otros_activos", "pas_med_lar_sector_privado", "pas_med_lar_sector_publico", "pas_cort_sist_financiero_sin_BCRP", "pas_cort_BCRP", "pas_cort_otros", "pas_inversion_directa", "pas_participacion_capital"]], on = "year", how = "left")
        df = pd.merge(df, df5[["year", "poblacion_total", "poblacion_censada", "poblacion_omitida"]], on = "year", how = "left")
        df = pd.merge(df, df6[["year", "pea_hombres", "pea_mujeres", "pea_14_24_yrs", "pea_25_44_yrs", "pea_45_64_yrs", "pea_65_o_mas_yrs", "pea_primaria_o_inferior", "pea_secundaria", "pea_superior_no_universitaria", "pea_universitaria", "pea_empresa_1_10_empleados", "pea_empresa_11_50_empleados", "pea_empresa_50_o_mas_empleados", "pea_agricultura_pesca_mineria", "pea_manufactura", "pea_construccion", "pea_comercio", "pea_transporte_comunicaciones", "pea_otros_servicios"]], on = "year", how = "left")
        df = pd.merge(df, df7[["year", "pea_sin_seguro_medico", "pea_con_seguro_medico"]], on = "year", how = "left")
        df = pd.merge(df, df8[["year", "perc_poblacion_con_1_nbi", "perc_poblacion_con_2_a_5_nbi", "perc_poblacion_nbi_vivienda_inadecuada", "perc_poblacion_nbi_vivienda_hacinada", "perc_poblacion_nbi_servicios_higienicos", "perc_poblacion_nbi_menores_sin_escuela", "perc_poblacion_nbi_alta_dependencia_economica"]], on = "year", how = "left")
        df = pd.merge(df, df9[["year", "gasto_social_educacion_inicial", "gasto_social_educacion_primaria", "gasto_social_educacion_secundaria", "gasto_social_asistencia_social", "gasto_social_salud_colectiva", "gasto_social_salud_individual"]], on = "year", how = "left")
        df = pd.merge(df, df10[["year", "gasto_gobierno_sector_publico", "gasto_gobierno_sector_privado"]], on = "year", how = "left")
        df = pd.merge(df, df11[["year", "analfabetismo_total_15_19", "analfabetismo_total_20_29", "analfabetismo_total_30_39", "analfabetismo_total_40_49", "analfabetismo_total_50_59", "analfabetismo_total_60_y_mas", "analfabetismo_h_15_19", "analfabetismo_f_15_19", "analfabetismo_h_20_29", "analfabetismo_f_20_29", "analfabetismo_h_30_39", "analfabetismo_f_30_39", "analfabetismo_h_40_49", "analfabetismo_f_40_49", "analfabetismo_h_50_59", "analfabetismo_f_50_59", "analfabetismo_h_60_y_mas", "analfabetismo_f_60_y_mas"]], on = "year", how = "left")
        df = pd.merge(df, df12[["year", "millones_toneladas_co2_equivalente"]], on = "year", how = "left")
        df = pd.merge(df,df13[['delitos_vida_cuerpo_salud', 'delitos_honor', 'delitos_familia', 'delitos_libertad', 'delitos_patrimonio', 'delitos_confianza_buena_fe_negocios', 'delitos_derechos_intelectuales', 'delitos_patrimonio_cultural', 'delitos_orden_economico', 'delitos_orden_financiero_monetario', 'delitos_tributarios', 'delitos_seguridad_publica', 'delitos_ambientales', 'delitos_tranquilidad_publica', 'delitos_humanidad', 'delitos_estado_defensa_nacional', 'delitos_poderes_estado_orden_const', 'delitos_voluntad_popular', 'delitos_administracion_publica', 'delitos_fe_publica', 'year']], on = "year", how = "left")

        df.replace("-", 0, inplace = True)
        # Changing str values to float/int values
        for col in df.columns:
            df[col] = df[col].astype(float)

        return df

class itp_indicators_y_n_nat_pipeline(EasyPipeline):

    @staticmethod
    def parameter_list():
        return [
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "year":                                                   "UInt16",
            "producto_interno_bruto":                                 "UInt16",
            "remuneraciones":                                         "UInt16",
            "derechos_importacion":                                   "UInt16",
            "impuestos_productos":                                    "UInt16",
            "otros_impuestos":                                        "UInt16",
            "ingreso_explotacion":                                    "UInt16",
            "excedente_explotacion_bruto":                            "UInt16",
            "ingreso_mixto":                                          "UInt16",
            "agricultura_ganaderia_caza_silvicultura":                "UInt16",
            "pesca":                                                  "UInt16",
            "mineria":                                                "UInt16",
            "industria_manufacturera":                                "UInt16",
            "electricidad_gas_agua":                                  "UInt16",
            "construccion":                                           "UInt16",
            "comercio":                                               "UInt16",
            "hoteles_restaurantes":                                   "UInt16",
            "transporte_almacenamiento_comunicaciones":               "UInt16",
            "intermediacion_financiera":                              "UInt16",
            "actividad_inmobiliarias_empresariales_alquiler":         "UInt16",
            "administracion_publica_defensa":                         "UInt16",
            "ensenianza":                                             "UInt16",
            "servicios_sociales_salud":                               "UInt16",
            "otras_actividades_servicios_comunitarios":               "UInt16",
            "hogares_privados_organizaciones_extraterritoriales":     "UInt16",
            "creditos_hipotecariosvivienda":                          "UInt16",
            "creditos_consumo":                                       "UInt16",
            "exportaciones":                                          "UInt16",
            "importaciones":                                          "UInt16",
            "balanza_comercial":                                      "Int32",
            "balanza_pagos":                                          "Int32",
            "activos_externos_netos_corto_plazo":                     "UInt16",
            "deuda_publica_externa":                                  "UInt16",
            "act_reserva_BCRP":                                       "UInt16",
            "act_sist_financiero_sin_BCRP":                           "UInt16",
            "act_otros_activos":                                      "UInt16",
            "pas_med_lar_sector_privado":                             "UInt16",
            "pas_med_lar_sector_publico":                             "UInt16",
            "pas_cort_sist_financiero_sin_BCRP":                      "UInt16",
            "pas_cort_BCRP":                                          "UInt16",
            "pas_cort_otros":                                         "UInt16",
            "pas_inversion_directa":                                  "UInt16",
            "pas_participacion_capital":                              "UInt16",
            "poblacion_total":                                        "UInt16",
            "poblacion_censada":                                      "UInt16",
            "poblacion_omitida":                                      "UInt16",
            "pea_hombres":                                            "UInt16",
            "pea_mujeres":                                            "UInt16",
            "pea_14_24_yrs":                                          "UInt16",
            "pea_25_44_yrs":                                          "UInt16",
            "pea_45_64_yrs":                                          "UInt16",
            "pea_65_o_mas_yrs":                                       "UInt16",
            "pea_primaria_o_inferior":                                "UInt16",
            "pea_secundaria":                                         "UInt16",
            "pea_superior_no_universitaria":                          "UInt16",
            "pea_universitaria":                                      "UInt16",
            "pea_empresa_1_10_empleados":                             "UInt16",
            "pea_empresa_11_50_empleados":                            "UInt16",
            "pea_empresa_50_o_mas_empleados":                         "UInt16",
            "pea_agricultura_pesca_mineria":                          "UInt16",
            "pea_manufactura":                                        "UInt16",
            "pea_construccion":                                       "UInt16",
            "pea_comercio":                                           "UInt16",
            "pea_transporte_comunicaciones":                          "UInt16",
            "pea_otros_servicios":                                    "UInt16",
            "pea_sin_seguro_medico":                                  "UInt16",
            "pea_con_seguro_medico":                                  "UInt16",
            "perc_poblacion_con_1_nbi":                               "UInt16",
            "perc_poblacion_con_2_a_5_nbi":                           "UInt16",
            "perc_poblacion_nbi_vivienda_inadecuada":                 "UInt16",
            "perc_poblacion_nbi_vivienda_hacinada":                   "UInt16",
            "perc_poblacion_nbi_servicios_higienicos":                "UInt16",
            "perc_poblacion_nbi_menores_sin_escuela":                 "UInt16",
            "perc_poblacion_nbi_alta_dependencia_economica":          "UInt16",
            "gasto_social_educacion_inicial":                         "UInt16",
            "gasto_social_educacion_primaria":                        "UInt16",
            "gasto_social_educacion_secundaria":                      "UInt16",
            "gasto_social_asistencia_social":                         "UInt16",
            "gasto_social_salud_colectiva":                           "UInt16",
            "gasto_social_salud_individual":                          "UInt16",
            "gasto_gobierno_sector_publico":                          "UInt16",
            "gasto_gobierno_sector_privado":                          "UInt16",
            "analfabetismo_total_15_19":                              "UInt16",
            "analfabetismo_total_20_29":                              "UInt16",
            "analfabetismo_total_30_39":                              "UInt16",
            "analfabetismo_total_40_49":                              "UInt16",
            "analfabetismo_total_50_59":                              "UInt16",
            "analfabetismo_total_60_y_mas":                           "UInt16",
            "analfabetismo_h_15_19":                                  "UInt16",
            "analfabetismo_f_15_19":                                  "UInt16",
            "analfabetismo_h_20_29":                                  "UInt16",
            "analfabetismo_f_20_29":                                  "UInt16",
            "analfabetismo_h_30_39":                                  "UInt16",
            "analfabetismo_f_30_39":                                  "UInt16",
            "analfabetismo_h_40_49":                                  "UInt16",
            "analfabetismo_f_40_49":                                  "UInt16",
            "analfabetismo_h_50_59":                                  "UInt16",
            "analfabetismo_f_50_59":                                  "UInt16",
            "analfabetismo_h_60_y_mas":                               "UInt16",
            "analfabetismo_f_60_y_mas":                               "UInt16",
            "millones_toneladas_co2_equivalente":                     "UInt16",
            "delitos_vida_cuerpo_salud":                              "UInt16",
            "delitos_honor":                                          "UInt16",
            "delitos_familia":                                        "UInt16",
            "delitos_libertad":                                       "UInt16",
            "delitos_patrimonio":                                     "UInt16",
            "delitos_confianza_buena_fe_negocios":                    "UInt16",
            "delitos_derechos_intelectuales":                         "UInt16",
            "delitos_patrimonio_cultural":                            "UInt16",
            "delitos_orden_economico":                                "UInt16",
            "delitos_orden_financiero_monetario":                     "UInt16",
            "delitos_tributarios":                                    "UInt16",
            "delitos_seguridad_publica":                              "UInt16",
            "delitos_ambientales":                                    "UInt16",
            "delitos_tranquilidad_publica":                           "UInt16",
            "delitos_humanidad":                                      "UInt16",
            "delitos_estado_defensa_nacional":                        "UInt16",
            "delitos_poderes_estado_orden_const":                     "UInt16",
            "delitos_voluntad_popular":                               "UInt16",
            "delitos_administracion_publica":                         "UInt16",
            "delitos_fe_publica":                                     "UInt16"
            }

        transform_step = TransformStep()
        load_step = LoadStep(
            "itp_indicators_y_n_nat", db_connector, if_exists="drop", pk=["year"], dtype=dtype, 
            nullable_list=["agricultura_ganaderia_caza_silvicultura", "pesca", "mineria", "industria_manufacturera", "electricidad_gas_agua", "construccion", "comercio",
                          "hoteles_restaurantes", "transporte_almacenamiento_comunicaciones", "intermediacion_financiera", "actividad_inmobiliarias_empresariales_alquiler",
                          "administracion_publica_defensa", "ensenianza", "servicios_sociales_salud", "otras_actividades_servicios_comunitarios",
                          "hogares_privados_organizaciones_extraterritoriales", "creditos_hipotecariosvivienda", "creditos_consumo", "poblacion_total", "poblacion_censada",
                          "poblacion_omitida", "perc_poblacion_con_1_nbi", "perc_poblacion_con_2_a_5_nbi", "perc_poblacion_nbi_vivienda_inadecuada",
                          "perc_poblacion_nbi_vivienda_hacinada", "perc_poblacion_nbi_servicios_higienicos", "perc_poblacion_nbi_menores_sin_escuela",
                          "perc_poblacion_nbi_alta_dependencia_economica", "gasto_social_educacion_inicial", "gasto_social_educacion_primaria", "gasto_social_educacion_secundaria",
                          "gasto_social_asistencia_social", "gasto_social_salud_colectiva", "gasto_social_salud_individual", "analfabetismo_h_15_19", "analfabetismo_f_15_19",
                          "analfabetismo_h_20_29", "analfabetismo_f_20_29", "analfabetismo_h_30_39", "analfabetismo_f_30_39", "analfabetismo_h_40_49", "analfabetismo_f_40_49",
                          "analfabetismo_h_50_59", "analfabetismo_f_50_59", "analfabetismo_h_60_y_mas", "analfabetismo_f_60_y_mas", "millones_toneladas_co2_equivalente",
                          "delitos_vida_cuerpo_salud", "delitos_honor", "delitos_familia", "delitos_libertad", "delitos_patrimonio", "delitos_confianza_buena_fe_negocios",
                          "delitos_derechos_intelectuales", "delitos_patrimonio_cultural", "delitos_orden_economico", "delitos_orden_financiero_monetario", "delitos_tributarios",
                          "delitos_seguridad_publica", "delitos_ambientales", "delitos_tranquilidad_publica", "delitos_humanidad", "delitos_estado_defensa_nacional",
                          "delitos_poderes_estado_orden_const", "delitos_voluntad_popular", "delitos_administracion_publica", "delitos_fe_publica"]
        )

        return [transform_step, load_step]

if __name__ == "__main__":
    pp = itp_indicators_y_n_nat_pipeline()
    pp.run({})