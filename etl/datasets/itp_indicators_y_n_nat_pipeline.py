from os import path
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from etl.consistency import AggregatorStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        # Loading data
        df1 =  pd.read_excel(io = path.join(params["datasets"],"20200318", "A. Economía", "A.24.xlsx"), skiprows = (0,1,2,4,17,18), usecols = "A:I", reset_index = True)
        df2 =  pd.read_excel(io = path.join(params["datasets"],"20200318", "A. Economía", "A.165.xlsx"), skiprows = (0,1,2,3,5,6,7), usecols = "A:K", reset_index = True)[0:19]
        df3 =  pd.read_excel(io = path.join(params["datasets"],"20200318", "A. Economía", "A.175.xlsx"), skiprows = (0,1,2,3,5,6,7,8), usecols = "A:G")[0:36]
        df4 =  pd.read_excel(io = path.join(params["datasets"],"20200318", "A. Economía", "A.186.xlsx"), skiprows = (0,1,2), usecols = "A:R")[0:17]
        df5 =  pd.read_excel(io = path.join(params["datasets"],"20200318", "B. Población y Vivienda", "B.1.xls"), skiprows = (0,1,2,3,4,6,7), usecols = "A:F")[0:8]
        df6 =  pd.read_excel(io = path.join(params["datasets"],"20200318", "C. Empleo", "C.13.xlsx"), skiprows = (0,1,2,4,5), usecols = "A:M")[0:41]
        df7 =  pd.read_excel(io = path.join(params["datasets"],"20200318", "C. Empleo", "C.14.xlsx"), skiprows = (0,1,2,3), usecols = "A:M")[3:5]
        df8 =  pd.read_excel(io = path.join(params["datasets"],"20200318", "D. Sociales", "D.5.xlsx"), skiprows = (0,1), usecols = "A:J")[3:12]
        df9 =  pd.read_excel(io = path.join(params["datasets"],"20200318", "D. Sociales", "D.6.xlsx"), skiprows = (0,1,2,4,6), usecols = "A:K")[0:7]
        df10 = pd.read_excel(io = path.join(params["datasets"],"20200318", "D. Sociales", "D.36.xlsx"), skiprows = range(0,7), usecols = "A:D")[0:25]
        df11 = pd.read_excel(io = path.join(params["datasets"],"20200318", "D. Sociales", "D.57.xlsx"), skiprows = (0,1,2,3,5,6,7,8,9), usecols = "A:O")[0:32]
        df12 = pd.read_excel(io = path.join(params["datasets"],"20200318", "E. Medio Ambiente", "E.39.xlsx"), skiprows = (0,1,2), usecols = "A:B")[0:21]
        df13 = pd.read_excel(io = path.join(params["datasets"],"20200318", "G. Seguridad Ciudadana", "G.3.xlsx"), skiprows = (0,1,2,3,4,6,7,8,9), usecols = "A,C:I")[0:20]
        df14 = pd.read_excel(io = path.join(params["datasets"],"20200318", "A. Economía", "A.182.xlsx"), skiprows = (0,1,2,3), usecols = "A,E:J,M,N")[8:20]
        df15 = pd.read_excel(io = path.join(params["datasets"],"20200318", "A. Economía", "A.164.xlsx"), skiprows = (0,1,2,3,5,6), usecols = "A,I:L,T:W")[0:8]

        df16 = pd.read_excel(io = path.join(params["datasets"],"20200318", "A. Economía", "A.71.xlsx"), skiprows = range(0,7), usecols = "A:D,F:H,L,M")[11:23]
        df17 = pd.read_excel(io = path.join(params["datasets"],"20200318", "A. Economía", "A.72.xlsx"), skiprows = range(0,8), usecols = "A,G:J,M,N,Q:S")[12:24]
        df18 = pd.read_excel(io = path.join(params["datasets"],"20200318", "A. Economía", "A.74.xlsx"), skiprows = (0,1,2,4,5,6), usecols = "A,I:T")[0:50]
        df19 = pd.read_excel(io = path.join(params["datasets"],"20200318", "A. Economía", "A.75.xlsx"), skiprows = (0,1,2,3,4), usecols = "A,J:U")[2:13]
        df20 = pd.read_excel(io = path.join(params["datasets"],"20200318", "A. Economía", "A.76.xlsx"), skiprows = (0,1,2,3,4), usecols = "A,K:V")[2:10]
        df21 = pd.read_excel(io = path.join(params["datasets"],"20200318", "A. Economía", "A.77.xlsx"), skiprows = (0,1,2,3), usecols = "A,K:V")[2:12]
        df22 = pd.read_excel(io = path.join(params["datasets"],"20200318", "A. Economía", "A.80.xlsx"), skiprows = (0,1,2,3), usecols = "A,J:U")[1:14]

        df23 = pd.read_excel(io = path.join(params["datasets"],"20200318", "A. Economía", "A.146.xls"), skiprows = range(0,6), usecols = "A:C,E,F,H,I")[14:26]
        df24 = pd.read_excel(io = path.join(params["datasets"],"20200318", "A. Economía", "A.148.xls"), skiprows = (0,1), usecols = "A,F:Q")[1:4]
        df25 = pd.read_excel(io = path.join(params["datasets"],"20200318", "A. Economía", "A.151.xls"), skiprows = (0,1,2,3), usecols = "A,D:O")[3:14]

        # Starts transforming step for each table, given thepath.join(params["datasets"],"20200318"      # df1
        df1.rename(columns= {"Años": "year", "Producto Bruto Interno ": "producto_interno_bruto_mill_n_soles", "Remune-\nraciones": "remuneraciones_mill_n_soles", "Derechos \nde \nImportación": "derechos_importacion_mill_n_soles", "Impuestos \na los \nProductos": "impuestos_productos_mill_n_soles", "Otros Impuestos": "otros_impuestos_mill_n_soles", " Ingreso de explotación": "ingreso_explotacion_mill_n_soles", " Excedente de explotación bruto": "excedente_explotacion_bruto_mill_n_soles", " Ingreso mixto": "ingreso_mixto_mill_n_soles"}, inplace = True)
        df1["year"].replace({"2016P/": 2016, "2017P/": 2017, "2018E/": 2018}, inplace = True)

        #df2
        df2 = df2.T
        new_header = df2.iloc[0]
        df2 = df2[1:]
        df2.columns = new_header
        df2.drop("  Hogares privados con servicio doméstico y", axis = 1, inplace = True)
        df2["year"] = df2.index
        df2.rename(columns= {"  Agricultura, ganadería, caza y silvicultura": "agricultura_ganaderia_caza_silvicultura_miles_soles", "  Pesca": "pesca_miles_soles", "  Minería": "mineria_miles_soles", "  Industria manufacturera": "industria_manufacturera_miles_soles", "  Electricidad, gas y agua": "electricidad_gas_agua_miles_soles", "  Construcción": "construccion_miles_soles", "  Comercio": "comercio_miles_soles", "  Hoteles y restaurantes": "hoteles_restaurantes_miles_soles", "  Transporte, almacenamiento y comunicaciones": "transporte_almacenamiento_comunicaciones_miles_soles", "  Intermediación financiera": "intermediacion_financiera_miles_soles", "  Actividad inmobiliarias, empresariales y de alquiler": "actividad_inmobiliarias_empresariales_alquiler_miles_soles", "  Administración pública y defensa": "administracion_publica_defensa_miles_soles", "  Enseñanza": "ensenianza_miles_soles", "  Servicios sociales y salud": "servicios_sociales_salud_miles_soles", "  Otras actividades de servicios comunitarios": "otras_actividades_servicios_comunitarios_miles_soles", "  Hogares privados con servicio doméstico y": "droped", "  organizaciones extraterritoriales": "hogares_privados_organizaciones_extraterritoriales_miles_soles", "Créditos Hipotecarios para Vivienda": "creditos_hipotecariosvivienda_miles_soles", "Créditos de Consumo": "creditos_consumo_miles_soles"}, inplace = True)

        # df3
        df3.rename(columns= {"Unnamed: 0": "year", "Expor-": "exportaciones_mill_dolares", "Impor-": "importaciones_mill_dolares", "Balanza": "balanza_comercial_mill_dolares", "Balanza.1": "balanza_pagos_mill_dolares", "Activos": "activos_externos_netos_corto_plazo_mill_dolares", "Deuda": "deuda_publica_externa_mill_dolares"}, inplace = True)

        # df4
        df4.rename(columns= {"2016 P/": 2016, "2017 P/": 2017, "2018 P/": 2018}, inplace = True)
        df4 = df4.T
        new_header = df4.iloc[0]
        df4 = df4[1:]
        df4.columns = new_header
        df4.drop(["I.  Activos", "II. Pasivos", "        Privada y Pública  1/", "    1. Bonos y Deuda Externa Total", "                  BCRP", "          a.    Mediano y Largo Plazo ", "           b.   Corto Plazo"], axis = 1, inplace = True)
        df4["year"] = df4.index
        df4.rename(columns= {"    1.  Activos de Reserva del BCRP": "act_reserva_BCRP_mill_dolares", "    2.  Sistema Financiero (sin BCRP)": "act_sist_financiero_sin_BCRP_mill_dolares", "    3.  Otros Activos": "act_otros_activos_mill_dolares", "                  Sector Privado    2/": "pas_med_lar_sector_privado_mill_dolares", "                  Sector Público    3/": "pas_med_lar_sector_publico_mill_dolares", "                  Sistema Financiero (sin BCRP)": "pas_cort_sist_financiero_sin_BCRP_mill_dolares", "                  BCRP   4/": "pas_cort_BCRP_mill_dolares", "                  Otros    5/": "pas_cort_otros_mill_dolares", "     2. Inversión Directa": "pas_inversion_directa_mill_dolares", "     3. Participación del Capital": "pas_participacion_capital_mill_dolares" }, inplace = True)

        # df5
        df5.drop(["Unnamed: 1", "Unnamed: 4"], axis = 1, inplace = True)
        df5.rename(columns= {"Año": "year", "Población": "poblacion_total", "Unnamed: 3": "poblacion_censada", "Unnamed: 5": "poblacion_omitida"}, inplace = True)

        # df6
        df6 = df6.T
        new_header = df6.iloc[0]
        df6 = df6[1:]
        df6.columns = new_header
        df6["year"] = df6.index
        df6 = df6[["Hombre", "Mujer", "14 a 24 años", "25 a 44 años", "45  a 64 años", "65 y más años", "Primaria o menos 1/", "Secundaria", "Superior no univeritaria", "Superior universitaria",  "De 1 a  10 personas", "De 11 a 50 personas", "Más de 50 personas", "Agricultura/Pesca/Minería", "Manufactura", "Construcción","Comercio", "Transp. y Comunicaciones ", "Otros Servicios 3/", "year"]]
        df6.rename(columns= {"Hombre": "pea_hombres", "Mujer": "pea_mujeres", "14 a 24 años": "pea_14_24_yrs", "25 a 44 años": "pea_25_44_yrs", "45  a 64 años": "pea_45_64_yrs", "65 y más años": "pea_65_o_mas_yrs", "Primaria o menos 1/": "pea_primaria_o_inferior", "Secundaria": "pea_secundaria", "Superior no univeritaria": "pea_superior_no_universitaria", "Superior universitaria": "pea_universitaria", "De 1 a  10 personas": "pea_empresa_1_10_empleados", "De 11 a 50 personas": "pea_empresa_11_50_empleados", "Más de 50 personas": "pea_empresa_50_o_mas_empleados", "Agricultura/Pesca/Minería": "pea_agricultura_pesca_mineria", "Manufactura": "pea_manufactura", "Construcción": "pea_construccion", "Comercio": "pea_comercio", "Transp. y Comunicaciones ": "pea_transporte_comunicaciones", "Otros Servicios 3/": "pea_otros_servicios" }, inplace = True)

        # df7
        df7 = df7.T
        new_header = df7.iloc[0]
        df7 = df7[1:]
        df7.columns = new_header
        df7["year"] = df7.index
        df7.rename(columns= {"Sin Seguro": "pea_sin_seguro_medico", "Con Seguro": "pea_con_seguro_medico"}, inplace = True)

        # df8
        df8 = df8.T
        new_header = df8.iloc[0]
        df8 = df8[1:]
        df8.columns = new_header
        df8["year"] = df8.index
        df8.rename(columns= {"Con 1 NBI": "perc_poblacion_con_1_nbi", "Con 2 a 5 NBI": "perc_poblacion_con_2_a_5_nbi", "Población en viviendas con características físicas inadecuadas": "perc_poblacion_nbi_vivienda_inadecuada", "Población en viviendas con hacinamiento": "perc_poblacion_nbi_vivienda_hacinada", "Población en viviendas sin servicios higiénicos": "perc_poblacion_nbi_servicios_higienicos", "Población en hogares con niños que no asisten a la escuela": "perc_poblacion_nbi_menores_sin_escuela", "Población en hogares con alta dependencia económica": "perc_poblacion_nbi_alta_dependencia_economica"}, inplace = True)
        df8 = df8[["year", "perc_poblacion_con_1_nbi", "perc_poblacion_con_2_a_5_nbi", "perc_poblacion_nbi_vivienda_inadecuada", "perc_poblacion_nbi_vivienda_hacinada", "perc_poblacion_nbi_servicios_higienicos", "perc_poblacion_nbi_menores_sin_escuela", "perc_poblacion_nbi_alta_dependencia_economica"]]

        # df9
        df9.rename(columns= {"2018 P/": 2018}, inplace = True)
        df9 = df9.T
        new_header = df9.iloc[0]
        df9 = df9[1:]
        df9.columns = new_header
        df9["year"] = df9.index
        df9.rename(columns= {"Educación Inicial": "gasto_social_educacion_inicial_mill_soles", "Educación Primaria": "gasto_social_educacion_primaria_mill_soles", "Educación Secundaria": "gasto_social_educacion_secundaria_mill_soles", "Asistencia Social": "gasto_social_asistencia_social_mill_soles", "Salud Colectiva": "gasto_social_salud_colectiva_mill_soles", "Salud Individual": "gasto_social_salud_individual_mill_soles" }, inplace = True)

        # df10
        df10.drop("Unnamed: 1", axis = 1, inplace = True)
        df10.rename(columns= {"Unnamed: 0": "year", "Unnamed: 2": "gasto_gobierno_sector_publico_mill_soles", "Unnamed: 3": "gasto_gobierno_sector_privado_mill_soles"}, inplace = True)
        df10["year"].replace({"2015  P/": 2015, "2016  P/": 2016, "2017  E/": 2017, "2018  E/": 2018}, inplace = True)

        # df11
        df11.rename(columns= {"Unnamed: 9": 2013, "Unnamed: 10": 2014, "Unnamed: 11": 2015, "Unnamed: 12": 2016, "Unnamed: 13": 2017, "Unnamed: 14": 2018}, inplace = True)
        df11.drop([6,7,8,11,12,15,16,19,20,23,24,27,28,31], axis = 0, inplace = True)
        df11 = df11.T
        new_header = ["analfabetismo_total_15_19", "analfabetismo_total_20_29", "analfabetismo_total_30_39", "analfabetismo_total_40_49", "analfabetismo_total_50_59", "analfabetismo_total_60_y_mas", "analfabetismo_h_15_19", "analfabetismo_f_15_19", "analfabetismo_h_20_29", "analfabetismo_f_20_29", "analfabetismo_h_30_39", "analfabetismo_f_30_39", "analfabetismo_h_40_49", "analfabetismo_f_40_49", "analfabetismo_h_50_59", "analfabetismo_f_50_59", "analfabetismo_h_60_y_mas", "analfabetismo_f_60_y_mas"]
        df11.columns = new_header
        df11.drop(["Grupos de edad / \nSexo"], axis = 0, inplace = True)
        df11["year"] = df11.index

        # df12
        df12.rename(columns= {"Año": "year", "Emisiones": "millones_toneladas_co2_equivalente"}, inplace = True)

        # df13
        df13 = df13.T
        new_header = df13.iloc[0]
        df13 = df13[1:]
        df13.columns = new_header
        df13["year"] = df13.index
        df13.rename(columns= {"Delitos contra la vida, el cuerpo y la salud": "delitos_vida_cuerpo_salud", "Delitos contra el honor": "delitos_honor", "Delitos contra la familia": "delitos_familia", "Delitos contra la libertad": "delitos_libertad", "Delitos contra el patrimonio": "delitos_patrimonio", "Delito contra la confianza y la buena fe en los negocios": "delitos_confianza_buena_fe_negocios", "Delitos contra los derechos intelectuales": "delitos_derechos_intelectuales", "Delitos contra el patrimonio cultural": "delitos_patrimonio_cultural", "Delitos contra el orden económico": "delitos_orden_economico", "Delitos contra el orden financiero y monetario": "delitos_orden_financiero_monetario", "Delitos tributarios": "delitos_tributarios", "Delitos contra la seguridad pública": "delitos_seguridad_publica", "Delitos ambientales": "delitos_ambientales", "Delitos contra la tranquilidad pública": "delitos_tranquilidad_publica", "Delitos contra la humanidad": "delitos_humanidad", "Delitos contra el estado y la defensa nacional": "delitos_estado_defensa_nacional", "Delitos contra los poderes del estado y el orden constitucional": "delitos_poderes_estado_orden_const", "Delito contra la voluntad popular": "delitos_voluntad_popular", "Delitos contra la administración pública": "delitos_administracion_publica", "Delitos contra la fe pública": "delitos_fe_publica"}, inplace = True)

        # df14
        df14.rename(columns= { "Año": "year", "Derechos  Ad Valorem": "trib_adu_ingr_teso_pub_DAV_mill_soles", "Derechos Específicos ": "trib_adu_ingr_teso_pub_D_especificos_mill_soles", "Sobretasa Adicional 5%": "trib_adu_ingr_teso_pub_sobretasa_ad_5perc_mill_soles", "IGV ": "trib_adu_ingr_teso_pub_IGV_mill_soles", "ISC ": "trib_adu_ingr_teso_pub_ISC_mill_soles", "Otros 1/": "trib_adu_ingr_teso_pub_otros_mill_soles", "Gobiernos Locales \n2/": "trib_adu_otros_org_gobiernos_loc_mill_soles", "INDECOPI": "trib_adu_otros_org_INDECOPI_mill_soles"}, inplace = True)
        df14["year"] = df14["year"].astype(int)

        # df15
        df_1 = df15[["Unnamed: 0", 2015, 2016, 2017, 2018]]
        df_2 = df15[["Unnamed: 0", "2015.1", "2016.1", "2017.1", "2018.1"]]
        df_1 = df_1.T
        df_2 = df_2.T
        df_1 = df_1[1:5].copy()
        df_2 = df_2[1:5].copy()
        new_header_1 = ["banca_multiple_creditos", "empresas_financieras_creditos", "cajas_municipales_creditos", "cajas_rur_ahorro_credito_creditos", "entidades_desa_peq_micr_empresa_EDPYME_creditos", "empresas_arrenda_financiero_creditos", "banco_nacion_creditos", "agrobanco_creditos", "year"]
        new_header_2 = ["banca_multiple_depositos", "empresas_financieras_depositos", "cajas_municipales_depositos", "cajas_rur_ahorro_credito_depositos", "entidades_desa_peq_micr_empresa_EDPYME_depositos", "empresas_arrenda_financiero_depositos", "banco_nacion_depositos", "agrobanco_depositos", "year"]
        df_1["year"] = df_1.index
        df_2["year"] = df_2.index
        df_1["year"] = df_1["year"].astype(int)
        df_2["year"] = df_2["year"].str.slice(0,-2).astype(int)
        df_1.columns = new_header_1
        df_2.columns = new_header_2
        df15 = pd.merge(df_1, df_2[["banca_multiple_depositos", "empresas_financieras_depositos", "cajas_municipales_depositos", "cajas_rur_ahorro_credito_depositos", "entidades_desa_peq_micr_empresa_EDPYME_depositos", "empresas_arrenda_financiero_depositos", "banco_nacion_depositos", "agrobanco_depositos", "year"]], on = "year", how = "left")

        # df16
        new_header = ["year", "sector_pesquero_PIB_mill_soles_const_2007", "sector_pesquero_VAB_mill_soles_const_2007", "sector_pesquero_porc_VAB_d_PIB", "sector_pesquero_desem_mil_ton_met", "sector_pesquero_trans_mil_ton_met", "sector_pesquero_prod_harina_pescado_mil_ton_met", "sector_pesquero_consumo_interno_total_mil_ton_met", "sector_pesquero_consumo_interno_per_capita_kg"]
        df16.columns = new_header

        # df17
        new_header = ["year", "sector_pesquero_mar_con_dir_enlatado_mil_ton_met", "sector_pesquero_mar_con_dir_congelado_mil_ton_met", "sector_pesquero_mar_con_dir_curado_mil_ton_met", "sector_pesquero_mar_con_dir_fresco_mil_ton_met", "sector_pesquero_mar_con_ind_anchoveta_mil_ton_met", "sector_pesquero_mar_con_ind_o_especies_mil_ton_met", "sector_pesquero_con_dir_curado_mil_ton_met", "sector_pesquero_con_dir_fresco_mil_ton_met", "sector_pesquero_con_dir_congelado_mil_ton_met"]
        df17.columns = new_header
        df17["year"].replace({"2018 P/": 2018, "2017 ": 2017}, inplace = True)

        # df18
        df18.rename(columns = {"2018 P/": 2018}, inplace = True)
        df18 = df18.T
        new_header = df18.iloc[0]
        df18 = df18[1:]
        df18.columns = new_header
        df18.drop(["Pelágicos 1/", "Demersales 2/", "Costeros (Pelágicos y Demersales)", "Otros Peces", "Otros Grupos", "Quelonios", "Crustáceos", "Moluscos", "Otros", "Equinodermos", "Cetáceos Menores", "Vegetales"], axis = 1, inplace = True)
        df18.rename(columns = {"Ayanque (Cachema)": "Ayanque", "Concha de Abanico": "Concha_de_Abanico", "Atún": "Atun", "Tiburón": "Tiburon", "Abalón": "abalon"}, inplace = True)
        df18.columns = [x.lower() for x in df18.columns]
        df18 = df18.add_prefix("pesca_desem_")
        df18 = df18.add_suffix("_mil_ton_metricas")
        df18["year"] = df18.index

        # df19
        df19.drop(df19.loc[df19["Giro industrial"].str.contains("Pesca |Consumo ")].index, inplace = True)
        df19.rename(columns= {"2018 P/": 2018}, inplace = True)
        df19 = df19.T
        new_header = ["pesca_trans_mar_enlatado_mil_ton_metricas", "pesca_trans_mar_congelado_mil_ton_metricas", "pesca_trans_mar_curado_mil_ton_metricas", "pesca_trans_mar_harina_pescado_mil_ton_metricas", "pesca_trans_mar_aceite_crudo_pescado_mil_ton_metricas", "pesca_trans_con_congelado_mil_ton_metricas", "pesca_trans_con_curado_mil_ton_metricas"]
        df19.drop(["Giro industrial"], axis = 0, inplace = True)
        df19.columns = new_header
        df19["year"] = df19.index

        # df20
        df20.drop(df20.loc[df20["Utilización"].str.contains("Consumo ")].index, inplace = True)
        df20.rename(columns= {"2018 P/": 2018}, inplace = True)
        df20 = df20.T
        df20.drop(["Utilización"], axis = 0, inplace = True)
        new_header = ["pesca_venta_interna_con_direc_enlatado_mil_ton_metricas", "pesca_venta_interna_con_direc_congelado_mil_ton_metricas", "pesca_venta_interna_con_direc_curado_mil_ton_metricas", "pesca_venta_interna_con_direc_fresco_mil_ton_metricas", "pesca_venta_interna_con_indirec_harina_pescado_mil_ton_metricas", "pesca_venta_interna_con_indirec_aceite_crudo_pescado_mil_ton_metricas"]
        df20.columns = new_header
        df20["year"] = df20.index

        # df21
        df21.rename(columns= {"2018 P/": 2018}, inplace = True)
        df_1 = df21.iloc[:4].copy()
        df_2 = df21.iloc[6:10].copy()
        df_1 = df_1.T
        df_2 = df_2.T
        df_1.drop(["Utilización"], axis = 0, inplace = True)
        df_2.drop(["Utilización"], axis = 0, inplace = True)
        new_header = ["pesca_consumo_interno_direc_enlatado_mil_ton_metricas", "pesca_consumo_interno_direc_congelado_mil_ton_metricas", "pesca_consumo_interno_direc_curado_mil_ton_metricas", "pesca_consumo_interno_direc_fresco_mil_ton_metricas"]
        df_1.columns = new_header
        new_header = ["pesca_consumo_interno_direc_enlatado_per_cap_kg_hab", "pesca_consumo_interno_direc_congelado_per_cap_kg_hab", "pesca_consumo_interno_direc_curado_per_cap_kg_hab", "pesca_consumo_interno_direc_fresco_per_cap_kg_hab"]
        df_2.columns = new_header
        df_1["year"] = df_1.index
        df_2["year"] = df_2.index
        df21 = pd.merge(df_1, df_2[["year", "pesca_consumo_interno_direc_enlatado_per_cap_kg_hab", "pesca_consumo_interno_direc_congelado_per_cap_kg_hab", "pesca_consumo_interno_direc_curado_per_cap_kg_hab", "pesca_consumo_interno_direc_fresco_per_cap_kg_hab"]], on = "year", how = "left")

        # df22
        df22.rename(columns= {"2018 P/": 2018}, inplace = True)
        df22 = df22.T
        df_ = df22[[2,3,6,7,9,10,12,13]].copy()
        new_header = ["pesca_n_plantas_instaladas_enlatado", "pesca_capaci_inst_enlatado_u_cajas_turno", "pesca_n_plantas_instaladas_congelado", "pesca_capaci_inst_congelado_u_ton_dia", "pesca_n_plantas_instaladas_curado", "pesca_capaci_inst_curado_u_ton_mes", "pesca_n_plantas_instaladas_harina", "pesca_capaci_inst_harina_u_ton_hora"]
        df_.columns = new_header
        df_.drop(["Rubro de producción"], axis = 0, inplace = True)
        df_['year'] = df_.index
        df22 = df_.copy()

        # df23
        new_header = ["year", "turismo_entrada_turistas", "turismo_salida_turistas", "turismo_ingreso_divisas_millones_dolares", "turismo_egreso_divisas_millones_dolares", "turismo_ingreso_divisas_per_capita_dolares", "turismo_egreso_divisas_per_capita_dolares"]
        df23.columns = new_header

        # df24
        df24 = df24.T
        df24.drop([" Lugar de Entrada"], axis = 0, inplace = True)
        new_header = ["ing_tur_intern_aeropuerto_Jorge_Chavez", "ing_tur_intern_puesto_control_Santa_Rosa","ing_tur_intern_otros_puntos"]
        df24.columns = new_header
        df24["year"] = df24.index

        # df25
        df25 = df25.T
        df25.drop(["Variable"], axis = 0, inplace = True)
        df25.drop([5,6,9,10,11], axis = 1, inplace = True)
        new_header = ["turismo_arribo_turistas_nacionales", "turismo_arribo_turistas_extranjeros", "turismo_pernoctacion_turistas_nacionales", "turismo_pernoctacion_turistas_extranjeros", "turismo_permanencia_prom_turistas_nacionales", "turismo_permanencia_prom_turistas_extranjeros"]
        df25.columns = new_header
        df25['year'] = df25.index

        # Merge of the 13 datasets
        df = pd.merge(df1, df2[["year", "agricultura_ganaderia_caza_silvicultura_miles_soles", "pesca_miles_soles", "mineria_miles_soles", "industria_manufacturera_miles_soles", "electricidad_gas_agua_miles_soles", "construccion_miles_soles", "comercio_miles_soles", "hoteles_restaurantes_miles_soles", "transporte_almacenamiento_comunicaciones_miles_soles", "intermediacion_financiera_miles_soles", "actividad_inmobiliarias_empresariales_alquiler_miles_soles", "administracion_publica_defensa_miles_soles", "ensenianza_miles_soles", "servicios_sociales_salud_miles_soles", "otras_actividades_servicios_comunitarios_miles_soles", "hogares_privados_organizaciones_extraterritoriales_miles_soles", "creditos_hipotecariosvivienda_miles_soles", "creditos_consumo_miles_soles"]], on = "year", how = "left")
        df = pd.merge(df,  df3[["year", "exportaciones_mill_dolares", "importaciones_mill_dolares", "balanza_comercial_mill_dolares", "balanza_pagos_mill_dolares", "activos_externos_netos_corto_plazo_mill_dolares", "deuda_publica_externa_mill_dolares"]], on = "year", how = "left")
        df = pd.merge(df,  df4[["year", "act_reserva_BCRP_mill_dolares", "act_sist_financiero_sin_BCRP_mill_dolares", "act_otros_activos_mill_dolares", "pas_med_lar_sector_privado_mill_dolares", "pas_med_lar_sector_publico_mill_dolares", "pas_cort_sist_financiero_sin_BCRP_mill_dolares", "pas_cort_BCRP_mill_dolares", "pas_cort_otros_mill_dolares", "pas_inversion_directa_mill_dolares", "pas_participacion_capital_mill_dolares"]], on = "year", how = "left")
        df = pd.merge(df,  df5[["year", "poblacion_total", "poblacion_censada", "poblacion_omitida"]], on = "year", how = "left")
        df = pd.merge(df,  df6[["year", "pea_hombres", "pea_mujeres", "pea_14_24_yrs", "pea_25_44_yrs", "pea_45_64_yrs", "pea_65_o_mas_yrs", "pea_primaria_o_inferior", "pea_secundaria", "pea_superior_no_universitaria", "pea_universitaria", "pea_empresa_1_10_empleados", "pea_empresa_11_50_empleados", "pea_empresa_50_o_mas_empleados", "pea_agricultura_pesca_mineria", "pea_manufactura", "pea_construccion", "pea_comercio", "pea_transporte_comunicaciones", "pea_otros_servicios"]], on = "year", how = "left")
        df = pd.merge(df,  df7[["year", "pea_sin_seguro_medico", "pea_con_seguro_medico"]], on = "year", how = "left")
        df = pd.merge(df,  df8[["year", "perc_poblacion_con_1_nbi", "perc_poblacion_con_2_a_5_nbi", "perc_poblacion_nbi_vivienda_inadecuada", "perc_poblacion_nbi_vivienda_hacinada", "perc_poblacion_nbi_servicios_higienicos", "perc_poblacion_nbi_menores_sin_escuela", "perc_poblacion_nbi_alta_dependencia_economica"]], on = "year", how = "left")
        df = pd.merge(df,  df9[["year", "gasto_social_educacion_inicial_mill_soles", "gasto_social_educacion_primaria_mill_soles", "gasto_social_educacion_secundaria_mill_soles", "gasto_social_asistencia_social_mill_soles", "gasto_social_salud_colectiva_mill_soles", "gasto_social_salud_individual_mill_soles"]], on = "year", how = "left")
        df = pd.merge(df, df10[["year", "gasto_gobierno_sector_publico_mill_soles", "gasto_gobierno_sector_privado_mill_soles"]], on = "year", how = "left")
        df = pd.merge(df, df11[["year", "analfabetismo_total_15_19", "analfabetismo_total_20_29", "analfabetismo_total_30_39", "analfabetismo_total_40_49", "analfabetismo_total_50_59", "analfabetismo_total_60_y_mas", "analfabetismo_h_15_19", "analfabetismo_f_15_19", "analfabetismo_h_20_29", "analfabetismo_f_20_29", "analfabetismo_h_30_39", "analfabetismo_f_30_39", "analfabetismo_h_40_49", "analfabetismo_f_40_49", "analfabetismo_h_50_59", "analfabetismo_f_50_59", "analfabetismo_h_60_y_mas", "analfabetismo_f_60_y_mas"]], on = "year", how = "left")
        df = pd.merge(df, df12[["year", "millones_toneladas_co2_equivalente"]], on = "year", how = "left")
        df = pd.merge(df, df13[["delitos_vida_cuerpo_salud", "delitos_honor", "delitos_familia", "delitos_libertad", "delitos_patrimonio", "delitos_confianza_buena_fe_negocios", "delitos_derechos_intelectuales", "delitos_patrimonio_cultural", "delitos_orden_economico", "delitos_orden_financiero_monetario", "delitos_tributarios", "delitos_seguridad_publica", "delitos_ambientales", "delitos_tranquilidad_publica", "delitos_humanidad", "delitos_estado_defensa_nacional", "delitos_poderes_estado_orden_const", "delitos_voluntad_popular", "delitos_administracion_publica", "delitos_fe_publica", "year"]], on = "year", how = "left")
        df = pd.merge(df, df14[["year", "trib_adu_ingr_teso_pub_DAV_mill_soles", "trib_adu_ingr_teso_pub_D_especificos_mill_soles", "trib_adu_ingr_teso_pub_sobretasa_ad_5perc_mill_soles", "trib_adu_ingr_teso_pub_IGV_mill_soles", "trib_adu_ingr_teso_pub_ISC_mill_soles", "trib_adu_ingr_teso_pub_otros_mill_soles", "trib_adu_otros_org_gobiernos_loc_mill_soles", "trib_adu_otros_org_INDECOPI_mill_soles"]], on = "year", how = "left")

        df = pd.merge(df, df15[["year", "banca_multiple_creditos", "empresas_financieras_creditos", "cajas_municipales_creditos", "cajas_rur_ahorro_credito_creditos", "entidades_desa_peq_micr_empresa_EDPYME_creditos", "empresas_arrenda_financiero_creditos", "banco_nacion_creditos", "agrobanco_creditos", "banca_multiple_depositos", "empresas_financieras_depositos", "cajas_municipales_depositos", "cajas_rur_ahorro_credito_depositos", "entidades_desa_peq_micr_empresa_EDPYME_depositos", "empresas_arrenda_financiero_depositos", "banco_nacion_depositos", "agrobanco_depositos"]], on = "year", how = "left")
        df = pd.merge(df, df16[["year", "sector_pesquero_PIB_mill_soles_const_2007", "sector_pesquero_VAB_mill_soles_const_2007", "sector_pesquero_porc_VAB_d_PIB", "sector_pesquero_desem_mil_ton_met", "sector_pesquero_trans_mil_ton_met", "sector_pesquero_prod_harina_pescado_mil_ton_met", "sector_pesquero_consumo_interno_total_mil_ton_met", "sector_pesquero_consumo_interno_per_capita_kg"]], on = "year", how = "left")
        df = pd.merge(df, df17[["year", "sector_pesquero_mar_con_dir_enlatado_mil_ton_met", "sector_pesquero_mar_con_dir_congelado_mil_ton_met", "sector_pesquero_mar_con_dir_curado_mil_ton_met", "sector_pesquero_mar_con_dir_fresco_mil_ton_met", "sector_pesquero_mar_con_ind_anchoveta_mil_ton_met", "sector_pesquero_mar_con_ind_o_especies_mil_ton_met", "sector_pesquero_con_dir_curado_mil_ton_met", "sector_pesquero_con_dir_fresco_mil_ton_met", "sector_pesquero_con_dir_congelado_mil_ton_met"]], on = "year", how = "left")
        df = pd.merge(df, df18[["year", "pesca_desem_anchoveta_mil_ton_metricas", "pesca_desem_atun_mil_ton_metricas", "pesca_desem_bonito_mil_ton_metricas", "pesca_desem_caballa_mil_ton_metricas", "pesca_desem_jurel_mil_ton_metricas", "pesca_desem_perico_mil_ton_metricas", "pesca_desem_samasa_mil_ton_metricas", "pesca_desem_sardina_mil_ton_metricas", "pesca_desem_tiburon_mil_ton_metricas", "pesca_desem_ayanque_mil_ton_metricas", "pesca_desem_cabrilla_mil_ton_metricas", "pesca_desem_coco_mil_ton_metricas", "pesca_desem_lenguado_mil_ton_metricas", "pesca_desem_merluza_mil_ton_metricas", "pesca_desem_raya_mil_ton_metricas", "pesca_desem_tollo_mil_ton_metricas", "pesca_desem_cabinza_mil_ton_metricas", "pesca_desem_cojinova_mil_ton_metricas", "pesca_desem_corvina_mil_ton_metricas", "pesca_desem_chita_mil_ton_metricas", "pesca_desem_liza_mil_ton_metricas", "pesca_desem_lorna_mil_ton_metricas", "pesca_desem_machete_mil_ton_metricas", "pesca_desem_pejerrey_mil_ton_metricas", "pesca_desem_pintadilla_mil_ton_metricas", "pesca_desem_cangrejo_mil_ton_metricas", "pesca_desem_langosta_mil_ton_metricas", "pesca_desem_langostino_mil_ton_metricas", "pesca_desem_abalon_mil_ton_metricas", "pesca_desem_caracol_mil_ton_metricas", "pesca_desem_choro_mil_ton_metricas", "pesca_desem_concha_de_abanico_mil_ton_metricas", "pesca_desem_macha_mil_ton_metricas", "pesca_desem_almeja_mil_ton_metricas", "pesca_desem_calamar_mil_ton_metricas", "pesca_desem_pota_mil_ton_metricas", "pesca_desem_pulpo_mil_ton_metricas"]], on = "year", how = "left")
        df = pd.merge(df, df19[["year", "pesca_trans_mar_enlatado_mil_ton_metricas", "pesca_trans_mar_congelado_mil_ton_metricas", "pesca_trans_mar_curado_mil_ton_metricas", "pesca_trans_mar_harina_pescado_mil_ton_metricas", "pesca_trans_mar_aceite_crudo_pescado_mil_ton_metricas", "pesca_trans_con_congelado_mil_ton_metricas", "pesca_trans_con_curado_mil_ton_metricas"]], on = "year", how = "left")
        df = pd.merge(df, df20[["year", "pesca_venta_interna_con_direc_enlatado_mil_ton_metricas", "pesca_venta_interna_con_direc_congelado_mil_ton_metricas", "pesca_venta_interna_con_direc_curado_mil_ton_metricas", "pesca_venta_interna_con_direc_fresco_mil_ton_metricas", "pesca_venta_interna_con_indirec_harina_pescado_mil_ton_metricas", "pesca_venta_interna_con_indirec_aceite_crudo_pescado_mil_ton_metricas"]], on = "year", how = "left")
        df = pd.merge(df, df21[["year", "pesca_consumo_interno_direc_enlatado_mil_ton_metricas", "pesca_consumo_interno_direc_congelado_mil_ton_metricas", "pesca_consumo_interno_direc_curado_mil_ton_metricas", "pesca_consumo_interno_direc_fresco_mil_ton_metricas", "pesca_consumo_interno_direc_enlatado_per_cap_kg_hab", "pesca_consumo_interno_direc_congelado_per_cap_kg_hab", "pesca_consumo_interno_direc_curado_per_cap_kg_hab", "pesca_consumo_interno_direc_fresco_per_cap_kg_hab"]], on = "year", how = "left")
        df = pd.merge(df, df22[["year", "pesca_n_plantas_instaladas_enlatado", "pesca_capaci_inst_enlatado_u_cajas_turno", "pesca_n_plantas_instaladas_congelado", "pesca_capaci_inst_congelado_u_ton_dia", "pesca_n_plantas_instaladas_curado", "pesca_capaci_inst_curado_u_ton_mes", "pesca_n_plantas_instaladas_harina", "pesca_capaci_inst_harina_u_ton_hora"]], on = "year", how = "left")
        df = pd.merge(df, df23[["year", "turismo_entrada_turistas", "turismo_salida_turistas", "turismo_ingreso_divisas_millones_dolares", "turismo_egreso_divisas_millones_dolares", "turismo_ingreso_divisas_per_capita_dolares", "turismo_egreso_divisas_per_capita_dolares"]], on = "year", how = "left")
        df = pd.merge(df, df24[["year", "ing_tur_intern_aeropuerto_Jorge_Chavez", "ing_tur_intern_puesto_control_Santa_Rosa", "ing_tur_intern_otros_puntos"]], on = "year", how = "left")
        df = pd.merge(df, df25[["year", "turismo_arribo_turistas_nacionales", "turismo_arribo_turistas_extranjeros", "turismo_pernoctacion_turistas_nacionales", "turismo_pernoctacion_turistas_extranjeros", "turismo_permanencia_prom_turistas_nacionales", "turismo_permanencia_prom_turistas_extranjeros"]], on = "year", how = "left")

        df.replace("-", 0, inplace = True)
        # Changing str values to float/int values
        for col in df.columns:
            df[col] = df[col].astype(float)

        df["ubigeo"] = "per"

        return df

class itp_indicators_y_n_nat_pipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))

        dtype = {
            "ubigeo":                                                                       "String",
            "year":                                                                         "UInt16",
            "producto_interno_bruto_mill_n_soles":                                          "UInt32",
            "remuneraciones_mill_n_soles":                                                  "UInt16",
            "derechos_importacion_mill_n_soles":                                            "UInt16",
            "impuestos_productos_mill_n_soles":                                             "UInt16",
            "otros_impuestos_mill_n_soles":                                                 "UInt16",
            "ingreso_explotacion_mill_n_soles":                                             "UInt32",
            "excedente_explotacion_bruto_mill_n_soles":                                     "UInt32",
            "ingreso_mixto_mill_n_soles":                                                   "UInt32",
            "agricultura_ganaderia_caza_silvicultura_miles_soles":                          "UInt32",
            "pesca_miles_soles":                                                            "UInt32",
            "mineria_miles_soles":                                                          "UInt32",
            "industria_manufacturera_miles_soles":                                          "UInt32",
            "electricidad_gas_agua_miles_soles":                                            "UInt32",
            "construccion_miles_soles":                                                     "UInt32",
            "comercio_miles_soles":                                                         "UInt32",
            "hoteles_restaurantes_miles_soles":                                             "UInt32",
            "transporte_almacenamiento_comunicaciones_miles_soles":                         "UInt32",
            "intermediacion_financiera_miles_soles":                                        "UInt32",
            "actividad_inmobiliarias_empresariales_alquiler_miles_soles":                   "UInt32",
            "administracion_publica_defensa_miles_soles":                                   "UInt32",
            "ensenianza_miles_soles":                                                       "UInt32",
            "servicios_sociales_salud_miles_soles":                                         "UInt32",
            "otras_actividades_servicios_comunitarios_miles_soles":                         "UInt32",
            "hogares_privados_organizaciones_extraterritoriales_miles_soles":               "UInt32",
            "creditos_hipotecariosvivienda_miles_soles":                                    "UInt32",
            "creditos_consumo_miles_soles":                                                 "UInt32",
            "exportaciones_mill_dolares":                                                   "UInt32",
            "importaciones_mill_dolares":                                                   "UInt32",
            "balanza_comercial_mill_dolares":                                               "Int32",
            "balanza_pagos_mill_dolares":                                                   "Int32",
            "activos_externos_netos_corto_plazo_mill_dolares":                              "UInt32",
            "deuda_publica_externa_mill_dolares":                                           "UInt32",
            "act_reserva_BCRP_mill_dolares":                                                "UInt32",
            "act_sist_financiero_sin_BCRP_mill_dolares":                                    "UInt32",
            "act_otros_activos_mill_dolares":                                               "UInt32",
            "pas_med_lar_sector_privado_mill_dolares":                                      "UInt32",
            "pas_med_lar_sector_publico_mill_dolares":                                      "UInt32",
            "pas_cort_sist_financiero_sin_BCRP_mill_dolares":                               "UInt16",
            "pas_cort_BCRP_mill_dolares":                                                   "UInt16",
            "pas_cort_otros_mill_dolares":                                                  "UInt16",
            "pas_inversion_directa_mill_dolares":                                           "UInt32",
            "pas_participacion_capital_mill_dolares":                                       "UInt16",
            "poblacion_total":                                                              "UInt32",
            "poblacion_censada":                                                            "UInt32",
            "poblacion_omitida":                                                            "UInt32",
            "pea_hombres":                                                                  "Float32",
            "pea_mujeres":                                                                  "Float32",
            "pea_14_24_yrs":                                                                "Float32",
            "pea_25_44_yrs":                                                                "Float32",
            "pea_45_64_yrs":                                                                "Float32",
            "pea_65_o_mas_yrs":                                                             "Float32",
            "pea_primaria_o_inferior":                                                      "Float32",
            "pea_secundaria":                                                               "Float32",
            "pea_superior_no_universitaria":                                                "Float32",
            "pea_universitaria":                                                            "Float32",
            "pea_empresa_1_10_empleados":                                                   "Float32",
            "pea_empresa_11_50_empleados":                                                  "Float32",
            "pea_empresa_50_o_mas_empleados":                                               "Float32",
            "pea_agricultura_pesca_mineria":                                                "Float32",
            "pea_manufactura":                                                              "Float32",
            "pea_construccion":                                                             "Float32",
            "pea_comercio":                                                                 "Float32",
            "pea_transporte_comunicaciones":                                                "Float32",
            "pea_otros_servicios":                                                          "Float32",
            "pea_sin_seguro_medico":                                                        "Float32",
            "pea_con_seguro_medico":                                                        "Float32",
            "perc_poblacion_con_1_nbi":                                                     "Float32",
            "perc_poblacion_con_2_a_5_nbi":                                                 "Float32",
            "perc_poblacion_nbi_vivienda_inadecuada":                                       "Float32",
            "perc_poblacion_nbi_vivienda_hacinada":                                         "Float32",
            "perc_poblacion_nbi_servicios_higienicos":                                      "Float32",
            "perc_poblacion_nbi_menores_sin_escuela":                                       "Float32",
            "perc_poblacion_nbi_alta_dependencia_economica":                                "Float32",
            "gasto_social_educacion_inicial_mill_soles":                                    "UInt16",
            "gasto_social_educacion_primaria_mill_soles":                                   "UInt16",
            "gasto_social_educacion_secundaria_mill_soles":                                 "UInt16",
            "gasto_social_asistencia_social_mill_soles":                                    "UInt16",
            "gasto_social_salud_colectiva_mill_soles":                                      "UInt16",
            "gasto_social_salud_individual_mill_soles":                                     "UInt16",
            "gasto_gobierno_sector_publico_mill_soles":                                     "UInt32",
            "gasto_gobierno_sector_privado_mill_soles":                                     "UInt32",
            "analfabetismo_total_15_19":                                                    "Float32",
            "analfabetismo_total_20_29":                                                    "Float32",
            "analfabetismo_total_30_39":                                                    "Float32",
            "analfabetismo_total_40_49":                                                    "Float32",
            "analfabetismo_total_50_59":                                                    "Float32",
            "analfabetismo_total_60_y_mas":                                                 "Float32",
            "analfabetismo_h_15_19":                                                        "Float32",
            "analfabetismo_f_15_19":                                                        "Float32",
            "analfabetismo_h_20_29":                                                        "Float32",
            "analfabetismo_f_20_29":                                                        "Float32",
            "analfabetismo_h_30_39":                                                        "Float32",
            "analfabetismo_f_30_39":                                                        "Float32",
            "analfabetismo_h_40_49":                                                        "Float32",
            "analfabetismo_f_40_49":                                                        "Float32",
            "analfabetismo_h_50_59":                                                        "Float32",
            "analfabetismo_f_50_59":                                                        "Float32",
            "analfabetismo_h_60_y_mas":                                                     "Float32",
            "analfabetismo_f_60_y_mas":                                                     "Float32",
            "millones_toneladas_co2_equivalente":                                           "UInt16",
            "delitos_vida_cuerpo_salud":                                                    "UInt16",
            "delitos_honor":                                                                "UInt16",
            "delitos_familia":                                                              "UInt16",
            "delitos_libertad":                                                             "UInt16",
            "delitos_patrimonio":                                                           "UInt16",
            "delitos_confianza_buena_fe_negocios":                                          "UInt16",
            "delitos_derechos_intelectuales":                                               "UInt16",
            "delitos_patrimonio_cultural":                                                  "UInt16",
            "delitos_orden_economico":                                                      "UInt16",
            "delitos_orden_financiero_monetario":                                           "UInt16",
            "delitos_tributarios":                                                          "UInt16",
            "delitos_seguridad_publica":                                                    "UInt16",
            "delitos_ambientales":                                                          "UInt16",
            "delitos_tranquilidad_publica":                                                 "UInt16",
            "delitos_humanidad":                                                            "UInt16",
            "delitos_estado_defensa_nacional":                                              "UInt16",
            "delitos_poderes_estado_orden_const":                                           "UInt16",
            "delitos_voluntad_popular":                                                     "UInt16",
            "delitos_administracion_publica":                                               "UInt16",
            "delitos_fe_publica":                                                           "UInt16",
            "trib_adu_ingr_teso_pub_DAV_mill_soles":                                        "Float32",
            "trib_adu_ingr_teso_pub_D_especificos_mill_soles":                              "Float32",
            "trib_adu_ingr_teso_pub_sobretasa_ad_5perc_mill_soles":                         "Float32",
            "trib_adu_ingr_teso_pub_IGV_mill_soles":                                        "Float32",
            "trib_adu_ingr_teso_pub_ISC_mill_soles":                                        "Float32",
            "trib_adu_ingr_teso_pub_otros_mill_soles":                                      "Float32",
            "trib_adu_otros_org_gobiernos_loc_mill_soles":                                  "Float32",
            "trib_adu_otros_org_INDECOPI_mill_soles":                                       "Float32",
            "banca_multiple_creditos":                                                      "Float32",
            "empresas_financieras_creditos":                                                "Float32",
            "cajas_municipales_creditos":                                                   "Float32",
            "cajas_rur_ahorro_credito_creditos":                                            "Float32",
            "entidades_desa_peq_micr_empresa_EDPYME_creditos":                              "Float32",
            "empresas_arrenda_financiero_creditos":                                         "Float32",
            "banco_nacion_creditos":                                                        "Float32",
            "agrobanco_creditos":                                                           "Float32",
            "banca_multiple_depositos":                                                     "Float32",
            "empresas_financieras_depositos":                                               "Float32",
            "cajas_municipales_depositos":                                                  "Float32",
            "cajas_rur_ahorro_credito_depositos":                                           "Float32",
            "entidades_desa_peq_micr_empresa_EDPYME_depositos":                             "Float32",
            "empresas_arrenda_financiero_depositos":                                        "Float32",
            "banco_nacion_depositos":                                                       "Float32",
            "agrobanco_depositos":                                                          "Float32",

            "sector_pesquero_PIB_mill_soles_const_2007":                                    "UInt32",
            "sector_pesquero_VAB_mill_soles_const_2007":                                    "UInt16",
            "sector_pesquero_porc_VAB_d_PIB":                                               "Float32",
            "sector_pesquero_desem_mil_ton_met":                                            "Float32",
            "sector_pesquero_trans_mil_ton_met":                                            "Float32",
            "sector_pesquero_prod_harina_pescado_mil_ton_met":                              "Float32",
            "sector_pesquero_consumo_interno_total_mil_ton_met":                            "Float32",
            "sector_pesquero_consumo_interno_per_capita_kg":                                "Float32",
            "sector_pesquero_mar_con_dir_enlatado_mil_ton_met":                             "Float32",
            "sector_pesquero_mar_con_dir_congelado_mil_ton_met":                            "Float32",
            "sector_pesquero_mar_con_dir_curado_mil_ton_met":                               "Float32",
            "sector_pesquero_mar_con_dir_fresco_mil_ton_met":                               "Float32",
            "sector_pesquero_mar_con_ind_anchoveta_mil_ton_met":                            "Float32",
            "sector_pesquero_mar_con_ind_o_especies_mil_ton_met":                           "Float32",
            "sector_pesquero_con_dir_curado_mil_ton_met":                                   "Float32",
            "sector_pesquero_con_dir_fresco_mil_ton_met":                                   "Float32",
            "sector_pesquero_con_dir_congelado_mil_ton_met":                                "Float32",
            "pesca_desem_anchoveta_mil_ton_metricas":                                       "Float32",
            "pesca_desem_atun_mil_ton_metricas":                                            "Float32",
            "pesca_desem_bonito_mil_ton_metricas":                                          "Float32",
            "pesca_desem_caballa_mil_ton_metricas":                                         "Float32",
            "pesca_desem_jurel_mil_ton_metricas":                                           "Float32",
            "pesca_desem_perico_mil_ton_metricas":                                          "Float32",
            "pesca_desem_samasa_mil_ton_metricas":                                          "Float32",
            "pesca_desem_sardina_mil_ton_metricas":                                         "Float32",
            "pesca_desem_tiburon_mil_ton_metricas":                                         "Float32",
            "pesca_desem_ayanque_mil_ton_metricas":                                         "Float32",
            "pesca_desem_cabrilla_mil_ton_metricas":                                        "Float32",
            "pesca_desem_coco_mil_ton_metricas":                                            "Float32",
            "pesca_desem_lenguado_mil_ton_metricas":                                        "Float32",
            "pesca_desem_merluza_mil_ton_metricas":                                         "Float32",
            "pesca_desem_raya_mil_ton_metricas":                                            "Float32",
            "pesca_desem_tollo_mil_ton_metricas":                                           "Float32",
            "pesca_desem_cabinza_mil_ton_metricas":                                         "Float32",
            "pesca_desem_cojinova_mil_ton_metricas":                                        "Float32",
            "pesca_desem_corvina_mil_ton_metricas":                                         "Float32",
            "pesca_desem_chita_mil_ton_metricas":                                           "Float32",
            "pesca_desem_liza_mil_ton_metricas":                                            "Float32",
            "pesca_desem_lorna_mil_ton_metricas":                                           "Float32",
            "pesca_desem_machete_mil_ton_metricas":                                         "Float32",
            "pesca_desem_pejerrey_mil_ton_metricas":                                        "Float32",
            "pesca_desem_pintadilla_mil_ton_metricas":                                      "Float32",
            "pesca_desem_cangrejo_mil_ton_metricas":                                        "Float32",
            "pesca_desem_langosta_mil_ton_metricas":                                        "Float32",
            "pesca_desem_langostino_mil_ton_metricas":                                      "Float32",
            "pesca_desem_abalon_mil_ton_metricas":                                          "Float32",
            "pesca_desem_caracol_mil_ton_metricas":                                         "Float32",
            "pesca_desem_choro_mil_ton_metricas":                                           "Float32",
            "pesca_desem_concha_de_abanico_mil_ton_metricas":                               "Float32",
            "pesca_desem_macha_mil_ton_metricas":                                           "Float32",
            "pesca_desem_almeja_mil_ton_metricas":                                          "Float32",
            "pesca_desem_calamar_mil_ton_metricas":                                         "Float32",
            "pesca_desem_pota_mil_ton_metricas":                                            "Float32",
            "pesca_desem_pulpo_mil_ton_metricas":                                           "Float32",
            "pesca_trans_mar_enlatado_mil_ton_metricas":                                    "Float32",
            "pesca_trans_mar_congelado_mil_ton_metricas":                                   "Float32",
            "pesca_trans_mar_curado_mil_ton_metricas":                                      "Float32",
            "pesca_trans_mar_harina_pescado_mil_ton_metricas":                              "Float32",
            "pesca_trans_mar_aceite_crudo_pescado_mil_ton_metricas":                        "Float32",
            "pesca_trans_con_congelado_mil_ton_metricas":                                   "Float32",
            "pesca_trans_con_curado_mil_ton_metricas":                                      "Float32",
            "pesca_venta_interna_con_direc_enlatado_mil_ton_metricas":                      "Float32",
            "pesca_venta_interna_con_direc_congelado_mil_ton_metricas":                     "Float32",
            "pesca_venta_interna_con_direc_curado_mil_ton_metricas":                        "Float32",
            "pesca_venta_interna_con_direc_fresco_mil_ton_metricas":                        "Float32",
            "pesca_venta_interna_con_indirec_harina_pescado_mil_ton_metricas":              "Float32",
            "pesca_venta_interna_con_indirec_aceite_crudo_pescado_mil_ton_metricas":        "Float32",
            "pesca_consumo_interno_direc_enlatado_mil_ton_metricas":                        "Float32",
            "pesca_consumo_interno_direc_congelado_mil_ton_metricas":                       "Float32",
            "pesca_consumo_interno_direc_curado_mil_ton_metricas":                          "Float32",
            "pesca_consumo_interno_direc_fresco_mil_ton_metricas":                          "Float32",
            "pesca_consumo_interno_direc_enlatado_per_cap_kg_hab":                          "Float32",
            "pesca_consumo_interno_direc_congelado_per_cap_kg_hab":                         "Float32",
            "pesca_consumo_interno_direc_curado_per_cap_kg_hab":                            "Float32",
            "pesca_consumo_interno_direc_fresco_per_cap_kg_hab":                            "Float32",
            "pesca_n_plantas_instaladas_enlatado":                                          "UInt8",
            "pesca_capaci_inst_enlatado_u_cajas_turno":                                     "UInt32",
            "pesca_n_plantas_instaladas_congelado":                                         "UInt8",
            "pesca_capaci_inst_congelado_u_ton_dia":                                        "UInt32",
            "pesca_n_plantas_instaladas_curado":                                            "UInt8",
            "pesca_capaci_inst_curado_u_ton_mes":                                           "UInt32",
            "pesca_n_plantas_instaladas_harina":                                            "UInt8",
            "pesca_capaci_inst_harina_u_ton_hora":                                          "UInt32",
            "turismo_entrada_turistas":                                                     "UInt32",
            "turismo_salida_turistas":                                                      "UInt32",
            "turismo_ingreso_divisas_millones_dolares":                                     "Float32",
            "turismo_egreso_divisas_millones_dolares":                                      "Float32",
            "turismo_ingreso_divisas_per_capita_dolares":                                   "Float32",
            "turismo_egreso_divisas_per_capita_dolares":                                    "Float32",
            "ing_tur_intern_aeropuerto_Jorge_Chavez":                                       "UInt32",
            "ing_tur_intern_puesto_control_Santa_Rosa":                                     "UInt32",
            "ing_tur_intern_otros_puntos":                                                  "UInt32",
            "turismo_arribo_turistas_nacionales":                                           "UInt32",
            "turismo_arribo_turistas_extranjeros":                                          "UInt32",
            "turismo_pernoctacion_turistas_nacionales":                                     "UInt32",
            "turismo_pernoctacion_turistas_extranjeros":                                    "UInt32",
            "turismo_permanencia_prom_turistas_nacionales":                                 "Float32",
            "turismo_permanencia_prom_turistas_extranjeros":                                "Float32"
            }

        transform_step = TransformStep()
        agg_step = AggregatorStep("itp_indicators_y_n_nat", measures=["producto_interno_bruto_mill_n_soles", "remuneraciones_mill_n_soles", "derechos_importacion_mill_n_soles", "impuestos_productos_mill_n_soles", "otros_impuestos_mill_n_soles", "ingreso_explotacion_mill_n_soles", "excedente_explotacion_bruto_mill_n_soles", "ingreso_mixto_mill_n_soles", "agricultura_ganaderia_caza_silvicultura_miles_soles", "pesca_miles_soles", "mineria_miles_soles", "industria_manufacturera_miles_soles", "electricidad_gas_agua_miles_soles", "construccion_miles_soles", "extraccion_petroleo_gas_minerales_servicios_conexos_miles_soles", "manufactura", "comercio_miles_soles", "hoteles_restaurantes_miles_soles", "transporte_almacenamiento_comunicaciones_miles_soles", "intermediacion_financiera_miles_soles", "actividad_inmobiliarias_empresariales_alquiler_miles_soles", "administracion_publica_defensa_miles_soles", "ensenianza_miles_soles", "servicios_sociales_salud_miles_soles", "otras_actividades_servicios_comunitarios_miles_soles", "hogares_privados_organizaciones_extraterritoriales_miles_soles", "creditos_hipotecariosvivienda_miles_soles", "creditos_consumo_miles_soles", "exportaciones_mill_dolares", "importaciones_mill_dolares", "balanza_comercial_mill_dolares", "balanza_pagos_mill_dolares", "activos_externos_netos_corto_plazo_mill_dolares", "deuda_publica_externa_mill_dolares", "act_reserva_BCRP_mill_dolares", "act_sist_financiero_sin_BCRP_mill_dolares", "act_otros_activos_mill_dolares", "pas_med_lar_sector_privado_mill_dolares", "pas_med_lar_sector_publico_mill_dolares", "pas_cort_sist_financiero_sin_BCRP_mill_dolares", "pas_cort_BCRP_mill_dolares", "pas_cort_otros_mill_dolares", "pas_inversion_directa_mill_dolares", "pas_participacion_capital_mill_dolares", "poblacion_total", "poblacion_censada", "poblacion_omitida", "pea_hombres", "pea_mujeres", "pea_14_24_yrs", "pea_25_44_yrs", "pea_45_64_yrs", "pea_65_o_mas_yrs", "pea_primaria_o_inferior", "pea_secundaria", "pea_superior_no_universitaria", "pea_universitaria", "pea_empresa_1_10_empleados", "pea_empresa_11_50_empleados", "pea_empresa_50_o_mas_empleados", "pea_agricultura_pesca_mineria", "pea_manufactura", "pea_construccion", "pea_comercio", "pea_transporte_comunicaciones", "pea_otros_servicios", "pea_sin_seguro_medico", "pea_con_seguro_medico", "perc_poblacion_con_1_nbi", "perc_poblacion_con_2_a_5_nbi", "perc_poblacion_nbi_vivienda_inadecuada", "perc_poblacion_nbi_vivienda_hacinada", "perc_poblacion_nbi_servicios_higienicos", "perc_poblacion_nbi_menores_sin_escuela", "perc_poblacion_nbi_alta_dependencia_economica", "gasto_social_educacion_inicial_mill_soles", "gasto_social_educacion_primaria_mill_soles", "gasto_social_educacion_secundaria_mill_soles", "gasto_social_asistencia_social_mill_soles", "gasto_social_salud_colectiva_mill_soles", "gasto_social_salud_individual_mill_soles", "gasto_gobierno_sector_publico_mill_soles", "gasto_gobierno_sector_privado_mill_soles", "analfabetismo_total_15_19", "analfabetismo_total_20_29", "analfabetismo_total_30_39", "analfabetismo_total_40_49", "analfabetismo_total_50_59", "analfabetismo_total_60_y_mas", "analfabetismo_h_15_19", "analfabetismo_f_15_19", "analfabetismo_h_20_29", "analfabetismo_f_20_29", "analfabetismo_h_30_39", "analfabetismo_f_30_39", "analfabetismo_h_40_49", "analfabetismo_f_40_49", "analfabetismo_h_50_59", "analfabetismo_f_50_59", "analfabetismo_h_60_y_mas", "analfabetismo_f_60_y_mas", "millones_toneladas_co2_equivalente", "delitos_vida_cuerpo_salud", "delitos_honor", "delitos_familia", "delitos_libertad", "delitos_patrimonio", "delitos_confianza_buena_fe_negocios", "delitos_derechos_intelectuales", "delitos_patrimonio_cultural", "delitos_orden_economico", "delitos_orden_financiero_monetario", "delitos_tributarios", "delitos_seguridad_publica", "delitos_ambientales", "delitos_tranquilidad_publica", "delitos_humanidad", "delitos_estado_defensa_nacional", "delitos_poderes_estado_orden_const", "delitos_voluntad_popular", "delitos_administracion_publica", "delitos_fe_publica", "trib_adu_ingr_teso_pub_DAV_mill_soles", "trib_adu_ingr_teso_pub_D_especificos_mill_soles", "trib_adu_ingr_teso_pub_sobretasa_ad_5perc_mill_soles", "trib_adu_ingr_teso_pub_IGV_mill_soles", "trib_adu_ingr_teso_pub_ISC_mill_soles", "trib_adu_ingr_teso_pub_otros_mill_soles", "trib_adu_otros_org_gobiernos_loc_mill_soles", "trib_adu_otros_org_INDECOPI_mill_soles", "banca_multiple_creditos", "empresas_financieras_creditos", "cajas_municipales_creditos", "cajas_rur_ahorro_credito_creditos", "entidades_desa_peq_micr_empresa_EDPYME_creditos", "empresas_arrenda_financiero_creditos", "banco_nacion_creditos", "agrobanco_creditos", "banca_multiple_depositos", "empresas_financieras_depositos", "cajas_municipales_depositos", "cajas_rur_ahorro_credito_depositos", "entidades_desa_peq_micr_empresa_EDPYME_depositos", "empresas_arrenda_financiero_depositos", "banco_nacion_depositos", "agrobanco_depositos", "sector_pesquero_PIB_mill_soles_const_2007", "sector_pesquero_VAB_mill_soles_const_2007", "sector_pesquero_porc_VAB_d_PIB", "sector_pesquero_desem_mil_ton_met", "sector_pesquero_trans_mil_ton_met", "sector_pesquero_prod_harina_pescado_mil_ton_met", "sector_pesquero_consumo_interno_total_mil_ton_met", "sector_pesquero_consumo_interno_per_capita_kg", "sector_pesquero_mar_con_dir_enlatado_mil_ton_met", "sector_pesquero_mar_con_dir_congelado_mil_ton_met", "sector_pesquero_mar_con_dir_curado_mil_ton_met", "sector_pesquero_mar_con_dir_fresco_mil_ton_met", "sector_pesquero_mar_con_ind_anchoveta_mil_ton_met", "sector_pesquero_mar_con_ind_o_especies_mil_ton_met", "sector_pesquero_con_dir_curado_mil_ton_met", "sector_pesquero_con_dir_fresco_mil_ton_met", "sector_pesquero_con_dir_congelado_mil_ton_met", "pesca_desem_anchoveta_mil_ton_metricas", "pesca_desem_atun_mil_ton_metricas", "pesca_desem_bonito_mil_ton_metricas", "pesca_desem_caballa_mil_ton_metricas", "pesca_desem_jurel_mil_ton_metricas", "pesca_desem_perico_mil_ton_metricas", "pesca_desem_samasa_mil_ton_metricas", "pesca_desem_sardina_mil_ton_metricas", "pesca_desem_tiburon_mil_ton_metricas", "pesca_desem_ayanque_mil_ton_metricas", "pesca_desem_cabrilla_mil_ton_metricas", "pesca_desem_coco_mil_ton_metricas", "pesca_desem_lenguado_mil_ton_metricas", "pesca_desem_merluza_mil_ton_metricas", "pesca_desem_raya_mil_ton_metricas", "pesca_desem_tollo_mil_ton_metricas", "pesca_desem_cabinza_mil_ton_metricas", "pesca_desem_cojinova_mil_ton_metricas", "pesca_desem_corvina_mil_ton_metricas", "pesca_desem_chita_mil_ton_metricas", "pesca_desem_liza_mil_ton_metricas", "pesca_desem_lorna_mil_ton_metricas", "pesca_desem_machete_mil_ton_metricas", "pesca_desem_pejerrey_mil_ton_metricas", "pesca_desem_pintadilla_mil_ton_metricas", "pesca_desem_cangrejo_mil_ton_metricas", "pesca_desem_langosta_mil_ton_metricas", "pesca_desem_langostino_mil_ton_metricas", "pesca_desem_abalon_mil_ton_metricas", "pesca_desem_caracol_mil_ton_metricas", "pesca_desem_choro_mil_ton_metricas", "pesca_desem_concha_de_abanico_mil_ton_metricas", "pesca_desem_macha_mil_ton_metricas", "pesca_desem_almeja_mil_ton_metricas", "pesca_desem_calamar_mil_ton_metricas", "pesca_desem_pota_mil_ton_metricas", "pesca_desem_pulpo_mil_ton_metricas", "pesca_trans_mar_enlatado_mil_ton_metricas", "pesca_trans_mar_congelado_mil_ton_metricas", "pesca_trans_mar_curado_mil_ton_metricas", "pesca_trans_mar_harina_pescado_mil_ton_metricas", "pesca_trans_mar_aceite_crudo_pescado_mil_ton_metricas", "pesca_trans_con_congelado_mil_ton_metricas", "pesca_trans_con_curado_mil_ton_metricas", "pesca_venta_interna_con_direc_enlatado_mil_ton_metricas", "pesca_venta_interna_con_direc_congelado_mil_ton_metricas", "pesca_venta_interna_con_direc_curado_mil_ton_metricas", "pesca_venta_interna_con_direc_fresco_mil_ton_metricas", "pesca_venta_interna_con_indirec_harina_pescado_mil_ton_metricas", "pesca_venta_interna_con_indirec_aceite_crudo_pescado_mil_ton_metricas", "pesca_consumo_interno_direc_enlatado_mil_ton_metricas", "pesca_consumo_interno_direc_congelado_mil_ton_metricas", "pesca_consumo_interno_direc_curado_mil_ton_metricas", "pesca_consumo_interno_direc_fresco_mil_ton_metricas", "pesca_consumo_interno_direc_enlatado_per_cap_kg_hab", "pesca_consumo_interno_direc_congelado_per_cap_kg_hab", "pesca_consumo_interno_direc_curado_per_cap_kg_hab", "pesca_consumo_interno_direc_fresco_per_cap_kg_hab", "pesca_n_plantas_instaladas_enlatado", "pesca_capaci_inst_enlatado_u_cajas_turno", "pesca_n_plantas_instaladas_congelado", "pesca_capaci_inst_congelado_u_ton_dia", "pesca_n_plantas_instaladas_curado", "pesca_capaci_inst_curado_u_ton_mes", "pesca_n_plantas_instaladas_harina", "pesca_capaci_inst_harina_u_ton_hora", "turismo_entrada_turistas", "turismo_salida_turistas", "turismo_ingreso_divisas_millones_dolares", "turismo_egreso_divisas_millones_dolares", "turismo_ingreso_divisas_per_capita_dolares", "turismo_egreso_divisas_per_capita_dolares", "ing_tur_intern_aeropuerto_Jorge_Chavez", "ing_tur_intern_puesto_control_Santa_Rosa", "ing_tur_intern_otros_puntos", "turismo_arribo_turistas_nacionales", "turismo_arribo_turistas_extranjeros", "turismo_pernoctacion_turistas_nacionales", "turismo_pernoctacion_turistas_extranjeros", "turismo_permanencia_prom_turistas_nacionales", "turismo_permanencia_prom_turistas_extranjeros"])
        load_step = LoadStep("itp_indicators_y_n_nat", db_connector, if_exists="drop", pk=["ubigeo"], dtype=dtype, 
            nullable_list=["agricultura_ganaderia_caza_silvicultura_miles_soles", "pesca_miles_soles", "mineria_miles_soles", "industria_manufacturera_miles_soles",
            "electricidad_gas_agua_miles_soles", "construccion_miles_soles", "comercio_miles_soles", "hoteles_restaurantes_miles_soles",
            "transporte_almacenamiento_comunicaciones_miles_soles", "intermediacion_financiera_miles_soles", "actividad_inmobiliarias_empresariales_alquiler_miles_soles",
            "administracion_publica_defensa_miles_soles", "ensenianza_miles_soles", "servicios_sociales_salud_miles_soles",
            "otras_actividades_servicios_comunitarios_miles_soles", "hogares_privados_organizaciones_extraterritoriales_miles_soles",
            "creditos_hipotecariosvivienda_miles_soles", "creditos_consumo_miles_soles", "poblacion_total", "poblacion_censada", "poblacion_omitida",
            "perc_poblacion_con_1_nbi", "perc_poblacion_con_2_a_5_nbi", "perc_poblacion_nbi_vivienda_inadecuada", "perc_poblacion_nbi_vivienda_hacinada",
            "perc_poblacion_nbi_servicios_higienicos", "perc_poblacion_nbi_menores_sin_escuela", "perc_poblacion_nbi_alta_dependencia_economica",
            "gasto_social_educacion_inicial_mill_soles", "gasto_social_educacion_primaria_mill_soles", "gasto_social_educacion_secundaria_mill_soles",
            "gasto_social_asistencia_social_mill_soles", "gasto_social_salud_colectiva_mill_soles", "gasto_social_salud_individual_mill_soles", "analfabetismo_h_15_19",
            "analfabetismo_f_15_19", "analfabetismo_h_20_29", "analfabetismo_f_20_29", "analfabetismo_h_30_39", "analfabetismo_f_30_39", "analfabetismo_h_40_49",
            "analfabetismo_f_40_49", "analfabetismo_h_50_59", "analfabetismo_f_50_59", "analfabetismo_h_60_y_mas", "analfabetismo_f_60_y_mas",
            "millones_toneladas_co2_equivalente", "delitos_vida_cuerpo_salud", "delitos_honor", "delitos_familia", "delitos_libertad", "delitos_patrimonio",
            "delitos_confianza_buena_fe_negocios", "delitos_derechos_intelectuales", "delitos_patrimonio_cultural", "delitos_orden_economico",
            "delitos_orden_financiero_monetario", "delitos_tributarios", "delitos_seguridad_publica", "delitos_ambientales", "delitos_tranquilidad_publica",
            "delitos_humanidad", "delitos_estado_defensa_nacional", "delitos_poderes_estado_orden_const", "delitos_voluntad_popular", "delitos_administracion_publica",
            "delitos_fe_publica", "trib_adu_ingr_teso_pub_DAV_mill_soles", "trib_adu_ingr_teso_pub_D_especificos_mill_soles",
            "trib_adu_ingr_teso_pub_sobretasa_ad_5perc_mill_soles", "trib_adu_ingr_teso_pub_IGV_mill_soles", "trib_adu_ingr_teso_pub_ISC_mill_soles",
            "trib_adu_ingr_teso_pub_otros_mill_soles", "trib_adu_otros_org_gobiernos_loc_mill_soles", "trib_adu_otros_org_INDECOPI_mill_soles", "banca_multiple_creditos",
            "empresas_financieras_creditos", "cajas_municipales_creditos", "cajas_rur_ahorro_credito_creditos", "entidades_desa_peq_micr_empresa_EDPYME_creditos",
            "empresas_arrenda_financiero_creditos", "banco_nacion_creditos", "agrobanco_creditos", "banca_multiple_depositos", "empresas_financieras_depositos",
            "cajas_municipales_depositos", "cajas_rur_ahorro_credito_depositos", "entidades_desa_peq_micr_empresa_EDPYME_depositos", "empresas_arrenda_financiero_depositos",
            "banco_nacion_depositos", "agrobanco_depositos", "sector_pesquero_PIB_mill_soles_const_2007", "sector_pesquero_VAB_mill_soles_const_2007",
            "sector_pesquero_porc_VAB_d_PIB", "sector_pesquero_desem_mil_ton_met", "sector_pesquero_trans_mil_ton_met", "sector_pesquero_prod_harina_pescado_mil_ton_met",
            "sector_pesquero_consumo_interno_total_mil_ton_met", "sector_pesquero_consumo_interno_per_capita_kg", "sector_pesquero_mar_con_dir_enlatado_mil_ton_met",
            "sector_pesquero_mar_con_dir_congelado_mil_ton_met", "sector_pesquero_mar_con_dir_curado_mil_ton_met", "sector_pesquero_mar_con_dir_fresco_mil_ton_met",
            "sector_pesquero_mar_con_ind_anchoveta_mil_ton_met", "sector_pesquero_mar_con_ind_o_especies_mil_ton_met", "sector_pesquero_con_dir_curado_mil_ton_met",
            "sector_pesquero_con_dir_fresco_mil_ton_met", "sector_pesquero_con_dir_congelado_mil_ton_met", "pesca_desem_anchoveta_mil_ton_metricas",
            "pesca_desem_atun_mil_ton_metricas", "pesca_desem_bonito_mil_ton_metricas", "pesca_desem_caballa_mil_ton_metricas", "pesca_desem_jurel_mil_ton_metricas",
            "pesca_desem_perico_mil_ton_metricas", "pesca_desem_samasa_mil_ton_metricas", "pesca_desem_sardina_mil_ton_metricas", "pesca_desem_tiburon_mil_ton_metricas",
            "pesca_desem_ayanque_mil_ton_metricas", "pesca_desem_cabrilla_mil_ton_metricas", "pesca_desem_coco_mil_ton_metricas", "pesca_desem_lenguado_mil_ton_metricas",
            "pesca_desem_merluza_mil_ton_metricas", "pesca_desem_raya_mil_ton_metricas", "pesca_desem_tollo_mil_ton_metricas", "pesca_desem_cabinza_mil_ton_metricas",
            "pesca_desem_cojinova_mil_ton_metricas", "pesca_desem_corvina_mil_ton_metricas", "pesca_desem_chita_mil_ton_metricas", "pesca_desem_liza_mil_ton_metricas",
            "pesca_desem_lorna_mil_ton_metricas", "pesca_desem_machete_mil_ton_metricas", "pesca_desem_pejerrey_mil_ton_metricas", "pesca_desem_pintadilla_mil_ton_metricas",
            "pesca_desem_cangrejo_mil_ton_metricas", "pesca_desem_langosta_mil_ton_metricas", "pesca_desem_langostino_mil_ton_metricas",
            "pesca_desem_abalon_mil_ton_metricas", "pesca_desem_caracol_mil_ton_metricas", "pesca_desem_choro_mil_ton_metricas",
            "pesca_desem_concha_de_abanico_mil_ton_metricas", "pesca_desem_macha_mil_ton_metricas", "pesca_desem_almeja_mil_ton_metricas",
            "pesca_desem_calamar_mil_ton_metricas", "pesca_desem_pota_mil_ton_metricas", "pesca_desem_pulpo_mil_ton_metricas",  "pesca_trans_mar_enlatado_mil_ton_metricas",
            "pesca_trans_mar_congelado_mil_ton_metricas", "pesca_trans_mar_curado_mil_ton_metricas", "pesca_trans_mar_harina_pescado_mil_ton_metricas",
            "pesca_trans_mar_aceite_crudo_pescado_mil_ton_metricas", "pesca_trans_con_congelado_mil_ton_metricas", "pesca_trans_con_curado_mil_ton_metricas", 
            "pesca_venta_interna_con_direc_enlatado_mil_ton_metricas", "pesca_venta_interna_con_direc_congelado_mil_ton_metricas",
            "pesca_venta_interna_con_direc_curado_mil_ton_metricas", "pesca_venta_interna_con_direc_fresco_mil_ton_metricas",
            "pesca_venta_interna_con_indirec_harina_pescado_mil_ton_metricas", "pesca_venta_interna_con_indirec_aceite_crudo_pescado_mil_ton_metricas",
            "pesca_consumo_interno_direc_enlatado_mil_ton_metricas", "pesca_consumo_interno_direc_congelado_mil_ton_metricas",
            "pesca_consumo_interno_direc_curado_mil_ton_metricas", "pesca_consumo_interno_direc_fresco_mil_ton_metricas",
            "pesca_consumo_interno_direc_enlatado_per_cap_kg_hab", "pesca_consumo_interno_direc_congelado_per_cap_kg_hab",
            "pesca_consumo_interno_direc_curado_per_cap_kg_hab", "pesca_consumo_interno_direc_fresco_per_cap_kg_hab", "pesca_n_plantas_instaladas_enlatado",
            "pesca_capaci_inst_enlatado_u_cajas_turno", "pesca_n_plantas_instaladas_congelado", "pesca_capaci_inst_congelado_u_ton_dia",
            "pesca_n_plantas_instaladas_curado", "pesca_capaci_inst_curado_u_ton_mes", "pesca_n_plantas_instaladas_harina", "pesca_capaci_inst_harina_u_ton_hora",
            "turismo_entrada_turistas", "turismo_salida_turistas", "turismo_ingreso_divisas_millones_dolares", "turismo_egreso_divisas_millones_dolares",
            "turismo_ingreso_divisas_per_capita_dolares", "turismo_egreso_divisas_per_capita_dolares",
            "ing_tur_intern_aeropuerto_Jorge_Chavez", "ing_tur_intern_puesto_control_Santa_Rosa", "ing_tur_intern_otros_puntos",
            "turismo_arribo_turistas_nacionales", "turismo_arribo_turistas_extranjeros",
            "turismo_pernoctacion_turistas_nacionales", "turismo_pernoctacion_turistas_extranjeros",
            "turismo_permanencia_prom_turistas_nacionales", "turismo_permanencia_prom_turistas_extranjeros"])

        return [transform_step, agg_step, load_step]

def run_pipeline(params: dict):
    pp = itp_indicators_y_n_nat_pipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
