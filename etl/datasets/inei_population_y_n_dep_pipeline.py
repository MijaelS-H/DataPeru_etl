import pandas as pd
from bamboo_lib.helpers import grab_parent_dir
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
path = grab_parent_dir("../../") + "/datasets/20200318"

depto_dict = {"Amazonas": "01", "Áncash": "02", "Apurímac": "03", "Arequipa": "04", "Ayacucho": "05", "Cajamarca": "06", "Callao": "07", "Callao 1/": "07", "Prov. Const. del Callao": "07", "Prov. Const.Callao": "07", "Cusco": "08", "Huancavelica": "09", "Huánuco": "10", "Ica": "11", "Junín": "12", "La Libertad": "13", "La libertad": "13", "Lambayeque": "14", "Lima": "15", "Lima 1/": "15", "Lima y Callao": "15", "Proyecto CARAL (PEZAC) 2/": "15", "Loreto": "16", "Madre de Dios": "17", "Moquegua": "18", "Pasco": "19", "Pasco 1/": "19", "Piura": "20", "Puno": "21", "San Martín": "22", "Tacna": "23", "Tumbes": "24", "Ucayali": "25", "Ucayali 1/": "25" }
years_migration = [1940, 1961, 1972, 1981, 1993, 2007, 2017]
columns_ = [["Departamento", "Inmi-", "Emi-"], ["Departamento", "Inmi-.1", "Emi-.1"], ["Departamento", "Inmi-.2", "Emi-.2"], ["Departamento", "Inmi-.3", "Emi-.3"], ["Departamento", "Inmi-.4", "Emi-.4"], ["Departamento", "Inmi-.5", "Emi-.5"], ["Departamento", "Inmi-.6", "Emi-.6"]]

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        # Loading data from: population and housing
        df1 = pd.read_excel(io = "{}/{}/{}".format(path, "B. Población y Vivienda", "B.18.xlsx"), skiprows = (0,1))[3:30]
        df2 = pd.read_excel(io = "{}/{}/{}".format(path, "B. Población y Vivienda", "B.21.xlsx"), skiprows = (0,1))[3:30]
        df3 = pd.read_excel(io = "{}/{}/{}".format(path, "B. Población y Vivienda", "B.24.xls"), skiprows = (0,1,2,3), usecols = "A:U")[3:27]
        # Loading data from: employment
        df4 = pd.read_excel(io = "{}/{}/{}".format(path, "C. Empleo", "C.15.xlsx"), skiprows = (0,1,2,3))[14:41]
        # Loading data from: socials
        df5 = pd.read_excel(io = "{}/{}/{}".format(path, "D. Sociales", "D.4.xlsx"), skiprows = (0,1,2), usecols = "A:K")[5:32]
        df6 = pd.read_excel(io = "{}/{}/{}".format(path, "D. Sociales", "D.8.xlsx"), skiprows = (0,1,2,3))[11:38]
        df7 = pd.read_excel(io = "{}/{}/{}".format(path, "D. Sociales", "D.9.xlsx"), skiprows = (0,1,2,3))[11:38]
        df8 = pd.read_excel(io = "{}/{}/{}".format(path, "D. Sociales", "D.11.xlsx"), skiprows = (0,1,2,3), usecols = "A:K")[3:28]
        df9 = pd.read_excel(io = "{}/{}/{}".format(path, "D. Sociales", "D.12.xlsx"), skiprows = (0,1,2,3), usecols = "A,J:R")[3:28]
        df10 = pd.read_excel(io = "{}/{}/{}".format(path, "D. Sociales", "D.14.xlsx"), skiprows = (0,1), usecols = "A,E:N")[3:30]
        df11 = pd.read_excel(io = "{}/{}/{}".format(path, "D. Sociales", "D.15.xlsx"), skiprows = (0,1,2), usecols = "A,F:N")[3:30]
        df12 = pd.read_excel(io = "{}/{}/{}".format(path, "D. Sociales", "D.16.xlsx"), skiprows = (0,1,2,3), usecols = "A,G:J")[3:30]
        df13 = pd.read_excel(io = "{}/{}/{}".format(path, "D. Sociales", "D.18.xlsx"), skiprows = range(0,4), usecols = "A,K:V")[3:30]
        df14 = pd.read_excel(io = "{}/{}/{}".format(path, "D. Sociales", "D.55.xlsx"), skiprows = range(0,6), usecols = "A:L")[12:39]
        # Loading data from: environment
        df15 = pd.read_excel(io = "{}/{}/{}".format(path, "E. Medio Ambiente", "E.12.xlsx"), skiprows = range(0,6), usecols = "A,C,D,G,H")[9:33]
        df16 = pd.read_excel(io = "{}/{}/{}".format(path, "E. Medio Ambiente", "E.13.xlsx"), skiprows = (0,1,2,3), usecols = "A,C:F,I:L")[8:32]
        df17 = pd.read_excel(io = "{}/{}/{}".format(path, "E. Medio Ambiente", "E.22.xlsx"), skiprows = (0,1,2))[3:18]
        # Loading data from: information and communication technology
        df18 = pd.read_excel(io = "{}/{}/{}".format(path, "F. Tecnología de la Información y Comunicación", "F.1.xlsx"), skiprows = range(0,6))[17:44]
        df19 = pd.read_excel(io = "{}/{}/{}".format(path, "F. Tecnología de la Información y Comunicación", "F.2.xlsx"), skiprows = range(0,4))[17:44]
        df20 = pd.read_excel(io = "{}/{}/{}".format(path, "F. Tecnología de la Información y Comunicación", "F.3.xlsx"), skiprows = range(0,4))[16:43]
        df21 = pd.read_excel(io = "{}/{}/{}".format(path, "F. Tecnología de la Información y Comunicación", "F.4.xlsx"), skiprows = range(0,4))[17:44]
        df22 = pd.read_excel(io = "{}/{}/{}".format(path, "F. Tecnología de la Información y Comunicación", "F.5.xlsx"), skiprows = range(0,4))[17:44]
        df23 = pd.read_excel(io = "{}/{}/{}".format(path, "F. Tecnología de la Información y Comunicación", "F.6.xlsx"), skiprows = range(0,4))[17:44]
        df24 = pd.read_excel(io = "{}/{}/{}".format(path, "F. Tecnología de la Información y Comunicación", "F.7.xlsx"), skiprows = range(0,4))[17:44]
        # Loading data from: public safety
        df25 = pd.read_excel(io = "{}/{}/{}".format(path, "G. Seguridad Ciudadana", "G.2.xlsx"), skiprows = (0,1,2))[3:28]
        df26 = pd.read_excel(io = "{}/{}/{}".format(path, "G. Seguridad Ciudadana", "G.4.xlsx"), skiprows = (0,1,2), usecols = "A,C:I")[3:30]
        df27 = pd.read_excel(io = "{}/{}/{}".format(path, "G. Seguridad Ciudadana", "G.7.xlsx"), skiprows = (0,1,2))[3:28]
        df28 = pd.read_excel(io = "{}/{}/{}".format(path, "G. Seguridad Ciudadana", "G.8.xlsx"), skiprows = (0,1,2))[3:28]
        df29 = pd.read_excel(io = "{}/{}/{}".format(path, "G. Seguridad Ciudadana", "G.9.xlsx"), skiprows = (0,1,2,3,4), usecols = "A,D:G")[3:30]
        df30 = pd.read_excel(io = "{}/{}/{}".format(path, "G. Seguridad Ciudadana", "G.10.xlsx"), skiprows = (0,1,2))[4:29]
        df31 = pd.read_excel(io = "{}/{}/{}".format(path, "G. Seguridad Ciudadana", "G.11.xlsx"), skiprows = (0,1,2))[4:29]
        df32 = pd.read_excel(io = "{}/{}/{}".format(path, "G. Seguridad Ciudadana", "G.12.xlsx"), skiprows = (0,1,2,3,4))[3:28]

        # Added new datasets
        df33 = pd.read_excel(io = "{}/{}/{}".format(path, "D. Sociales", "D.19.xlsx"), skiprows = (0,1,2,3,4), usecols = "A,J:U")[4:31]
        df34 = pd.read_excel(io = "{}/{}/{}".format(path, "A. Economía", "A.152.xls"), sheet_name = "19.13 (a)", skiprows = range(0,34))[2:27]
        df35 = pd.read_excel(io = "{}/{}/{}".format(path, "A. Economía", "A.155.xls"), skiprows = (0,1,2,3,4))[1:23]

        # Special steps for some datasets: census survey migration data (20017-2017)
        df_3 = pd.DataFrame(columns = ["ubigeo", "inmigrantes", "emigrantes", "year"])
        for i in list(range(0,7)):
            pivote = df3[columns_[i]].copy()
            pivote.rename(columns = {columns_[i][0] : "ubigeo", columns_[i][1] : "inmigrantes", columns_[i][2]: "emigrantes"}, inplace = True)
            pivote["year"] = years_migration[i]
            pivote["ubigeo"].replace(depto_dict, inplace = True)
            df_3 = df_3.append(pivote)

        # Special steps for some datasets: agriculture area available by surfice type
        df_1 = df15[["Unnamed: 0", "Superficie\nagrícola", "Superficie\nno agrícola"]].copy()
        df_2 = df15[["Unnamed: 0", "Superficie\nagrícola.1", "Superficie\nno agrícola.1"]].copy()
        df_1.rename(columns = {"Unnamed: 0": "ubigeo", "Superficie\nagrícola": "superficie_agricola_hect", "Superficie\nno agrícola": "superficie_no_agricola_hect", "Superficie\nagrícola.1": "superficie_agricola_hect", "Superficie\nno agrícola.1": "superficie_no_agricola_hect"}, inplace = True)
        df_2.rename(columns = {"Unnamed: 0": "ubigeo", "Superficie\nagrícola": "superficie_agricola_hect", "Superficie\nno agrícola": "superficie_no_agricola_hect", "Superficie\nagrícola.1": "superficie_agricola_hect", "Superficie\nno agrícola.1": "superficie_no_agricola_hect"}, inplace = True)
        df_1["year"] = 2017
        df_2["year"] = 2018
        df_15 = df_1.append(df_2, sort = True)

        # Special steps for some datasets: agriculture area available by use
        df_1 = df16[["Unnamed: 0", "Agrícola \ncon \ncultivos", "Tierras en \nbarbecho", "Tierras \nagrícolas \nno trabajadas", "Tierras en \ndescanso"]].copy()
        df_2 = df16[["Unnamed: 0", "Agrícola \ncon \ncultivos.1", "Tierras en \nbarbecho.1", "Tierras \nagrícolas \nno trabajadas.1", "Tierras en \ndescanso.1"]].copy()
        df_1.rename(columns = {"Unnamed: 0": "ubigeo", "Agrícola \ncon \ncultivos": "sup_agr_cultivos_hect", "Tierras en \nbarbecho": "sup_agr_en_barbencho_hect", "Tierras \nagrícolas \nno trabajadas": "sup_agr_no_trabajadas_hect", "Tierras en \ndescanso": "sup_agr_en_descanso_hect", "Agrícola \ncon \ncultivos.1": "sup_agr_cultivos_hect", "Tierras en \nbarbecho.1": "sup_agr_en_barbencho_hect", "Tierras \nagrícolas \nno trabajadas.1": "sup_agr_no_trabajadas_hect", "Tierras en \ndescanso.1": "sup_agr_en_descanso_hect"}, inplace = True)
        df_2.rename(columns = {"Unnamed: 0": "ubigeo", "Agrícola \ncon \ncultivos": "sup_agr_cultivos_hect", "Tierras en \nbarbecho": "sup_agr_en_barbencho_hect", "Tierras \nagrícolas \nno trabajadas": "sup_agr_no_trabajadas_hect", "Tierras en \ndescanso": "sup_agr_en_descanso_hect", "Agrícola \ncon \ncultivos.1": "sup_agr_cultivos_hect", "Tierras en \nbarbecho.1": "sup_agr_en_barbencho_hect", "Tierras \nagrícolas \nno trabajadas.1": "sup_agr_no_trabajadas_hect", "Tierras en \ndescanso.1": "sup_agr_en_descanso_hect"}, inplace = True)
        df_1["year"] = 2017
        df_2["year"] = 2018
        df_16 = df_1.append(df_2, sort = True)

        # Special steps for some datasets: agriculture area available by use
        #df8.drop(["2012.1"], axis =1, inplace = True)

        # Special steps for some datasets: tourism data related to arrivals
        df34.dropna(axis = 1 , how = "any", inplace = True)
        df34.drop(['Unnamed: 1', 'Unnamed: 5', 2013, 2014, 2015, 2016, 2017, 2018], axis = 1, inplace = True)
        df_1_ = df34[['Departamento', 2011, 'Unnamed: 3']].copy()
        df_2_ = df34[['Departamento', 2012, 'Unnamed: 7']].copy()
        df_3_ = df34[['Departamento', 'Unnamed: 10', 'Unnamed: 11']].copy()
        df_4_ = df34[['Departamento', 'Unnamed: 14', 'Unnamed: 15']].copy()
        df_5_ = df34[['Departamento', 'Unnamed: 18', 'Unnamed: 19']].copy()
        df_6_ = df34[['Departamento', 'Unnamed: 22', 'Unnamed: 23']].copy()
        df_7_ = df34[['Departamento', 'Unnamed: 26', 'Unnamed: 27']].copy()
        df_8_ = df34[['Departamento', 'Unnamed: 30', 'Unnamed: 31']].copy()
        df_ = [df_1_, df_2_, df_3_, df_4_, df_5_, df_6_, df_7_, df_8_]
        df_34 = pd.DataFrame(columns = ["ubigeo", "huespedes_nacionales", "huespedes_extranjeros",  "year"])
        for i in range(0,8):
            df_[i].rename(columns = {df_[i].columns[0] : 'ubigeo', df_[i].columns[1] : 'huespedes_nacionales', df_[i].columns[2]: 'huespedes_extranjeros'}, inplace = True)
            df_[i]["year"] = 2011 + i
            df_34 = df_34.append(df_[i])

        # Special steps for some datasets: tourism data related to visits to archeologicals sites and museums
        df35.dropna(axis = 1 , how = "any", inplace = True)
        df35.drop(['Unnamed: 1','Unnamed: 4', 'Unnamed: 8', 'Unnamed: 12', 'Unnamed: 17', 'Unnamed: 21'], axis = 1, inplace = True)
        df_1_ = df35[['Unnamed: 0', 'Unnamed: 2', 'Unnamed: 3']].copy()
        df_2_ = df35[['Unnamed: 0', 'Unnamed: 5', 'Unnamed: 6',]].copy()
        df_3_ = df35[['Unnamed: 0', 'Unnamed: 9', 'Unnamed: 10']].copy()
        df_4_ = df35[['Unnamed: 0', 'Unnamed: 13', 'Unnamed: 14']].copy()
        df_5_ = df35[['Unnamed: 0', 'Unnamed: 18', 'Unnamed: 19']].copy()
        df_6_ = df35[['Unnamed: 0', 'Unnamed: 22', 'Unnamed: 23']].copy()
        df_ = [df_1_, df_2_, df_3_, df_4_, df_5_, df_6_]
        df35 = pd.DataFrame(columns = ["ubigeo", "visitantes_nacionales_museos_arqueologia", "visitantes_extranjeros_museos_arqueologia",  "year"])
        for i in range(0,6):
            df_[i].rename(columns = {df_[i].columns[0] : 'ubigeo', df_[i].columns[1] : 'visitantes_nacionales_museos_arqueologia', df_[i].columns[2]: 'visitantes_extranjeros_museos_arqueologia'}, inplace = True)
        df_1_["year"] = 2011
        df_2_["year"] = 2013
        df_3_["year"] = 2015
        df_4_["year"] = 2016
        df_5_["year"] = 2017
        df_6_["year"] = 2018
        df35 = df35.append(df_, sort=True)

        # Setting the datasets on the same rename/replace step to standard ubigeo for merge step
        for item in [df1, df2, df4, df5, df6, df7, df8, df9, df10, df11, df12, df13, df14, df_15, df_16, df17, df18, df19, df20, df21, df22, df23, df24, df25, df26, df27, df28, df29, df30, df31, df32, df33, df_34, df35]:
            item.rename(columns = {"Departamento de inscripción": "ubigeo", " Ámbito geográfico": "ubigeo", "Departamento": "ubigeo", "Ámbito geográfico": "ubigeo", "Departamento ": "ubigeo"}, inplace = True)
            item["ubigeo"] = item["ubigeo"].str.strip()
            item.drop(item.loc[item["ubigeo"].str.contains("Provincia|Regi")].index, inplace=True)
            item["ubigeo"].replace(depto_dict, inplace = True)

        # Group by step to add monumetns from site to department Lima
        df_35 = df35.groupby(["ubigeo", "year"]).sum().reset_index()

        # Melt step for 29 tables, df_3, df_15 and df_16 are already created
        df_1  = pd.melt(df1,  id_vars = ["ubigeo"], value_vars = [2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018], var_name = "year", value_name = "nacimientos")
        df_2  = pd.melt(df2,  id_vars = ["ubigeo"], value_vars = [2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018], var_name = "year", value_name = "defunciones")
        df_4  = pd.melt(df4,  id_vars = ["ubigeo"], value_vars = [2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018], var_name = "year", value_name = "peao_afiliada_pensiones")
        df_5  = pd.melt(df5,  id_vars = ["ubigeo"], value_vars = [2009,2010,2011,2012,2013,2014,2015,2016,2017,2018], var_name = "year", value_name = "nbi_1_o_mas_perc_pob")
        df_6  = pd.melt(df6,  id_vars = ["ubigeo"], value_vars = [2009,2010,2011,2012,2013,2014,2015,2016,2017,2018], var_name = "year", value_name = "mbpa_1_o_mas_members_perc_hog")
        df_7  = pd.melt(df7,  id_vars = ["ubigeo"], value_vars = [2009,2010,2011,2012,2013,2014,2015,2016,2017,2018], var_name = "year", value_name = "mbpa_1_o_mas_members_perc_hog_pob")
        df_8  = pd.melt(df8,  id_vars = ["ubigeo"], value_vars = [2010,2011,2012,2013,2014,2015,2016,2017,2018], var_name = "year", value_name = "n_medicos_colegiados")
        df_9  = pd.melt(df9,  id_vars = ["ubigeo"], value_vars = [2010,2011,2012,2013,2014,2015,2016,2017,2018], var_name = "year", value_name = "n_habitantes_por_medico")
        df_10 = pd.melt(df10, id_vars = ["ubigeo"], value_vars = [2010,2011,2012,2013,2014,2015,2016,2017], var_name = "year", value_name = "n_enfermeras_os_colegiados")
        df_11 = pd.melt(df11, id_vars = ["ubigeo"], value_vars = [2010,2011,2012,2013,2014,2015,2016,2017], var_name = "year", value_name = "n_habitantes_por_enfermera_os")
        df_12 = pd.melt(df12, id_vars = ["ubigeo"], value_vars = [2015,2016,2017,2018], var_name = "year", value_name = "desnutricion_5yrs_perc")
        df_13 = pd.melt(df13, id_vars = ["ubigeo"], value_vars = [2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018], var_name = "year", value_name = "enfermedades_ra_5yrs_perc")
        df_14 = pd.melt(df14, id_vars = ["ubigeo"], value_vars = [2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018], var_name = "year", value_name = "estudios_prom_15yrs")
        df_17 = pd.melt(df17, id_vars = ["ubigeo"], value_vars = [2010,2011,2012,2013,2014,2015,2016,2017], var_name = "year", value_name = "sup_bosque_humedo_amazonico_hect")
        df_18 = pd.melt(df18, id_vars = ["ubigeo"], value_vars = [2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018], var_name = "year", value_name = "hogares_tecn_informacion_perc")
        df_19 = pd.melt(df19, id_vars = ["ubigeo"], value_vars = [2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018], var_name = "year", value_name = "hogares_television_perc")
        df_20 = pd.melt(df20, id_vars = ["ubigeo"], value_vars = [2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018], var_name = "year", value_name = "hogares_cable_perc")
        df_21 = pd.melt(df21, id_vars = ["ubigeo"], value_vars = [2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018], var_name = "year", value_name = "hogares_telefono_fijo_perc")
        df_22 = pd.melt(df22, id_vars = ["ubigeo"], value_vars = [2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018], var_name = "year", value_name = "hogares_telefono_movil_perc")
        df_23 = pd.melt(df23, id_vars = ["ubigeo"], value_vars = [2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018], var_name = "year", value_name = "hogares_computadora_perc")
        df_24 = pd.melt(df24, id_vars = ["ubigeo"], value_vars = [2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018], var_name = "year", value_name = "hogares_internet_perc")
        df_25 = pd.melt(df25, id_vars = ["ubigeo"], value_vars = [2010,2011,2012,2013,2014,2015,2016,2017,2018], var_name = "year", value_name = "n_faltas_registradas")
        df_26 = pd.melt(df26, id_vars = ["ubigeo"], value_vars = [2012,2013,2014,2015,2016,2017,2018], var_name = "year", value_name = "n_comision_delitos")
        df_27 = pd.melt(df27, id_vars = ["ubigeo"], value_vars = [2009,2010,2011,2012,2013,2014,2015,2016,2017,2018], var_name = "year", value_name = "n_personas_detenidas_delitos")
        df_28 = pd.melt(df28, id_vars = ["ubigeo"], value_vars = [2009,2010,2011,2012,2013,2014,2015,2016,2017,2018], var_name = "year", value_name = "n_bandas_delictuales_desarticuladas")
        df_29 = pd.melt(df29, id_vars = ["ubigeo"], value_vars = [2015,2016,2017,2018], var_name = "year", value_name = "n_victimas_femicidios")
        df_30 = pd.melt(df30, id_vars = ["ubigeo"], value_vars = [2011,2012,2013,2014,2015,2016,2017,2018], var_name = "year", value_name = "n_denuncias_violencia_familiar_fisica")
        df_31 = pd.melt(df31, id_vars = ["ubigeo"], value_vars = [2011,2012,2013,2014,2015,2016,2017,2018], var_name = "year", value_name = "n_denuncias_violencia_familiar_sicolo")
        df_32 = pd.melt(df32, id_vars = ["ubigeo"], value_vars = [2011,2012,2013,2014,2015,2016,2017,2018], var_name = "year", value_name = "n_denuncias_robo_vehiculos")

        # Added datasets
        df_33 = pd.melt(df33, id_vars = ["ubigeo"], value_vars = [2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018], var_name = "year", value_name = "enfer_diarreicas_5yrs")

        # Correction to the scale to thousands of people
        df_4["peao_afiliada_pensiones"] = df_4["peao_afiliada_pensiones"] * 1000

        # Creating standard code to merge the 32 tables
        for item in [df_1, df_2, df_3, df_4, df_5, df_6, df_7, df_8, df_9, df_10, df_11, df_12, df_13, df_14, df_15, df_16, df_17, df_18, df_19, df_20, df_21, df_22, df_23, df_24, df_25, df_26, df_27, df_28, df_29, df_30, df_31, df_32, df_33, df_34, df_35]:
            item["code"] = item["ubigeo"].astype("str") + item["year"].astype("str")

        # Actually merging the datasets 
        df = pd.merge(df_1,  df_2[["code", "defunciones"]], on = "code", how = "left")
        df = pd.merge(df,  df_3[["code", "inmigrantes", "emigrantes"]], on = "code", how = "left")
        df = pd.merge(df,  df_4[["code", "peao_afiliada_pensiones"]], on = "code", how = "left")
        df = pd.merge(df,  df_5[["code", "nbi_1_o_mas_perc_pob"]], on = "code", how = "left")
        df = pd.merge(df,  df_6[["code", "mbpa_1_o_mas_members_perc_hog"]], on = "code", how = "left")
        df = pd.merge(df,  df_7[["code", "mbpa_1_o_mas_members_perc_hog_pob"]], on = "code", how = "left")
        df = pd.merge(df,  df_8[["code", "n_medicos_colegiados"]], on = "code", how = "left")
        df = pd.merge(df,  df_9[["code", "n_habitantes_por_medico"]], on = "code", how = "left")
        df = pd.merge(df,  df_10[["code", "n_enfermeras_os_colegiados"]], on = "code", how = "left")
        df = pd.merge(df,  df_11[["code", "n_habitantes_por_enfermera_os"]], on = "code", how = "left")
        df = pd.merge(df,  df_12[["code", "desnutricion_5yrs_perc"]], on = "code", how = "left")
        df = pd.merge(df,  df_13[["code", "enfermedades_ra_5yrs_perc"]], on = "code", how = "left")
        df = pd.merge(df,  df_14[["code", "estudios_prom_15yrs"]], on = "code", how = "left")
        df = pd.merge(df,  df_15[["code", "superficie_agricola_hect", "superficie_no_agricola_hect"]], on = "code", how = "left")
        df = pd.merge(df,  df_16[["code", "sup_agr_cultivos_hect", "sup_agr_en_barbencho_hect", "sup_agr_no_trabajadas_hect", "sup_agr_en_descanso_hect"]], on = "code", how = "left")
        df = pd.merge(df,  df_17[["code", "sup_bosque_humedo_amazonico_hect"]], on = "code", how = "left")
        df = pd.merge(df,  df_18[["code", "hogares_tecn_informacion_perc"]], on = "code", how = "left")
        df = pd.merge(df,  df_19[["code", "hogares_television_perc"]], on = "code", how = "left")
        df = pd.merge(df,  df_20[["code", "hogares_cable_perc"]], on = "code", how = "left")
        df = pd.merge(df,  df_21[["code", "hogares_telefono_fijo_perc"]], on = "code", how = "left")
        df = pd.merge(df,  df_22[["code", "hogares_telefono_movil_perc"]], on = "code", how = "left")
        df = pd.merge(df,  df_23[["code", "hogares_computadora_perc"]], on = "code", how = "left")
        df = pd.merge(df,  df_24[["code", "hogares_internet_perc"]], on = "code", how = "left")
        df = pd.merge(df,  df_25[["code", "n_faltas_registradas"]], on = "code", how = "left")
        df = pd.merge(df,  df_26[["code", "n_comision_delitos"]], on = "code", how = "left")
        df = pd.merge(df,  df_27[["code", "n_personas_detenidas_delitos"]], on = "code", how = "left")
        df = pd.merge(df,  df_28[["code", "n_bandas_delictuales_desarticuladas"]], on = "code", how = "left")
        df = pd.merge(df,  df_29[["code", "n_victimas_femicidios"]], on = "code", how = "left")
        df = pd.merge(df,  df_30[["code", "n_denuncias_violencia_familiar_fisica"]], on = "code", how = "left")
        df = pd.merge(df,  df_31[["code", "n_denuncias_violencia_familiar_sicolo"]], on = "code", how = "left")
        df = pd.merge(df,  df_32[["code", "n_denuncias_robo_vehiculos"]], on = "code", how = "left")

        # Added datasets
        df = pd.merge(df,  df_33[["code", "enfer_diarreicas_5yrs"]], on = "code", how = "left")
        df = pd.merge(df,  df_34[["code", "huespedes_nacionales", "huespedes_extranjeros"]], on = "code", how = "left")
        df = pd.merge(df,  df_35[["code", "visitantes_nacionales_museos_arqueologia", "visitantes_extranjeros_museos_arqueologia"]], on = "code", how = "left")

        # Dropping used code column from df
        df.drop(["code"], axis=1, inplace=True)

        # Removing strings from measure columns
        for item in ["desnutricion_5yrs_perc", "sup_agr_en_descanso_hect", "n_personas_detenidas_delitos", "n_victimas_femicidios", "n_denuncias_robo_vehiculos"]:
            df[item].replace({"-": pd.np.nan}, inplace = True)

        return df

class inei_population_y_n_dep(EasyPipeline):
  
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "ubigeo":                                              "String",
            "year":                                                "UInt16",
            "nacimientos":                                         "UInt32",
            "defunciones":                                         "UInt32",
            "inmigrantes":                                         "UInt32",
            "emigrantes":                                          "UInt32",
            "peao_afiliada_pensiones":                             "UInt32",
            "nbi_1_o_mas_perc_pob":                                "Float64",
            "mbpa_1_o_mas_members_perc_hog":                       "Float64",
            "mbpa_1_o_mas_members_perc_hog_pob":                   "Float64",
            "n_medicos_colegiados":                                "UInt16",
            "n_habitantes_por_medico":                             "Float64",
            "n_enfermeras_os_colegiados":                          "UInt16",
            "n_habitantes_por_enfermera_os":                       "Float64",
            "desnutricion_5yrs_perc":                              "Float64",
            "enfermedades_ra_5yrs_perc":                           "UInt32",
            "estudios_prom_15yrs":                                 "Float64",
            "superficie_agricola_hect":                            "Float64",
            "superficie_no_agricola_hect":                         "Float64",
            "sup_agr_cultivos_hect":                               "Float64",
            "sup_agr_en_barbencho_hect":                           "Float64",
            "sup_agr_no_trabajadas_hect":                          "Float64",
            "sup_agr_en_descanso_hect":                            "Float64",
            "sup_bosque_humedo_amazonico_hect":                    "Float64",
            "hogares_tecn_informacion_perc":                       "Float64",
            "hogares_television_perc":                             "Float64",
            "hogares_cable_perc":                                  "Float64",
            "hogares_telefono_fijo_perc":                          "Float64",
            "hogares_telefono_movil_perc":                         "Float64",
            "hogares_computadora_perc":                            "Float64",
            "hogares_internet_perc":                               "Float64",
            "n_faltas_registradas":                                "UInt32",
            "n_comision_delitos":                                  "UInt32",
            "n_personas_detenidas_delitos":                        "UInt32",
            "n_bandas_delictuales_desarticuladas":                 "UInt32",
            "n_victimas_femicidios":                               "UInt32",
            "n_denuncias_violencia_familiar_fisica":               "UInt32",
            "n_denuncias_violencia_familiar_sicolo":               "UInt32",
            "n_denuncias_robo_vehiculos":                          "UInt32",
            "enfer_diarreicas_5yrs":                               "UInt32",
            "huespedes_nacionales":                                "UInt32",
            "huespedes_extranjeros":                               "UInt32",
            "visitantes_nacionales_museos_arqueologia":            "UInt32",
            "visitantes_extranjeros_museos_arqueologia":           "UInt32"
            }

        transform_step = TransformStep()
        load_step = LoadStep(
            "inei_population_y_n_dep", db_connector, if_exists="drop", pk=["ubigeo"], dtype=dtype, 
            nullable_list = ["inmigrantes", "emigrantes", "nbi_1_o_mas_perc_pob", "mbpa_1_o_mas_members_perc_hog", "mbpa_1_o_mas_members_perc_hog_pob", "n_medicos_colegiados", "n_habitantes_por_medico", "n_enfermeras_os_colegiados", "n_habitantes_por_enfermera_os", "desnutricion_5yrs_perc", "estudios_prom_15yrs", "superficie_agricola_hect", "superficie_no_agricola_hect", "sup_agr_cultivos_hect", "sup_agr_en_barbencho_hect", "sup_agr_no_trabajadas_hect", "sup_agr_en_descanso_hect", "sup_bosque_humedo_amazonico_hect", "hogares_tecn_informacion_perc", "hogares_television_perc", "hogares_cable_perc", "hogares_telefono_fijo_perc", "hogares_telefono_movil_perc", "hogares_computadora_perc", "hogares_internet_perc", "n_faltas_registradas", "n_comision_delitos", "n_personas_detenidas_delitos", "n_bandas_delictuales_desarticuladas", "n_victimas_femicidios", "n_denuncias_violencia_familiar_fisica", "n_denuncias_violencia_familiar_sicolo", "n_denuncias_robo_vehiculos", "enfer_diarreicas_5yrs", "huespedes_nacionales", "huespedes_extranjeros", "visitantes_nacionales_museos_arqueologia", "visitantes_extranjeros_museos_arqueologia"]
        )

        return [transform_step, load_step]

if __name__ == "__main__":
    pp = inei_population_y_n_dep()
    pp.run({})