import glob
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        # Selected columns from dataset for available years, in modules -200, -300 & -400
        batch_200 = ["conglome", "vivienda", "hogar", "ubigeo", "dominio", "estrato", "codperso",
                    "p207", "p208a", "p209", "facpob07"]

        batch_300_18 = ["conglome", "vivienda", "hogar", "ubigeo", "dominio", "estrato", "codperso",
                        "p300a", "p301a", "p301a0", "p301a1", "p301b0", "p301b1", "p313", "p314a", 
                        "p314b_1", "p314b_2", "p314b_3", "p314b_4", "p314b_5", "p314b_6",
                        "p314b1_1", "p314b1_2", "p314b1_3", "p314b1_4", "p314b1_5", "p314b1_6", "p314b1_7",
                        "p314c", "p315a", 
                        "p316_1", "p316_2", "p316_3", "p316_4", "p316_5", "p316_6", "p316_7", "p316_8", 
                        "d311d_1", "d311d_2", "d311d_3", "d311d_4", "d311d_5", "d311d_6", "d311d_7", 
                        "d3121b", "d3122b", "d315a"]

        batch_300_15 = ["conglome", "vivienda", "hogar", "ubigeo", "dominio", "estrato", "codperso",
                        "p300a", "p301a", "p301a0", "p301a1", "p301b0", "p301b1", "p313", "p314a", 
                        "p314b_1", "p314b_2", "p314b_3", "p314b_4", "p314b_5", "p314b_6",

                        "p314c", "p315a", 
                        "p316_1", "p316_2", "p316_3", "p316_4", "p316_5", "p316_6", "p316_7", "p316_8", 
                        "d311d_1", "d311d_2", "d311d_3", "d311d_4", "d311d_5", "d311d_6", "d311d_7", 
                        "d3121b", "d3122b", "d315a"]

        batch_300_14 = ["conglome", "vivienda", "hogar", "ubigeo", "dominio", "estrato", "codperso",
                        "p300a", "p301a", "p301a0", "p301a1", "p301b0", "p301b1", "p313", "p314a", 
                        "p314b_1", "p314b_2", "p314b_3", "p314b_4", "p314b_5",

                        "p314c", "p315a", 
                        "p316_1", "p316_2", "p316_3", "p316_4", "p316_5", "p316_6", "p316_7",
                        "d311d_1", "d311d_2", "d311d_3", "d311d_4", "d311d_5", "d311d_6", "d311d_7", 
                        "d3121b", "d3122b", "d315a"]

        batch_400 = ["conglome", "vivienda", "hogar", "ubigeo", "dominio", "estrato", "codperso",
                    "p401c", "p4031", "p4032", "p4033", "p4034", "p4035", "p4036", "p4037",
                    "p4038", "p4039", "p40310", "p40311", "p40313", "p40314",
                    "p4091", "p4092", "p4093", "p4094", "p4095", "p4096",
                    "p4097", "p4098", "p4099", "p40910", "p40911",
                    "p417_02", "p417_08", "p417_11", "p417_12", "p417_13", "p417_14",
                    "p4191", "p4192", "p4193", "p4194", "p4195", "p4196", "p4197", "p4198",
                    "d41601", "d41602", "d41603", "d41604", "d41605", "d41606", "d41607", "d41608",
                    "d41609", "d41610", "d41611", "d41612", "d41613", "d41614", "d41615", "d41616"]

        # Selected merge columns from Modules -300 & -400
        merge_300 = ["_key", "p300a", "p301a", "p301a0", "p301a1", "p301b0",
                    "p301b1",
                    "p313", "p314a",
                    "p314b_1", "p314b_2", "p314b_3", "p314b_4", "p314b_5", "p314b_6",
                    "p314b1_1", "p314b1_2", "p314b1_3", "p314b1_4", "p314b1_5", "p314b1_6", "p314b1_7",
                    "p314c", "p315a",
                    "p316_1", "p316_2", "p316_3", "p316_4", "p316_5", "p316_6", "p316_7", "p316_8",
                    "d311d_1", "d311d_2", "d311d_3", "d311d_4", "d311d_5", "d311d_6", "d311d_7",
                    "d3121b", "d3122b", "d315a"]

        merge_400 = ["_key", "p401c",
                    "p4031", "p4032", "p4033", "p4034", "p4035", "p4036", "p4037",
                    "p4038", "p4039", "p40310", "p40311", "p40313", "p40314",
                    "p4091", "p4092", "p4093", "p4094", "p4095", "p4096",
                    "p4097", "p4098", "p4099", "p40910", "p40911",
                    "p417_02", "p417_08", "p417_11", "p417_12", "p417_13", "p417_14",
                    "p4191", "p4192", "p4193", "p4194", "p4195", "p4196", "p4197", "p4198",
                    "d41601", "d41602", "d41603", "d41604", "d41605", "d41606", "d41607", "d41608",
                    "d41609", "d41610", "d41611", "d41612", "d41613", "d41614", "d41615", "d41616"]

        # Loading dataframes stata step
        df_200 = pd.read_stata(params.get('url1'), columns = batch_200)
        df_400 = pd.read_stata(params.get('url3'), columns = batch_400)

        try:
            df_300 = pd.read_stata(params.get('url2'), convert_categoricals = False, columns = batch_300_18)
        except:
            try:
                df_300 = pd.read_stata(params.get('url2'), convert_categoricals = False, columns = batch_300_15)
            except:
                df_300 = pd.read_stata(params.get('url2'), convert_categoricals = False, columns = batch_300_14)

        # Adding missing columns from previous years 2014-2015
        missing_col = ["p314b_6", "p314b1_1", "p314b1_2", "p314b1_3", "p314b1_4", "p314b1_5", "p314b1_6", "p314b1_7", "p316_8"]
        for item in missing_col:
            if item not in df_300:
                df_300[item] = pd.np.nan

        # Creating unique key value to make merge
        df_200["_key"] = df_200["conglome"].astype(str).str.zfill(6) + df_200["vivienda"].astype(str).str.zfill(3) + df_200["hogar"].astype(str).str.zfill(3) + df_200["codperso"].astype(str).str.zfill(2)
        df_300["_key"] = df_300["conglome"].astype(str).str.zfill(6) + df_300["vivienda"].astype(str).str.zfill(3) + df_300["hogar"].astype(str).str.zfill(3) + df_300["codperso"].astype(str).str.zfill(2)
        df_400["_key"] = df_400["conglome"].astype(str).str.zfill(6) + df_400["vivienda"].astype(str).str.zfill(3) + df_400["hogar"].astype(str).str.zfill(3) + df_400["codperso"].astype(str).str.zfill(2)

        print("Version de la encuesta año", params.get('year'))
        # Merge step of the 3 datasets
        df__ = pd.merge(df_200, df_300[merge_300], on="_key", how="left") # Population + Education
        df = pd.merge(df__, df_400[merge_400], on="_key", how="left") # Population/Education + Healthcare

        # Cleaning column to id replacing step
        df["estrato"].replace({"." : ""}, inplace= True)
        df["estrato"] = df["estrato"].str.lstrip()

        # Deleting used columns
        df.drop(columns=["conglome", "vivienda", "hogar", "codperso", "_key"], inplace = True)

        # Excel spreadsheet for replacing steps
        df_labels = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTATtANn3dUv0nucHaGqKXl_hpMjnAHhVHXweIki65R2zS4z00IWOaasm0ZjBCZNwXT2C8ADUhOuyCr/pub?output=xlsx"
        df_cols_names = "https://docs.google.com/spreadsheets/d/e/2PACX-1vS556vrgVnk80uquHVKSHekXyb4Os6Mg5OItZV-oWo9i-YRSU4LvrnHLaV-43Mbyc7mS6tvSTJRL-_j/pub?output=xlsx"

        # Excel spreadsheet automatized replace step
        for i in df.columns:
            try:
                df_page = pd.read_excel(df_labels, i)
                df[i] = df[i].replace(dict(zip(df_page.col, df_page.id)))
            except:
                pass

        # Excel spreadsheet automatized rename columns step
        modules = ["Module -200", "Module -300", "Module -400"]
        for item in modules:
            df_page = pd.read_excel(df_cols_names, item)
            df = df.rename(columns = dict(zip(df_page.Col_id, df_page.Col_rename)))

        # Getting values of year for the survey
        df["year"] = int(params.get('year'))

        # Excel spreadsheet automatized replace step 
        # df["ubigeo"] = df["ubigeo"].astype(int)
        for i in [x for x in df.columns if x != "ubigeo"]:
            try:
                df[i] = df[i].astype(pd.Int8Dtype())
            except:
                pass
        print(df["gender_id"].unique())
        return df

class ENHPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Year", name="year", dtype=str),
            Parameter(label="Url1", name="url1", dtype=str),
            Parameter(label="Url2", name="url2", dtype=str),
            Parameter(label="Url3", name="url3", dtype=str)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "ubigeo":                                                          "String",
            "dominio":                                                         "UInt8",
            "estrato":                                                         "UInt8",
            "gender_id":                                                       "UInt8",
            "civil_status":                                                    "UInt8",
            "age_years":                                                       "UInt8",
            "childhood_learned_lenguage":                                      "UInt8",
            "actual_last_academic_degree":                                     "UInt8",
            "has_college_career":                                              "UInt8",
            "has_college_career_id":                                           "UInt32",
            "has_college_name":                                                "UInt8",
            "has_college_id":                                                  "UInt32",
            "main_reason_no_actual_studies":                                   "UInt8",
            "last_month_use_internet":                                         "UInt8",
            "use_internet_from_home":                                          "UInt8",
            "use_internet_from_work":                                          "UInt8",
            "use_internet_from_education_facility":                            "UInt8",
            "use_internet_from_public_cabin":                                  "UInt8",
            "use_internet_from_another_home":                                  "UInt8",
            "use_internet_from_other":                                         "UInt8",
            "access_internet_from_computer":                                   "UInt8",
            "access_internet_from_laptop":                                     "UInt8",
            "access_internet_from_own_cellphone":                              "UInt8",
            "access_internet_from_friends_cellphone":                          "UInt8",
            "access_internet_from_works_cellphone":                            "UInt8",
            "access_internet_from_tablet":                                     "UInt8",
            "access_internet_from_other_device":                               "UInt8",
            "last_month_internet_highest_used":                                "UInt8",
            "how_much_cost_internet":                                          "UInt16",
            "use_internet_for_information":                                    "UInt8",
            "use_internet_for_comunication":                                   "UInt8",
            "use_internet_for_buying":                                         "UInt8",
            "use_internet_for_financial_transactions":                         "UInt8",
            "use_internet_for_formal_education":                               "UInt8",
            "use_internet_for_organizations_transactions":                     "UInt8",
            "use_internet_for_entertainment":                                  "UInt8",
            "use_internet_for_sells":                                          "UInt8",
            "deflected_total_how_much_cost_uniform":                           "UInt16",
            "deflected_total_how_much_cost_shoes":                             "UInt16",
            "deflected_total_how_much_cost_text_books":                        "UInt16",
            "deflected_total_how_much_cost_school_supplies":                   "UInt16",
            "deflected_total_how_much_cost_enrollment":                        "UInt16",
            "deflected_total_how_much_cost_APAFA":                             "UInt16",
            "deflected_total_how_much_cost_others":                            "UInt16",
            "deflected_how_much_cost_mensual_teaching_pension":                "UInt16",
            "deflected_how_much_cost_particular_transportation":               "UInt16",
            "deflected_how_much_cost_internet":                                "UInt16",
            "has_DNI":                                                         "UInt8",
            "look_attention_MINSA_health_post":                                "UInt8",
            "look_attention_MINSA_clinic":                                     "UInt8",
            "look_attention_CLAS_clinic_health_post":                          "UInt8",
            "look_attention_ESSALUD_post_polyclinic":                          "UInt8",
            "look_attention_MINSA_hospital":                                   "UInt8",
            "look_attention_ESSALUD_ensurance_hospital":                       "UInt8",
            "look_attention_FFAA_Npolice_hospital":                            "UInt8",
            "look_attention_private_medical_office":                           "UInt8",
            "look_attention_private_clinic":                                   "UInt8",
            "look_attention_drug_store_pharmacy":                              "UInt8",
            "look_attention_own_home":                                         "UInt8",
            "look_attention_other":                                            "UInt8",
            "look_attention_does_not_look_attention":                          "UInt8",
            "does_not_look_attention_money":                                   "UInt8",
            "does_not_look_attention_far_away":                                "UInt8",
            "does_not_look_attention_takes_too_much_time":                     "UInt8",
            "does_not_look_attention_no_trust_doctors":                        "UInt8",
            "does_not_look_attention_no_necesary_grave":                       "UInt8",
            "does_not_look_attention_use_homemade_treatments":                 "UInt8",
            "does_not_look_attention_no_health_insuranse":                     "UInt8",
            "does_not_look_attention_selfprescription_repeat_recipe":          "UInt8",
            "does_not_look_attention_no_enough_time":                          "UInt8",
            "does_not_look_attention_health_personal_bad_treatment":           "UInt8",
            "does_not_look_attention_other":                                   "UInt8",
            "where_did_you_buy_medicine":                                      "UInt8",
            "where_did_you_buy_glasses":                                       "UInt8",
            "where_did_you_buy_contraceptives":                                "UInt8",
            "where_did_you_buy_others_as_termometer_orthopedics":              "UInt8",
            "where_did_you_buy_hospitalization":                               "UInt8",
            "where_did_you_buy_surgical_intervention":                         "UInt8",
            "healthcare_provider_ESSALUD_IPSS":                                "UInt8",
            "healthcare_provider_private_insurance":                           "UInt8",
            "healthcare_provider_entity_health_provider":                      "UInt8",
            "healthcare_provider_FFAA_police_ensurance":                       "UInt8",
            "healthcare_provider_integral_health_insurance":                   "UInt8",
            "healthcare_provider_universitary_insurance":                      "UInt8",
            "healthcare_provider_school_private_insurance":                    "UInt8",
            "healthcare_provider_other":                                       "UInt8",
            "expenses_home_in_deflected_medical_consultations":                "UInt16",
            "expenses_home_in_deflected_medicine":                             "UInt32",
            "expenses_home_in_deflected_analisis":                             "UInt8",
            "expenses_home_in_deflected_x_rays":                               "UInt8",
            "expenses_home_in_deflected_others_tests":                         "UInt8",
            "expenses_home_in_deflected_dental_services_similars":             "UInt8",
            "expenses_home_in_deflected_oftalmologic_services":                "UInt8",
            "expenses_home_in_deflected_buy_glasses":                          "UInt8",
            "expenses_home_in_deflected_vaccination":                          "UInt8",
            "expenses_home_in_deflected_children_health_control":              "UInt8",
            "expenses_home_in_deflected_contraceptives":                       "UInt8",
            "expenses_home_in_deflected_others_as_termometer_orthopedics":     "UInt8",
            "expenses_home_in_deflected_hospitalization":                      "UInt8",
            "expenses_home_in_deflected_surgical_intervention":                "UInt8",
            "expenses_home_in_deflected_pregnancy_checks":                     "UInt8",
            "expenses_home_in_deflected_delivery_care":                        "UInt8",
            "facpob07":                                                        "UInt8",
            "year":                                                            "UInt16"
        }

        transform_step = TransformStep()

        load_step = LoadStep(
            "enaho_population", db_connector, if_exists="append", pk=["ubigeo", "year"], dtype=dtype, 
            nullable_list=["gender_id", "age_years", "civil_status", "childhood_learned_lenguage", "actual_last_academic_degree", "has_college_career", "has_college_career_id",
                "has_college_name", "has_college_id", "main_reason_no_actual_studies", "last_month_use_internet", "use_internet_from_home",
                "use_internet_from_work", "use_internet_from_education_facility", "use_internet_from_public_cabin", "use_internet_from_another_home",
                "use_internet_from_other", "access_internet_from_computer", "access_internet_from_laptop",
                "access_internet_from_own_cellphone", "access_internet_from_friends_cellphone", "access_internet_from_works_cellphone",
                "access_internet_from_tablet", "access_internet_from_other_device", "last_month_internet_highest_used", "how_much_cost_internet",
                "use_internet_for_information", "use_internet_for_comunication", "use_internet_for_buying", "use_internet_for_financial_transactions",
                "use_internet_for_formal_education", "use_internet_for_organizations_transactions", "use_internet_for_entertainment",
                "use_internet_for_sells", "deflected_total_how_much_cost_uniform", "deflected_total_how_much_cost_shoes",
                "deflected_total_how_much_cost_text_books", "deflected_total_how_much_cost_school_supplies", "deflected_total_how_much_cost_enrollment",
                "deflected_total_how_much_cost_APAFA", "deflected_total_how_much_cost_others", "deflected_how_much_cost_mensual_teaching_pension",
                "deflected_how_much_cost_particular_transportation", "deflected_how_much_cost_internet", "has_DNI", "look_attention_MINSA_health_post",
                "look_attention_MINSA_clinic", "look_attention_CLAS_clinic_health_post", "look_attention_ESSALUD_post_polyclinic",
                "look_attention_MINSA_hospital", "look_attention_ESSALUD_ensurance_hospital", "look_attention_FFAA_Npolice_hospital",
                "look_attention_private_medical_office", "look_attention_private_clinic", "look_attention_drug_store_pharmacy",
                "look_attention_own_home", "look_attention_other", "look_attention_does_not_look_attention", 
                "does_not_look_attention_money", "does_not_look_attention_far_away", "does_not_look_attention_takes_too_much_time", 
                "does_not_look_attention_no_trust_doctors", "does_not_look_attention_no_necesary_grave", "does_not_look_attention_use_homemade_treatments",
                "does_not_look_attention_no_health_insuranse", "does_not_look_attention_selfprescription_repeat_recipe", "does_not_look_attention_no_enough_time",
                "does_not_look_attention_health_personal_bad_treatment", "does_not_look_attention_other",
                "where_did_you_buy_medicine", "where_did_you_buy_glasses", "where_did_you_buy_contraceptives",
                "where_did_you_buy_others_as_termometer_orthopedics", "where_did_you_buy_hospitalization", "where_did_you_buy_surgical_intervention", 
                "healthcare_provider_ESSALUD_IPSS", "healthcare_provider_private_insurance", "healthcare_provider_entity_health_provider",
                "healthcare_provider_FFAA_police_ensurance", "healthcare_provider_integral_health_insurance", "healthcare_provider_universitary_insurance",
                "healthcare_provider_school_private_insurance", "healthcare_provider_other", "expenses_home_in_deflected_medical_consultations",
                "expenses_home_in_deflected_medicine", "expenses_home_in_deflected_analisis", "expenses_home_in_deflected_x_rays", "expenses_home_in_deflected_others_tests",
                "expenses_home_in_deflected_dental_services_similars", "expenses_home_in_deflected_oftalmologic_services", "expenses_home_in_deflected_buy_glasses",
                "expenses_home_in_deflected_vaccination", "expenses_home_in_deflected_children_health_control", "expenses_home_in_deflected_contraceptives",
                "expenses_home_in_deflected_others_as_termometer_orthopedics", "expenses_home_in_deflected_hospitalization", "expenses_home_in_deflected_surgical_intervention",
                "expenses_home_in_deflected_pregnancy_checks", "expenses_home_in_deflected_delivery_care"
              ]
        )

        return [transform_step, load_step]

if __name__ == "__main__":
    
    data = glob.glob('../../data/enh/*.dta')

    pp = ENHPipeline()
    for year in range(2014, 2018 + 1):
    #for year in range(2018, 2018 + 1):
        pp.run({
            'url1': '../../data/enh/enaho01-{}-200.dta'.format(year),
            'url2': '../../data/enh/enaho01a-{}-300.dta'.format(year),
            'url3': '../../data/enh/enaho01a-{}-400.dta'.format(year),
            'year': year
        })