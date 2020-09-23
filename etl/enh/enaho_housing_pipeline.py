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

        # Selected columns from dataset for available years, in module -100 & sumaria
        batch_100 = [
            "conglome", "vivienda", "hogar", "ubigeo", "dominio", "estrato",
            "p101", "p102", "p103", "p103a", "p105a", "p110", "p110a1", "p111a", "d105b",
            "d107d1", "d107d2", "d107d3", "d107d4",
            "d1172_01", "d1172_02", "d1172_04", "d1172_05", "d1172_06", "d1172_07", "d1172_08",
            "d1172_09", "d1172_10", "d1172_11", "d1172_12", "d1172_13", "d1172_14", "d1172_15", #"d1172_16",
            "nbi1", "nbi2", "nbi3", "nbi4", "nbi5", "factor07"
        ]

        batch_sumaria = [
            "conglome", "vivienda", "hogar",
            "percepho", "mieperho", "totmieho",
            "ia01hd", "ia02hd",
            "ingbruhd", "ingnethd", "insedthd", "insedlhd",
            "sg23", "sg25", "ga03hd", "sg42", "sg421", "sg422", "sg423",
            "gru11hd", "gru21hd", "gru31hd", "gru41hd", "gru51hd", "gru61hd", "gru71hd", "gru81hd",
            "gashog1d", "ld", "linpe", "linea", "pobreza", "estrsocial"
        ]

        merge_sumaria = [
            "_key", "percepho", "mieperho", "totmieho",
            "ia01hd", "ia02hd",
            "ingbruhd", "ingnethd", "insedthd", "insedlhd",
            "sg23", "sg25", "ga03hd", "sg42", "sg421", "sg422", "sg423",
            "gru11hd", "gru21hd", "gru31hd", "gru41hd", "gru51hd", "gru61hd", "gru71hd", "gru81hd",
            "gashog1d", "ld", "linpe", "linea", "pobreza", "estrsocial"
        ]

        # Loading dataframes stata step
        df_100 = pd.read_stata(params.get('url1'), columns = batch_100)
        df_sum = pd.read_stata(params.get('url2'), columns = batch_sumaria)

        # Creating unique key value to make merge
        df_100["_key"] = df_100["conglome"].astype(str).str.zfill(6) + df_100["vivienda"].astype(str).str.zfill(3) + df_100["hogar"].astype(str).str.zfill(3)
        df_sum["_key"] = df_sum["conglome"].astype(str).str.zfill(6) + df_sum["vivienda"].astype(str).str.zfill(3) + df_sum["hogar"].astype(str).str.zfill(3)

        print("Version de la encuesta año", params.get('year'))
        # Merge step
        df = pd.merge(df_100, df_sum[merge_sumaria], on="_key", how="left")

        # Deleting used columns
        df.drop(columns=["conglome", "vivienda", "hogar", "_key"], inplace = True)

        # Cleaning column to id replacing step
        df["estrato"].replace({"." : ""}, inplace= True)
        df["estrato"] = df["estrato"].str.lstrip()

        # Excel spreadsheet for replacing steps
        df_labels = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQJrA-7Hctfv0VmbY8B0UoPNseTRBZ3DWSsHDFhFVlC2w-Efz_8RpxooAxcNLIxK5djVMy3rCAyQOuD/pub?output=xlsx"
        df_cols_names = "https://docs.google.com/spreadsheets/d/e/2PACX-1vS556vrgVnk80uquHVKSHekXyb4Os6Mg5OItZV-oWo9i-YRSU4LvrnHLaV-43Mbyc7mS6tvSTJRL-_j/pub?output=xlsx"

        # Excel spreadsheet automatized replace id step
        for i in df.columns:
            try:
                df_page = pd.read_excel(df_labels, i)
                df[i] = df[i].replace(dict(zip(df_page.col, df_page.id)))
            except:
                pass

        # Excel spreadsheet automatized rename columns step
        df_page = pd.read_excel(df_cols_names, "Module -100")
        df = df.rename(columns = dict(zip(df_page.Col_id, df_page.Col_rename)))

        df_page = pd.read_excel(df_cols_names, "Module -sumaria")
        df = df.rename(columns = dict(zip(df_page.Col_id, df_page.Col_rename)))

        # Getting values of year for the survey
        df["year"] = int(params.get('year'))

        # Excel spreadsheet automatized replace step 
        for i in df.columns:
            try:
                df[i] = df[i].astype(pd.Int8Dtype())
            except:
                pass

        return df

class ENHPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Year", name="year", dtype=str),
            Parameter(label="Url1", name="url1", dtype=str),
            Parameter(label="Url2", name="url2", dtype=str)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "ubigeo":                                             "String",
            "dominio":                                            "UInt8",
            "estrato":                                            "UInt8",
            "type_household":                                     "UInt8",
            "walls_material":                                     "UInt8",
            "floor_material":                                     "UInt8",
            "ceiling_material":                                   "UInt8",
            "type_home":                                          "UInt8",
            "water_source":                                       "UInt8",
            "is_water_drinkable":                                 "UInt8",
            "sewer_conection":                                    "UInt8",
            "monthly_rent_household":                             "UInt32",
            "credit_buying_house_apartment":                      "UInt32",
            "credit_ground_house":                                "UInt32",
            "credit_house_improvements":                          "UInt32",
            "credit_build_new_house":                             "UInt32",
            "last_month_total_paid_water":                        "UInt16",
            "last_month_total_paid_electricity":                  "UInt16",
            "last_month_total_paid_gas_glp":                      "UInt16",
            "last_month_total_paid_natural_gas":                  "UInt16",
            "last_month_total_paid_candle":                       "UInt16",
            "last_month_total_paid_coal":                         "UInt16",
            "last_month_total_paid_wood":                         "UInt16",
            "last_month_total_paid_petroleum":                    "UInt16",
            "last_month_total_paid_gasoline":                     "UInt16",
            "last_month_total_paid_landline":                     "UInt16",
            "last_month_total_paid_cellphone":                    "UInt16",
            "last_month_total_paid_tv_cable":                     "UInt16",
            "last_month_total_paid_internet":                     "UInt16",
            "last_month_total_paid_other":                        "UInt16",
            "basic_needs_inadequate_house":                       "UInt8",
            "basic_needs_overcrowd_house":                        "UInt8",
            "basic_needs_no_higienic_services":                   "UInt8",
            "basic_needs_kids_without_school":                    "UInt8",
            "basic_needs_high_economic_dependency":               "UInt8",
            "n_members_with_income":                              "UInt16",
            "n_members_household":                                "UInt32",
            "n_people_household":                                 "UInt32",
            "income_from_rent_household":                         "UInt32",
            "income_from_rent_transfer":                          "UInt32",
            "gross_income_dependent_main_activity":               "UInt32",
            "net_income_dependent_main_activity":                 "UInt32",
            "gross_income_dependent_second_activity":             "UInt32",
            "net_income_dependent_second_activity":               "UInt32",
            "expenses_feeding_in_home":                           "UInt32",
            "expenses_feeding_in_home_less_14_yo":                "UInt32",
            "expenses_by_rent":                                   "UInt32",
            "home_expenses_boughts_tv_computer_sound":            "UInt32",
            "home_expenses_boughts_home_appliances":              "UInt32",
            "home_expenses_boughts_transport":                    "UInt32",
            "home_expenses_boughts_others":                       "UInt32",
            "group_1_food_expenses":                              "UInt32",
            "group_2_wardrove_expenses":                          "UInt32",
            "group_3_rent_fuel_elect_expenses":                   "UInt32",
            "group_4_furniture_fixtures_expenses":                "UInt32",
            "group_5_health_personal_care_expenses":              "UInt32",
            "group_6_transportation_comunication_expenses":       "UInt32",
            "group_7_fun_recreation_expenses":                    "UInt32",
            "group_8_others_goods_services_expenses":             "UInt32",
            "total_monetary_expenses":                            "UInt32",
            "spatial_deflactor":                                  "UInt32",
            "poverty_line_feeding":                               "UInt32",
            "poverty_line_total":                                 "UInt32",
            "poverty_type":                                       "UInt32",
            "socioeconomic_stratum":                              "UInt8",
            "factor07":                                           "UInt8",
            "year":                                               "UInt16"
        }

        transform_step = TransformStep()

        load_step = LoadStep(
            "enaho_housing", db_connector, if_exists="append", pk=["ubigeo", "year"], dtype=dtype, 
            nullable_list=[
                "type_household", "walls_material", "floor_material", "ceiling_material", "type_home", 
                "water_source", "is_water_drinkable", "sewer_conection", "monthly_rent_household", 
                "credit_buying_house_apartment", "credit_ground_house", "credit_house_improvements", "credit_build_new_house", 
                "last_month_total_paid_water", "last_month_total_paid_electricity", "last_month_total_paid_gas_glp", 
                "last_month_total_paid_natural_gas", "last_month_total_paid_candle", "last_month_total_paid_coal",
                "last_month_total_paid_wood", "last_month_total_paid_petroleum", "last_month_total_paid_gasoline",
                "last_month_total_paid_landline", "last_month_total_paid_cellphone", "last_month_total_paid_tv_cable",
                "last_month_total_paid_internet", "last_month_total_paid_other", 
                "basic_needs_inadequate_house", "basic_needs_overcrowd_house", "basic_needs_no_higienic_services",
                "basic_needs_kids_without_school", "basic_needs_high_economic_dependency", 
                "n_members_with_income", "n_members_household", "n_people_household", 
                "income_from_rent_household", "income_from_rent_transfer", 
                "gross_income_dependent_main_activity", "net_income_dependent_main_activity", 
                "gross_income_dependent_second_activity", "net_income_dependent_second_activity", 
                "expenses_feeding_in_home", "expenses_feeding_in_home_less_14_yo", "expenses_by_rent", 
                "home_expenses_boughts_tv_computer_sound", "home_expenses_boughts_home_appliances",
                "home_expenses_boughts_transport", "home_expenses_boughts_others", 
                "group_1_food_expenses", "group_2_wardrove_expenses", "group_3_rent_fuel_elect_expenses",
                "group_4_furniture_fixtures_expenses", "group_5_health_personal_care_expenses", 
                "group_6_transportation_comunication_expenses", "group_7_fun_recreation_expenses", "group_8_others_goods_services_expenses",
                "total_monetary_expenses", 
                "spatial_deflactor", "poverty_line_feeding", "poverty_line_total", "poverty_type", "socioeconomic_stratum"
              ]
        )

        return [transform_step, load_step]

if __name__ == "__main__":
    
    data = glob.glob('../../data/enh/*.dta')

    pp = ENHPipeline()
    for year in range(2014, 2018 + 1):
    #for year in range(2018, 2018 + 1):
        pp.run({
            'url1': '../../data/enh/enaho01-{}-100.dta'.format(year),
            'url2': '../../data/enh/sumaria-{}.dta'.format(year),
            'year': year
        })