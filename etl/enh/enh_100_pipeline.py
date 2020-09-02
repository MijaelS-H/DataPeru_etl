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

        # Selected columns from dataset for available years
        batch_2018 = [
                'hogar', 'ubigeo', 'dominio','estrato', 'p101', 'p102',  'p103', 'p103a', 'p105a', 'p106a', 'p106b', 'p110', 'p110a1', 'p110c',
                'p110f', 'p110g', 'p111a', 'p1121', 'p1123', 'p1124', 'p1125', 'p1126', 'p1127', 'p112a', 'p1131', 'p1132',
                'p1133', 'p1135', 'p1136', 'p1137', 'p1138', 'p113a', 'p1141', 'p1142', 'p1143', 'p1144', 'p1145', 'p1171_01',
                'p1171_02', 'p1171_04', 'p1171_05', 'p1171_06', 'p1171_07', 'p1171_08', 'p1171_09', 'p1171_10',
                'p1171_11', 'p1171_12', 'p1171_13', 'p1171_14', 'p1171_15', 'p1171_16',
                'p1172_01', 'p1172_02', 'p1172_04', 'p1172_05', 'p1172_06', 'p1172_07', 'p1172_08', 'p1172_09', 'p1172_10',
                'p1172_11', 'p1172_12', 'p1172_13', 'p1172_14', 'p1172_15', 'p1172_16',
                'p1173_01', 'p1173_02', 'p1173_04', 'p1173_05', 'p1173_06', 'p1173_07', 'p1173_08', 'p1173_09', 'p1173_10',
                'p1173_11', 'p1173_12', 'p1173_13', 'p1173_14', 'p1173_15', 'p1173_16',
                'p1174_01', 'p1174_02', 'p1174_04', 'p1174_05', 'p1174_06', 'p1174_07', 'p1174_08', 'p1174_09', 'p1174_10',
                'p1174_11', 'p1174_12', 'p1174_13', 'p1174_14', 'p1174_15', 'p1174_16',
                'p1175_01', 'p1175_02', 'p1175_04', 'p1175_05', 'p1175_06', 'p1175_07', 'p1175_08', 'p1175_09', 'p1175_10',
                'p1175_11', 'p1175_12', 'p1175_13', 'p1175_14', 'p1175_15', 'p1175_16', 'p117t3', 'p117t4',
                'd105b', 'd106', 'd107d1', 'd107d2', 'd107d3', 'd107d4',
                'd1172_01', 'd1172_02', 'd1172_04', 'd1172_05', 'd1172_06', 'd1172_07', 'd1172_08', 'd1172_09', 'd1172_10',
                'd1172_11', 'd1172_12', 'd1172_13', 'd1172_14', 'd1172_15', 'd1172_16', 
                'd1173_01', 'd1173_02', 'd1173_04', 'd1173_05', 'd1173_06', 'd1173_07', 'd1173_08', 'd1173_09', 'd1173_10',
                'd1173_11', 'd1173_12', 'd1173_13', 'd1173_14', 'd1173_15', 'd1173_16',
                'd1174_01', 'd1174_02', 'd1174_04', 'd1174_05', 'd1174_06', 'd1174_07', 'd1174_08', 'd1174_09', 'd1174_10',
                'd1174_11', 'd1174_12', 'd1174_13', 'd1174_14', 'd1174_15', 'd1174_16',
                'nbi1', 'nbi2', 'nbi3', 'nbi4', 'nbi5', 'factor07']

        batch_2017 = [
                'hogar', 'ubigeo', 'dominio','estrato', 'p101', 'p102',  'p103', 'p103a', 'p105a', 'p106a', 'p106b', 'p110', 'p110a1', 'p110c',
                'p111a', 'p1121', 'p1123', 'p1124', 'p1125', 'p1126', 'p1127', 'p112a', 'p1131', 'p1132', 
                'p1133', 'p1135', 'p1136', 'p1137', 'p1138', 'p113a', 'p1141', 'p1142', 'p1143', 'p1144', 'p1145', 'p1171_01',
                'p1171_02', 'p1171_04', 'p1171_05', 'p1171_06', 'p1171_07', 'p1171_08', 'p1171_09', 'p1171_10',
                'p1171_11', 'p1171_12', 'p1171_13', 'p1171_14', 'p1171_15',
                'p1172_01', 'p1172_02', 'p1172_04', 'p1172_05', 'p1172_06', 'p1172_07', 'p1172_08', 'p1172_09', 'p1172_10',
                'p1172_11', 'p1172_12', 'p1172_13', 'p1172_14', 'p1172_15',
                'p1173_01', 'p1173_02', 'p1173_04', 'p1173_05', 'p1173_06', 'p1173_07', 'p1173_08', 'p1173_09', 'p1173_10',
                'p1173_11', 'p1173_12', 'p1173_13', 'p1173_14', 'p1173_15',
                'p1174_01', 'p1174_02', 'p1174_04', 'p1174_05', 'p1174_06', 'p1174_07', 'p1174_08', 'p1174_09', 'p1174_10',
                'p1174_11', 'p1174_12', 'p1174_13', 'p1174_14', 'p1174_15',
                'p1175_01', 'p1175_02', 'p1175_04', 'p1175_05', 'p1175_06', 'p1175_07', 'p1175_08', 'p1175_09', 'p1175_10',
                'p1175_11', 'p1175_12', 'p1175_13', 'p1175_14', 'p1175_15', 'p117t3', 'p117t4',
                'd105b', 'd106', 'd107d1', 'd107d2', 'd107d3', 'd107d4',
                'd1172_01', 'd1172_02', 'd1172_04', 'd1172_05', 'd1172_06', 'd1172_07', 'd1172_08', 'd1172_09', 'd1172_10',
                'd1172_11', 'd1172_12', 'd1172_13', 'd1172_14', 'd1172_15',
                'd1173_01', 'd1173_02', 'd1173_04', 'd1173_05', 'd1173_06', 'd1173_07', 'd1173_08', 'd1173_09', 'd1173_10',
                'd1173_11', 'd1173_12', 'd1173_13', 'd1173_14', 'd1173_15',
                'd1174_01', 'd1174_02', 'd1174_04', 'd1174_05', 'd1174_06', 'd1174_07', 'd1174_08', 'd1174_09', 'd1174_10',
                'd1174_11', 'd1174_12', 'd1174_13', 'd1174_14', 'd1174_15',
                'nbi1', 'nbi2', 'nbi3', 'nbi4', 'nbi5', 'factor07']

        batch_2016 = [
                'hogar', 'ubigeo', 'dominio','estrato', 'p101', 'p102',  'p103', 'p103a', 'p105a', 'p106a', 'p106b', 'p110', 'p110a1',
                'p111a', 'p1121', 'p1123', 'p1124', 'p1125', 'p1126', 'p1127', 'p112a', 'p1131', 'p1132',
                'p1133', 'p1135', 'p1136', 'p1137', 'p1138', 'p113a', 'p1141', 'p1142', 'p1143', 'p1144', 'p1145', 'p1171_01',
                'p1171_02', 'p1171_04', 'p1171_05', 'p1171_06', 'p1171_07', 'p1171_08', 'p1171_09', 'p1171_10',
                'p1171_11', 'p1171_12', 'p1171_13', 'p1171_14', 'p1171_15',
                'p1172_01', 'p1172_02', 'p1172_04', 'p1172_05', 'p1172_06', 'p1172_07', 'p1172_08', 'p1172_09', 'p1172_10',
                'p1172_11', 'p1172_12', 'p1172_13', 'p1172_14', 'p1172_15',
                'p1173_01', 'p1173_02', 'p1173_04', 'p1173_05', 'p1173_06', 'p1173_07', 'p1173_08', 'p1173_09', 'p1173_10',
                'p1173_11', 'p1173_12', 'p1173_13', 'p1173_14', 'p1173_15',
                'p1174_01', 'p1174_02', 'p1174_04', 'p1174_05', 'p1174_06', 'p1174_07', 'p1174_08', 'p1174_09', 'p1174_10',
                'p1174_11', 'p1174_12', 'p1174_13', 'p1174_14', 'p1174_15',
                'p1175_01', 'p1175_02', 'p1175_04', 'p1175_05', 'p1175_06', 'p1175_07', 'p1175_08', 'p1175_09', 'p1175_10',
                'p1175_11', 'p1175_12', 'p1175_13', 'p1175_14', 'p1175_15', 'p117t3', 'p117t4',
                'd105b', 'd106', 'd107d1', 'd107d2', 'd107d3', 'd107d4',
                'd1172_01', 'd1172_02', 'd1172_04', 'd1172_05', 'd1172_06', 'd1172_07', 'd1172_08', 'd1172_09', 'd1172_10',
                'd1172_11', 'd1172_12', 'd1172_13', 'd1172_14', 'd1172_15',
                'd1173_01', 'd1173_02', 'd1173_04', 'd1173_05', 'd1173_06', 'd1173_07', 'd1173_08', 'd1173_09', 'd1173_10',
                'd1173_11', 'd1173_12', 'd1173_13', 'd1173_14', 'd1173_15',
                'd1174_01', 'd1174_02', 'd1174_04', 'd1174_05', 'd1174_06', 'd1174_07', 'd1174_08', 'd1174_09', 'd1174_10',
                'd1174_11', 'd1174_12', 'd1174_13', 'd1174_14', 'd1174_15',
                'nbi1', 'nbi2', 'nbi3', 'nbi4', 'nbi5', 'factor07']

        # Loading dataframe stata step
        try: 
            df = pd.read_stata(params.get('url'), columns = batch_2018)
        except:
            try:
                df = pd.read_stata(params.get('url'), columns = batch_2017)
            except: 
                df = pd.read_stata(params.get('url'), columns = batch_2016)

        # Excel spreadsheet for replace text to id step
        df_labels = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQJrA-7Hctfv0VmbY8B0UoPNseTRBZ3DWSsHDFhFVlC2w-Efz_8RpxooAxcNLIxK5djVMy3rCAyQOuD/pub?output=xlsx"

        # Getting values of year for the survey
        df["year"] = int(params.get('year'))

        # Correction step to certain years dataset _100
        df["p113a"].replace({9.0 :  9}, inplace= True)
        df["estrato"].replace({"." : ""}, inplace= True)
        df["estrato"] = df["estrato"].str.lstrip()

        # Adding missing columns between years dataset
        missing_col = ["p110c", "d1172_16", "p110f", "d1173_16", "p1171_16", "p1173_16", "p1174_16","p110g", "d1174_16", "p1175_16", "p1172_16"]
        for item in missing_col:
            if item not in df:
                df[item] = pd.np.nan

        # Excel spreadsheet automatized replace step
        for i in df.columns:
            try:
                df_page = pd.read_excel(df_labels, i)
                df[i] = df[i].replace(dict(zip(df_page.col, df_page.id)))
            except:
                pass

        # Renaming columns to an understandable name
        df = df.rename(columns={
            "p101" : "type_household", "p102": "walls_material", "p103": "floor_material",
            "p103a": "ceiling_material", "p105a": "type_home",
            "p106a": "household_property_title", "p106b": "household_property_title_SUNARP",
            "p110": "water_source", "p110a1": "is_water_drinkable", "p110c": "water_all_week",
            "p110f": "water_payment", "p110g": "water_source_company", "p111a": "sewer_conection",
            "p1121": "light_electricity", "p1123": "light_petroleum_gas", "p1124": "light_candle",
            "p1125": "light_generator", "p1126": "light_other", "p1127": "light_no_use_home",
            "p112a": "electric_meter_type",
            "p1131": "cooking_fuel_electricity", "p1132": "cooking_fuel_gas_glp", "p1133": "cooking_fuel_natural_gas",
            "p1135": "cooking_fuel_coal", "p1136": "cooking_fuel_kerosene", "p1137": "cooking_fuel_other", "p1138": "cooking_fuel_no_cook",
            "p113a": "cooking_fuel_main", "p1141": "has_landline", "p1142": "has_cellphone", "p1143": "has_tv_cable_satelite",
            "p1144": "has_internet", "p1145": "has_not",

            # Binary
            "p1171_01": "last_month_expenses_water", "p1171_02": "last_month_expenses_electricity",
            "p1171_04": "last_month_expenses_gas_glp", "p1171_05": "last_month_expenses_natural_gas",
            "p1171_06": "last_month_expenses_candle", "p1171_07": "last_month_expenses_coal",
            "p1171_08": "last_month_expenses_wood",
            "p1171_09": "last_month_expenses_petroleum", "p1171_10": "last_month_expenses_gasoline",
            "p1171_11": "last_month_expenses_landline", "p1171_12": "last_month_expenses_cellphone",
            "p1171_13": "last_month_expenses_tv_cable", "p1171_14": "last_month_expenses_internet",
            "p1171_15": "last_month_expenses_other", "p1171_16": "last_month_expenses_manure",

            # Values
            "p1172_01": "paid_from_home_water",
            "p1172_02": "paid_from_home_electricity",
            "p1172_04": "paid_from_home_gas_glp",
            "p1172_05": "paid_from_home_natural_gas",
            "p1172_06": "paid_from_home_candle",
            "p1172_07": "paid_from_home_coal",
            "p1172_08": "paid_from_home_wood",
            "p1172_09": "paid_from_home_petroleum",
            "p1172_10": "paid_from_home_gasoline",
            "p1172_11": "paid_from_home_landline",
            "p1172_12": "paid_from_home_cellphone",
            "p1172_13": "paid_from_home_tv_cable",
            "p1172_14": "paid_from_home_internet",
            "p1172_15": "paid_from_home_other",
            "p1172_16": "paid_from_home_manure",

            # Values
            "p1173_01": "paid_from_third_home_water",
            "p1173_02": "paid_from_third_home_electricity",
            "p1173_04": "paid_from_third_home_gas_glp",
            "p1173_05": "paid_from_third_home_natural_gas",
            "p1173_06": "paid_from_third_home_candle",
            "p1173_07": "paid_from_third_home_coal",
            "p1173_08": "paid_from_third_home_wood",
            "p1173_09": "paid_from_third_home_petroleum",
            "p1173_10": "paid_from_third_home_gasoline",
            "p1173_11": "paid_from_third_home_landline",
            "p1173_12": "paid_from_third_home_cellphone",
            "p1173_13": "paid_from_third_home_tv_cable",
            "p1173_14": "paid_from_third_home_internet",
            "p1173_15": "paid_from_third_home_other",
            "p1173_16": "paid_from_third_home_manure",

            # Values
            "p1174_01": "self_supply_water",
            "p1174_02": "self_supply_electricity",
            "p1174_04": "self_supply_gas_glp",
            "p1174_05": "self_supply_natural_gas",
            "p1174_06": "self_supply_candle",
            "p1174_07": "self_supply_coal",
            "p1174_08": "self_supply_wood",
            "p1174_09": "self_supply_petroleum",
            "p1174_10": "self_supply_gasoline",
            "p1174_11": "self_supply_landline",
            "p1174_12": "self_supply_cellphone",
            "p1174_13": "self_supply_tv_cable",
            "p1174_14": "self_supply_internet",
            "p1174_15": "self_supply_other",
            "p1174_16": "self_supply_manure",

            # Alternatives
            "p1175_01": "payment_situation_water",
            "p1175_02": "payment_situation_electricity",
            "p1175_04": "payment_situation_gas_glp",
            "p1175_05": "payment_situation_natural_gas",
            "p1175_06": "payment_situation_candle",
            "p1175_07": "payment_situation_coal",
            "p1175_08": "payment_situation_wood",
            "p1175_09": "payment_situation_petroleum",
            "p1175_10": "payment_situation_gasoline",
            "p1175_11": "payment_situation_landline",
            "p1175_12": "payment_situation_cellphone",
            "p1175_13": "payment_situation_tv_cable",
            "p1175_14": "payment_situation_internet",
            "p1175_15": "payment_situation_other",
            "p1175_16": "payment_situation_manure",

            # Values
            "p117t3": "total_last_month_paid_from_third_home",
            "p117t4": "total_last_month_self_supply",

            "d105b": "monthly_rent_household",
            "d106": "monthly_you_be_paid_rent",
            "d107d1": "credit_buying_house_apartment",
            "d107d2": "credit_ground_house",
            "d107d3": "credit_house_improvements",
            "d107d4": "credit_build_new_house",

            # Values
            "d1172_01": "last_month_total_paid_water",
            "d1172_02": "last_month_total_paid_electricity",
            "d1172_04": "last_month_total_paid_gas_glp",
            "d1172_05": "last_month_total_paid_natural_gas",
            "d1172_06": "last_month_total_paid_candle",
            "d1172_07": "last_month_total_paid_coal",
            "d1172_08": "last_month_total_paid_wood",
            "d1172_09": "last_month_total_paid_petroleum",
            "d1172_10": "last_month_total_paid_gasoline",
            "d1172_11": "last_month_total_paid_landline",
            "d1172_12": "last_month_total_paid_cellphone",
            "d1172_13": "last_month_total_paid_tv_cable",
            "d1172_14": "last_month_total_paid_internet",
            "d1172_15": "last_month_total_paid_other",
            "d1172_16": "last_month_total_paid_manure",

            "d1173_01": "last_month_total_donated_water",
            "d1173_02": "last_month_total_donated_electricity",
            "d1173_04": "last_month_total_donated_gas_glp",
            "d1173_05": "last_month_total_donated_natural_gas",
            "d1173_06": "last_month_total_donated_candle",
            "d1173_07": "last_month_total_donated_coal",
            "d1173_08": "last_month_total_donated_wood",
            "d1173_09": "last_month_total_donated_petroleum",
            "d1173_10": "last_month_total_donated_gasoline",
            "d1173_11": "last_month_total_donated_landline",
            "d1173_12": "last_month_total_donated_cellphone",
            "d1173_13": "last_month_total_donated_tv_cable",
            "d1173_14": "last_month_total_donated_internet",
            "d1173_15": "last_month_total_donated_other",
            "d1173_16": "last_month_total_donated_manure",

            "d1174_01": "last_month_total_self_supply_water",
            "d1174_02": "last_month_total_self_supply_electricity",
            "d1174_04": "last_month_total_self_supply_gas_glp",
            "d1174_05": "last_month_total_self_supply_natural_gas",
            "d1174_06": "last_month_total_self_supply_candle",
            "d1174_07": "last_month_total_self_supply_coal",
            "d1174_08": "last_month_total_self_supply_wood",
            "d1174_09": "last_month_total_self_supply_petroleum",
            "d1174_10": "last_month_total_self_supply_gasoline",
            "d1174_11": "last_month_total_self_supply_landline",
            "d1174_12": "last_month_total_self_supply_cellphone",
            "d1174_13": "last_month_total_self_supply_tv_cable",
            "d1174_14": "last_month_total_self_supply_internet",
            "d1174_15": "last_month_total_self_supply_other",
            "d1174_16": "last_month_total_self_supply_manure",

            # Binary
            "nbi1": "basic_needs_inadequate_house", "nbi2": "basic_needs_overcrowd_house",
            "nbi3": "basic_needs_no_higienic_services", "nbi4": "basic_needs_kids_without_school",
            "nbi5": "basic_needs_high_economic_dependency",
            })

        # Excel spreadsheet automatized replace step 
        for i in df.columns:
            try:
                df[i] = df[i].astype(float)
            except:
                pass


        return df

class ENHPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Year", name="year", dtype=str),
            Parameter(label="Url", name="url", dtype=str)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "ubigeo":                                           "String",
            "dominio":                                          "UInt8",
            "estrato":                                          "UInt8",

            "monthly_rent_household":                           "UInt32",
            "monthly_you_be_paid_rent":                          "UInt32",

            "credit_buying_house_apartment":                        "UInt16",
            "credit_ground_house":                       "UInt16",
            "credit_house_improvements":                        "UInt16",
            "credit_build_new_house":                       "UInt16",

            "last_month_total_paid_water":                  "UInt32",
            "last_month_total_paid_electricity":                    "UInt32",
            "last_month_total_paid_gas_glp":                    "UInt32",
            "last_month_total_paid_natural_gas":                    "UInt32",
            "last_month_total_paid_candle":                     "UInt32",
            "last_month_total_paid_coal":                   "UInt32",
            "last_month_total_paid_wood":                   "UInt32",
            "last_month_total_paid_petroleum":                  "UInt32",
            "last_month_total_paid_gasoline":                   "UInt32",
            "last_month_total_paid_landline":                   "UInt32",
            "last_month_total_paid_cellphone":                  "UInt32",
            "last_month_total_paid_tv_cable":                   "UInt32",
            "last_month_total_paid_internet":                   "UInt32",
            "last_month_total_paid_other":                  "UInt32",
            "last_month_total_paid_manure":                     "UInt32",

            "last_month_total_donated_water":                   "UInt32",
            "last_month_total_donated_electricity":                     "UInt32",
            "last_month_total_donated_gas_glp":                     "UInt32",
            "last_month_total_donated_natural_gas":                     "UInt32",
            "last_month_total_donated_candle":                  "UInt32",
            "last_month_total_donated_coal":                    "UInt32",
            "last_month_total_donated_wood":                    "UInt32",
            "last_month_total_donated_petroleum":                   "UInt32",
            "last_month_total_donated_gasoline":                    "UInt32",
            "last_month_total_donated_landline":                    "UInt32",
            "last_month_total_donated_cellphone":                   "UInt32",
            "last_month_total_donated_tv_cable":                    "UInt32",
            "last_month_total_donated_internet":                    "UInt32",
            "last_month_total_donated_other":                   "UInt32",
            "last_month_total_donated_manure":                  "UInt32",

            "last_month_total_self_supply_water":               "UInt32",
            "last_month_total_self_supply_electricity":                 "UInt32",
            "last_month_total_self_supply_gas_glp":              "UInt32",
            "last_month_total_self_supply_natural_gas":              "UInt32",
            "last_month_total_self_supply_candle":               "UInt32",
            "last_month_total_self_supply_coal":                 "UInt32",
            "last_month_total_self_supply_wood":                "UInt32",
            "last_month_total_self_supply_petroleum":               "UInt32",
            "last_month_total_self_supply_gasoline":                 "UInt32",
            "last_month_total_self_supply_landline":                 "UInt32",
            "last_month_total_self_supply_cellphone":                "UInt32",
            "last_month_total_self_supply_tv_cable":                "UInt32",
            "last_month_total_self_supply_internet":                "UInt32",
            "last_month_total_self_supply_other":               "UInt32",
            "last_month_total_self_supply_manure":              "UInt32",

            "basic_needs_inadequate_house":                         "UInt8",
            "basic_needs_overcrowd_house":                      "UInt8",
            "basic_needs_no_higienic_services":                         "UInt8",
            "basic_needs_kids_without_school":                      "UInt8",
            "basic_needs_high_economic_dependency":                         "UInt8",
            "type_household":                                       "UInt8",
            "walls_material":                                       "UInt8",
            "floor_material":                                       "UInt8",
            "ceiling_material":                                         "UInt8",
            "type_home":                                        "UInt8",

            "household_property_title":                                         "UInt8",
            "household_property_title_SUNARP":                                      "UInt8",
            "water_source":                                         "UInt8",
            "is_water_drinkable":                                       "UInt8",
            "water_all_week":                                       "UInt8",
            "water_payment":                                        "UInt8",
            "water_source_company":                                         "UInt8",
            "sewer_conection":                                      "UInt8",
            "light_electricity":                                         "UInt8",
            "light_petroleum_gas":                                       "UInt8",
            "light_candle":                                              "UInt8",
            "light_generator":                                              "UInt8",
            "light_other":                                              "UInt8",
            "light_no_use_home":                                                "UInt8",
            "electric_meter_type":                                              "UInt8",
            "cooking_fuel_electricity":                                                 "UInt8",
            "cooking_fuel_gas_glp":                                                 "UInt8",
            "cooking_fuel_natural_gas":                                                 "UInt8",
            "cooking_fuel_coal":                                                "UInt8",
            "cooking_fuel_kerosene":                                                "UInt8",
            "cooking_fuel_other":                                               "UInt8",
            "cooking_fuel_no_cook":                                                 "UInt8",
            "cooking_fuel_main":                                                "UInt8",
            "has_landline":                                                     "UInt8",
            "has_cellphone":                                                    "UInt8",
            "has_tv_cable_satelite":                                                    "UInt8",
            "has_internet":                                                     "UInt8",
            "has_not":                                                  "UInt8",
            "last_month_expenses_water":                                "UInt8",
            "last_month_expenses_electricity":                              "UInt8",
            "last_month_expenses_gas_glp":                              "UInt8",
            "last_month_expenses_natural_gas":                              "UInt8",
            "last_month_expenses_candle":                               "UInt8",
            "last_month_expenses_coal":                                 "UInt8",
            "last_month_expenses_wood":                                 "UInt8",
            "last_month_expenses_petroleum":                                "UInt8",
            "last_month_expenses_gasoline":                                 "UInt8",
            "last_month_expenses_landline":                                 "UInt8",
            "last_month_expenses_cellphone":                                "UInt8",
            "last_month_expenses_tv_cable":                                 "UInt8",
            "last_month_expenses_internet":                                 "UInt8",
            "last_month_expenses_other":                                    "UInt8",
            "last_month_expenses_manure":                                   "UInt8",
            "paid_from_home_water":                                     "UInt8",
            "paid_from_home_electricity":                                   "UInt8",
            "paid_from_home_gas_glp":                                   "UInt8",
            "paid_from_home_natural_gas":                                    "UInt8",
            "paid_from_home_candle":                                    "UInt8",
            "paid_from_home_coal":                                  "UInt8",
            "paid_from_home_wood":                                  "UInt8",
            "paid_from_home_petroleum":                                     "UInt8",
            "paid_from_home_gasoline":                                  "UInt8",
            "paid_from_home_landline":                                  "UInt8",
            "paid_from_home_cellphone":                                  "UInt8",
            "paid_from_home_tv_cable":                                  "UInt8",
            "paid_from_home_internet":                                  "UInt8",
            "paid_from_home_other":                                  "UInt8",
            "paid_from_home_manure":                                    "UInt8",
            "paid_from_third_home_water":                                   "UInt8",
            "paid_from_third_home_electricity":                             "UInt8",
            "paid_from_third_home_gas_glp":                             "UInt8",
            "paid_from_third_home_natural_gas":                          "UInt8",
            "paid_from_third_home_candle":                          "UInt8",
            "paid_from_third_home_coal":                            "UInt8",
            "paid_from_third_home_wood":                            "UInt8",
            "paid_from_third_home_petroleum":                           "UInt8",
            "paid_from_third_home_gasoline":                             "UInt8",
            "paid_from_third_home_landline":                             "UInt8",
            "paid_from_third_home_cellphone":                            "UInt8",
            "paid_from_third_home_tv_cable":                             "UInt8",
            "paid_from_third_home_internet":                             "UInt8",
            "paid_from_third_home_other":                            "UInt8",
            "paid_from_third_home_manure":                           "UInt8",
            "self_supply_water":                                    "UInt8",
            "self_supply_electricity":                                  "UInt8",
            "self_supply_gas_glp":                                  "UInt8",
            "self_supply_natural_gas":                                  "UInt8",
            "self_supply_candle":                                   "UInt8",
            "self_supply_coal":                                     "UInt8",
            "self_supply_wood":                                     "UInt8",
            "self_supply_petroleum":                                 "UInt8",
            "self_supply_gasoline":                                 "UInt8",
            "self_supply_landline":                                 "UInt8",
            "self_supply_cellphone":                                 "UInt8",
            "self_supply_tv_cable":                                 "UInt8",
            "self_supply_internet":                                 "UInt8",
            "self_supply_other":                                "UInt8",
            "self_supply_manure":                               "UInt8",
            "payment_situation_water":                          "UInt8",
            "payment_situation_electricity":                    "UInt8",
            "payment_situation_gas_glp":                            "UInt8",
            "payment_situation_natural_gas":                            "UInt8",
            "payment_situation_candle":                             "UInt8",
            "payment_situation_coal":                           "UInt8",
            "payment_situation_wood":                           "UInt8",
            "payment_situation_petroleum":                          "UInt8",
            "payment_situation_gasoline":                           "UInt8",
            "payment_situation_landline":                           "UInt8",
            "payment_situation_cellphone":                          "UInt8",
            "payment_situation_tv_cable":                           "UInt8",
            "payment_situation_internet":                           "UInt8",
            "payment_situation_other":                          "UInt8",
            "payment_situation_manure":                             "UInt8",
            "total_last_month_paid_from_third_home":            "UInt8",
            "total_last_month_self_supply":                                  "UInt8",
            "factor07":                                             "UInt8",
            "year":                                                 "UInt16",
        }

        transform_step = TransformStep()

        load_step = LoadStep(
            "housing_survey_home", db_connector, if_exists="append", pk=["ubigeo", "year"], dtype=dtype, 
            nullable_list=[
              "monthly_rent_household", "monthly_you_be_paid_rent", "credit_buying_house_apartment", "credit_ground_house", "credit_house_improvements",
              "credit_build_new_house", "last_month_total_paid_water", "last_month_total_paid_electricity", "last_month_total_paid_gas_glp",
              "last_month_total_paid_natural_gas", "last_month_total_paid_candle", "last_month_total_paid_coal", "last_month_total_paid_wood",
              "last_month_total_paid_petroleum", "last_month_total_paid_gasoline", "last_month_total_paid_landline", "last_month_total_paid_cellphone",
              "last_month_total_paid_tv_cable", "last_month_total_paid_internet", "last_month_total_paid_other", "last_month_total_paid_manure",
              "last_month_total_donated_water", "last_month_total_donated_electricity", "last_month_total_donated_gas_glp", "last_month_total_donated_natural_gas",
              "last_month_total_donated_candle", "last_month_total_donated_coal", "last_month_total_donated_wood", "last_month_total_donated_petroleum",
              "last_month_total_donated_gasoline", "last_month_total_donated_landline", "last_month_total_donated_cellphone", "last_month_total_donated_tv_cable",
              "last_month_total_donated_internet", "last_month_total_donated_other", "last_month_total_donated_manure", "last_month_total_self_supply_water",
              "last_month_total_self_supply_electricity", "last_month_total_self_supply_gas_glp", "last_month_total_self_supply_natural_gas", "last_month_total_self_supply_candle",
              "last_month_total_self_supply_coal", "last_month_total_self_supply_wood", "last_month_total_self_supply_petroleum", "last_month_total_self_supply_gasoline",
              "last_month_total_self_supply_landline", "last_month_total_self_supply_cellphone", "last_month_total_self_supply_tv_cable", "last_month_total_self_supply_internet",
              "last_month_total_self_supply_other", "last_month_total_self_supply_manure",
              "basic_needs_inadequate_house", "basic_needs_overcrowd_house",
              "basic_needs_no_higienic_services", "basic_needs_kids_without_school", "basic_needs_high_economic_dependency", "type_household",
              "walls_material", "floor_material", "ceiling_material", "type_home",
              "household_property_title", "household_property_title_SUNARP", "water_source", "is_water_drinkable",
              "water_all_week", "water_payment", "water_source_company", "sewer_conection",
              "light_electricity", "light_petroleum_gas", "light_candle", "light_generator", "light_other", "light_no_use_home",
              "electric_meter_type",
              "cooking_fuel_electricity", "cooking_fuel_gas_glp", "cooking_fuel_natural_gas", "cooking_fuel_coal", "cooking_fuel_kerosene",
              "cooking_fuel_other", "cooking_fuel_no_cook", "cooking_fuel_main",
              "has_landline", "has_cellphone", "has_tv_cable_satelite", "has_internet", "has_not",
              "last_month_expenses_water", "last_month_expenses_electricity", "last_month_expenses_gas_glp", "last_month_expenses_natural_gas",
              "last_month_expenses_candle", "last_month_expenses_coal", "last_month_expenses_wood", "last_month_expenses_petroleum",
              "last_month_expenses_gasoline", "last_month_expenses_landline", "last_month_expenses_cellphone", "last_month_expenses_tv_cable",
              "last_month_expenses_internet", "last_month_expenses_other", "last_month_expenses_manure",
              "paid_from_home_water", "paid_from_home_electricity", "paid_from_home_gas_glp", "paid_from_home_natural_gas",
              "paid_from_home_candle", "paid_from_home_coal", "paid_from_home_wood", "paid_from_home_petroleum", "paid_from_home_gasoline",
              "paid_from_home_landline", "paid_from_home_cellphone", "paid_from_home_tv_cable", "paid_from_home_internet",
              "paid_from_home_other", "paid_from_home_manure",
              "paid_from_third_home_water", "paid_from_third_home_electricity", "paid_from_third_home_gas_glp", "paid_from_third_home_natural_gas",
              "paid_from_third_home_candle", "paid_from_third_home_coal", "paid_from_third_home_wood", "paid_from_third_home_petroleum",
              "paid_from_third_home_gasoline", "paid_from_third_home_landline", "paid_from_third_home_cellphone", "paid_from_third_home_tv_cable",
              "paid_from_third_home_internet", "paid_from_third_home_other", "paid_from_third_home_manure", "self_supply_water",
              "self_supply_electricity", "self_supply_gas_glp", "self_supply_natural_gas", "self_supply_candle",
              "self_supply_coal", "self_supply_wood", "self_supply_petroleum", "self_supply_gasoline",
              "self_supply_landline", "self_supply_cellphone", "self_supply_tv_cable", "self_supply_internet",
              "self_supply_other", "self_supply_manure",
              "payment_situation_water", "payment_situation_electricity", "payment_situation_gas_glp", "payment_situation_natural_gas",
              "payment_situation_candle", "payment_situation_coal", "payment_situation_wood", "payment_situation_petroleum",
              "payment_situation_gasoline", "payment_situation_landline", "payment_situation_cellphone", "payment_situation_tv_cable",
              "payment_situation_internet", "payment_situation_other", "payment_situation_manure",
              "total_last_month_paid_from_third_home", "total_last_month_self_supply"
              ]
        )

        return [transform_step, load_step]

if __name__ == "__main__":
    
    data = glob.glob('../../data/enh/*.dta')

    pp = ENHPipeline()
    for year in range(2014, 2018 + 1):
    #for year in range(2018, 2018 + 1):
        pp.run({
            'url': '../../data/enh/enaho01-{}-100.dta'.format(year),
            'year': year
        })