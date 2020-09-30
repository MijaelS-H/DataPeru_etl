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
        batch = ["ubigeo", "dominio", "estrato",
                    "p501", "p502", "p503",
                    "p5041", "p5042", "p5043", "p5044", "p5045", "p5046", "p5047",
                    "p5048", "p5049", "p50410", "p50411",
                    "p507", "p510", "p514",
                    "p5151", "p5152", "p5153", "p5154", "p5155", "p5156",
                    "p5157", "p5158", "p5159", "p51510", "p51511",
                    "p520", "p521d",
                    "p524a1", "p524e1", "p538a1", "p538e1", "p558c", "p558d",
                    "fac500a"]


        # Loading dataframes stata step
        df = pd.read_stata(params.get('url1'), columns = batch)

        print("Version de la encuesta año", params.get('year'))

        # Cleaning column to id replacing step
        df["estrato"].replace({"." : ""}, inplace= True)
        df["estrato"] = df["estrato"].str.lstrip()

        # Excel spreadsheet for replacing steps
        df_labels = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRbA9MTCzCg2BVDB13SQomN0aDIbPVDGkeELcGWNPzgm9ptQj2D6Dx9taogqtSFFo1aWvsK8RK8Pcdd/pub?output=xlsx"
        df_cols_names = "https://docs.google.com/spreadsheets/d/e/2PACX-1vS556vrgVnk80uquHVKSHekXyb4Os6Mg5OItZV-oWo9i-YRSU4LvrnHLaV-43Mbyc7mS6tvSTJRL-_j/pub?output=xlsx"

        # Excel spreadsheet automatized replace id step
        for i in df.columns:
            try:
                df_page = pd.read_excel(df_labels, i)
                df[i] = df[i].replace(dict(zip(df_page.col, df_page.id)))
            except:
                pass

        # Excel spreadsheet automatized rename columns step
        df_page = pd.read_excel(df_cols_names, "Module -500")
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
            Parameter(label="Url1", name="url1", dtype=str)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "ubigeo":                                             "String",
            "dominio":                                            "UInt8",
            "estrato":                                            "UInt8",
            "had_work_last_week":                                 "UInt8",
            "if_no_have_one_go_to":                               "UInt8",
            "if_no_have_own_business":                            "UInt8",
            "last_week_work_family_own_business":                 "UInt8",
            "last_week_work_providing_service":                   "UInt8",
            "last_week_work_selling_homemade_products":           "UInt8",
            "last_week_work_jewelry_cosmetics_clothes":           "UInt8",
            "last_week_work_artesanal_labor":                     "UInt8",
            "last_week_work_practice_hours":                      "UInt8",
            "last_week_work_particular_home":                     "UInt8",
            "last_week_work_making_product":                      "UInt8",
            "last_week_work_farming_livestock":                   "UInt8",
            "last_week_work_no_remuneration_familiar":            "UInt8",
            "last_week_work_other":                               "UInt8",
            "main_occupation_job_position":                       "UInt8",
            "main_occupation_type_employer":                      "UInt8",
            "had_second_occupation":                              "UInt8",
            "second_occ_work_family_own_business":                "UInt8",
            "second_occ_work_providing_service":                  "UInt8",
            "second_occ_work_selling_homemade_products":          "UInt8",
            "second_occ_work_jewelry_cosmetics_clothes":          "UInt8",
            "second_occ_work_artesanal_labor":                    "UInt8",
            "second_occ_work_practice_hours":                     "UInt8",
            "second_occ_work_particular_home":                    "UInt8",
            "second_occ_work_making_product":                     "UInt8",
            "second_occ_work_farming_livestock":                  "UInt8",
            "second_occ_work_no_remuneration_familiar":           "UInt8",
            "second_occ_work_other":                              "UInt8",
            "combined_occ_total_hours_worked":                    "UInt8",
            "reason_wants_another_new_job":                       "UInt8",
            "main_occupation_total_gross_income":                 "UInt32",
            "main_occupation_total_net_income":                   "UInt32",
            "second_occupation_total_gross_income":               "UInt16",
            "second_occupation_total_net_income":                 "UInt16",
            "identified_yourself_as_race":                        "UInt8",
            "belong_to_native_population":                        "UInt8",
            "fac500a":                                            "Float",
            "year":                                               "UInt16"
        }

        transform_step = TransformStep()

        load_step = LoadStep(
            "enaho_workers", db_connector, if_exists="append", pk=["ubigeo", "year"], dtype=dtype, 
            nullable_list=[
                "had_work_last_week", "if_no_have_one_go_to", "if_no_have_own_business", "last_week_work_family_own_business",
                "last_week_work_providing_service", "last_week_work_selling_homemade_products", "last_week_work_jewelry_cosmetics_clothes",
                "last_week_work_artesanal_labor", "last_week_work_practice_hours", "last_week_work_particular_home",
                "last_week_work_making_product", "last_week_work_farming_livestock", "last_week_work_no_remuneration_familiar",
                "last_week_work_other", "main_occupation_job_position", "main_occupation_type_employer", "had_second_occupation",
                "second_occ_work_family_own_business", "second_occ_work_providing_service", "second_occ_work_selling_homemade_products",
                "second_occ_work_jewelry_cosmetics_clothes", "second_occ_work_artesanal_labor", "second_occ_work_practice_hours",
                "second_occ_work_particular_home", "second_occ_work_making_product", "second_occ_work_farming_livestock",
                "second_occ_work_no_remuneration_familiar", "second_occ_work_other", "combined_occ_total_hours_worked",
                "reason_wants_another_new_job", "main_occupation_total_gross_income", "main_occupation_total_net_income",
                "second_occupation_total_gross_income", "second_occupation_total_net_income", "identified_yourself_as_race", "belong_to_native_population"
              ]
        )

        return [transform_step, load_step]

if __name__ == "__main__":
    
    data = glob.glob('../../data/enh/*.dta')

    pp = ENHPipeline()
    for year in range(2014, 2018 + 1):
        pp.run({
            'url1': '../../data/enh/enaho01a-{}-500.dta'.format(year),
            'year': year
        })