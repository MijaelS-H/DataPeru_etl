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
        batch_tt = ['activida', 'ubigeo', 'dominio', 'estrato', 'codinfor', 'periodo',
                    'e24a', 'e24b', 'e24c', 'e24d', 'e24e1', 'e24e2',
                    'e24f', 'e24g', 'e24h', 'e24i', 'factora07']

        # Loading dataframe stata step
        df = pd.read_stata(params.get('url'), columns = batch_tt)

        # Excel spreadsheet for replace text to id step
        df_labels = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSla-szgKx1SGwW5YglxN7qyB92eDCpSkBhvfQi0QMIjIIgcp20pTSzC6D5P5uOkwqzev2iLtHSPNIQ/pub?output=xlsx"

        # Getting values of year for the survey
        df["year"] = int(params.get('year'))

        # Deleting empty spaces
        df["estrato"] = df["estrato"].str.strip()
        df["periodo"] = df["periodo"].str.strip()

        # Excel spreadsheet automatized replace step 
        for i in df.columns:
            try:
                df_page = pd.read_excel(df_labels, i)
                df[i] = df[i].replace(dict(zip(df_page.col, df_page.id)))
            except:
                pass

        # Renaming columns to an understandable name
        df = df.rename(columns={
            "activida": "main_secondary_occupation",
            "e24a": "coworkers_relative_numbers",
            "e24b": "gender_id",
            "e24c": "age",
            "e24d": "last_approved_education",
            "e24e1": "working_years",
            "e24e2": "working_months",
            "e24f": "worked_hours_last_week",
            "e24g": "full_gross_salary",
            "e24h": "work_has_healthcare",    
            "e24i": "employ_relative",
            "factora07": "factor07"
        })

        # Changing type columns step
        for i in df.columns:
            try:
                df[i] = df[i].astype(pd.Int8Dtype())
            except:
                pass

        for j in ["main_secondary_occupation", "dominio", "estrato", "periodo",
                  "coworkers_relative_numbers", "gender_id"]:
            df[j] = df[j].astype(int)

        return df

class ENHPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Year", name="year", dtype=str),
            Parameter(label="Url", name="url", dtype=str),
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "ubigeo":                                             "String",
            "codinfor":                                           "String",
            "dominio":                                            "UInt8",
            "estrato":                                            "UInt8",
            "main_secondary_occupation":                          "UInt8",
            "periodo":                                            "UInt8",
            "coworkers_relative_numbers":                         "UInt8",
            "gender_id":                                          "UInt8",
            "age":                                                "UInt16",
            "last_approved_education":                            "UInt8",
            "working_years":                                      "UInt16",
            "working_months":                                     "UInt16",
            "worked_hours_last_week":                             "UInt16",
            "full_gross_salary":                                  "UInt32",
            "work_has_healthcare":                                "UInt8",
            "employ_relative":                                    "UInt8",
            "factor07":                                           "UInt8",
            "year":                                               "UInt16",
        }

        transform_step = TransformStep()

        load_step = LoadStep(
            "housing_survey_employment", db_connector, if_exists="append", pk=["ubigeo", "year"], dtype=dtype,
            nullable_list=[
                "age", "last_approved_education", "working_years", "working_months", "worked_hours_last_week",
                "full_gross_salary", "work_has_healthcare", "employ_relative"
              ]
        )

        return [transform_step, load_step]

if __name__ == "__main__":
    
    data = glob.glob('../../data/enh/*.dta')

    pp = ENHPipeline()
    for year in range(2014, 2018 + 1):
    #for year in range(2018, 2018 + 1):
        pp.run({
            'url': '../../data/enh/enaho04-{}-4-preg-24.dta'.format(year),
            'year': year
        })