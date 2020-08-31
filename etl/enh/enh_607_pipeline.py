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

        batch_2018 = ['ubigeo', 'dominio', 'estrato', 'p607n',
                      'p607', 'p607a1','p607a2', 'p607a3', 'p607a4',
                      'p607a5', 'p607a6', 'p607a7', 'p607a8', 'p607aa', 'p607b', 'p607c',
                      'p607c2', 'p607c3', 'p607c4', 'p607c5', 'p607c6', 'p607c7',
                      'd607b', 'd607c',
                      'd607c2', 'd607c3', 'd607c4', 'd607c5', 'd607c6', 'd607c7', 
                      'factor07']

        batch_2015 = ['ubigeo', 'dominio', 'estrato', 'p607n',
                      'p607', 'p607a1','p607a2', 'p607a3', 'p607a4',
                      'p607a5', 'p607a6', 'p607a7', 'p607a8', 'p607aa', 'p607b', 'p607c', 

                      'd607b', 'd607c',

                      'factor07']

        # Loading dataframe stata step
        try: 
            df = pd.read_stata(params.get('url'), columns = batch_2018)
        except:
            df = pd.read_stata(params.get('url'), columns = batch_2015)

        # Adding missing columns between years dataset
        missing_col = ['p607c2', 'p607c3', 'p607c4', 'p607c5', 'p607c6', 'p607c7',
                       'd607c2', 'd607c3', 'd607c4', 'd607c5', 'd607c6', 'd607c7']
        for item in missing_col:
            if item not in df:
                df[item] = pd.np.nan

        # Excel spreadsheet for replace text to id step
        df_labels = "https://docs.google.com/spreadsheets/d/e/2PACX-1vS1jXemsL6QjdyTRO2Qq7_XtZV8ri1nwe9lz0OTwTLCRLhXk7rD81LIi9F7B_CtllYq1hpVyDAJ-3sg/pub?output=xlsx"

        # Getting values of year for the survey
        df["year"] = int(params.get('year'))

        # Deleting empty spaces
        df["estrato"] = df["estrato"].str.strip()
        df["p606n"] = df["p606n"].str.strip()
        df["p606ee"] = df["p606ee"].str.strip()

        # Excel spreadsheet automatized replace step 
        for i in df.columns:
            try:
                df_page = pd.read_excel(df_labels, i)
                df[i] = df[i].replace(dict(zip(df_page.col, df_page.id)))
            except:
                pass

        # Renaming columns to an understandable name
        df = df.rename(columns={
                #Variables deflectadas
                "d606f": "total_amount_paid_annualized",
                "d606g": "how_much_think_cost_annualized",
                "d606g2": "how_much_think_cost_self_consumption_annualized",
                "d606g3": "how_much_think_cost_self_supply_annualized",
                "d606g4": "how_much_think_cost_family_member_annualized",
                "d606g5": "how_much_think_cost_from_third_home_annualized",
                "d606g6": "how_much_think_cost_donated_annualized",
                "d606g7": "how_much_think_cost_other_annualized",

                #Consumption
                "p606d": "did_home_get_product_service",
                "p606e1": "get_product_service_buy",
                "p606e2": "get_product_service_self_consumption",
                "p606e3": "get_product_service_self_supply",
                "p606e4": "get_product_service_family_member",
                "p606e5": "get_product_service_from_third_home",
                "p606e6": "get_product_service_donated",
                "p606e7": "get_product_service_other",
                "p606e8": "get_product_service_does_not_know",
                "p606ee": "where_get_product_service",
                "p606f": "total_amount_paid",
                "p606g": "how_much_think_cost",
                "p606g2": "how_much_think_cost_self_consumption",
                "p606g3": "how_much_think_cost_self_supply",
                "p606g4": "how_much_think_cost_family_member",
                "p606g5": "how_much_think_cost_from_third_home",
                "p606g6": "how_much_think_cost_donated",
                "p606g7": "how_much_think_cost_other",
                "p606n": "product_service_id"
        })

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
            Parameter(label="Url", name="url", dtype=str),
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "ubigeo":                                             "UInt32",
            "dominio":                                            "UInt8",
            "estrato":                                            "UInt8",
            "total_amount_paid_annualized":                       "UInt32",
            "how_much_think_cost_annualized":                     "UInt32",
            "how_much_think_cost_self_consumption_annualized":    "UInt32",
            "how_much_think_cost_self_supply_annualized":         "UInt32",
            "how_much_think_cost_family_member_annualized":       "UInt32",
            "how_much_think_cost_from_third_home_annualized":     "UInt32",
            "how_much_think_cost_donated_annualized":             "UInt32",
            "how_much_think_cost_other_annualized":               "UInt32",
            "did_home_get_product_service":                       "UInt32",
            "get_product_service_buy":                            "UInt8",
            "get_product_service_self_consumption":               "UInt8",
            "get_product_service_self_supply":                    "UInt8",
            "get_product_service_family_member":                  "UInt8",  
            "get_product_service_from_third_home":                "UInt8",
            "get_product_service_donated":                        "UInt8",
            "get_product_service_other":                          "UInt8",
            "get_product_service_does_not_know":                  "UInt8",
            "where_get_product_service":                          "UInt8",
            "total_amount_paid":                                  "UInt32",
            "how_much_think_cost":                                "UInt32",
            "how_much_think_cost_self_consumption":               "UInt32",
            "how_much_think_cost_self_supply":                    "UInt32",
            "how_much_think_cost_family_member":                  "UInt32",
            "how_much_think_cost_from_third_home":                "UInt32",
            "how_much_think_cost_donated":                        "UInt32",
            "how_much_think_cost_other":                          "UInt32",
            "factor07":                                           "UInt8",
            "year":                                               "UInt16",
        }

        transform_step = TransformStep()

        load_step = LoadStep(
            "housing_survey_606d", db_connector, if_exists="append", pk=["ubigeo", "year", "product_service_id"], dtype=dtype, 
            nullable_list=[
              "total_amount_paid_annualized", "how_much_think_cost_annualized", "how_much_think_cost_self_consumption_annualized",
              "how_much_think_cost_self_supply_annualized", "how_much_think_cost_family_member_annualized",
              "how_much_think_cost_from_third_home_annualized", "how_much_think_cost_donated_annualized",
              "how_much_think_cost_other_annualized", "did_home_get_product_service", "get_product_service_buy",
              "get_product_service_self_consumption", "get_product_service_self_supply", "get_product_service_family_member",
              "get_product_service_from_third_home", "get_product_service_donated", "get_product_service_other",
              "get_product_service_does_not_know", "where_get_product_service", "total_amount_paid", "how_much_think_cost",
              "how_much_think_cost_self_consumption", "how_much_think_cost_self_supply", "how_much_think_cost_family_member",
              "how_much_think_cost_from_third_home", "how_much_think_cost_donated", "how_much_think_cost_other", "product_service_id" 
              ]
        )

        return [transform_step, load_step]

if __name__ == "__main__":
    
    data = glob.glob('../../data/enh/*.dta')

    pp = ENHPipeline()
    for year in range(2014, 2018 + 1):
    #for year in range(2018, 2018 + 1):
        pp.run({
            'url': '../../data/enh/enaho01-{}-606d.dta'.format(year),
            'year': year
        })