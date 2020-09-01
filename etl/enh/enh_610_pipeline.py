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
        batch_2018 = ['ubigeo', 'dominio', 'estrato',
                    'p610n', 'p610', 'p610a1', 'p610a2', 'p610a3', 'p610a4',
                    'p610a5', 'p610a6', 'p610a7', 'p610a8', 'p610aa',
                    'p610b', 'p610c',
                    'p610c2', 'p610c3', 'p610c4', 'p610c5', 'p610c6', 'p610c7',
                    'd610b','d610c',
                    'd610c2', 'd610c3', 'd610c4', 'd610c5', 'd610c6', 'd610c7',
                    'factor07']

        batch_2015 = ['ubigeo', 'dominio', 'estrato',
                    'p610n', 'p610', 'p610a1', 'p610a2', 'p610a3', 'p610a4',
                    'p610a5', 'p610a6', 'p610a7', 'p610a8', 'p610aa',

                    'p610b', 'p610c',
                    'd610b', 'd610c',

                    'factor07']

        # Loading dataframe stata step
        try: 
            df = pd.read_stata(params.get('url'), columns = batch_2018)
        except:
            df = pd.read_stata(params.get('url'), columns = batch_2015)

        # Adding missing columns between years dataset
        missing_col = ['d610c2', 'd610c3', 'd610c4', 'd610c5', 'd610c6', 'd610c7',
                       'p610c2', 'p610c3', 'p610c4', 'p610c5', 'p610c6', 'p610c7']
        for item in missing_col:
            if item not in df:
                df[item] = pd.np.nan

        # Excel spreadsheet for replace text to id step
        df_labels = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRYllPCwdeNdcEaUI30oM3eMwn74f09CfbBwZSIJh4H789yAyiO1UoAwPnPI1TcEZP1Ltm0ZDx7pnGq/pub?output=xlsx"

        # Getting values of year for the survey
        df["year"] = int(params.get('year'))

        # Deleting empty spaces
        df["estrato"] = df["estrato"].str.strip()
        df["p610n"] = df["p610n"].str.strip()

        # Excel spreadsheet automatized replace step 
        for i in df.columns:
            try:
                df_page = pd.read_excel(df_labels, i)
                df[i] = df[i].replace(dict(zip(df_page.col, df_page.id)))
            except:
                pass

        # Renaming columns to an understandable name
        df = df.rename(columns={
            "p610": "did_home_get_product_service",
            "p610n": "product_service_id",
            # Product Origin
            "p610a1": "get_product_service_buy",
            "p610a2": "get_product_service_self_consumption",
            "p610a3": "get_product_service_self_supply",
            "p610a4": "get_product_service_family_member",
            "p610a5": "get_product_service_from_third_home",
            "p610a6": "get_product_service_donated",
            "p610a7": "get_product_service_other",
            "p610a8": "get_product_service_does_not_know",
            "p610aa": "where_get_product_service",
            
            # Consumption
            "p610b": "total_amount_paid",
            "p610c": "how_much_think_cost",
            "p610c2": "how_much_think_cost_self_consumption",
            "p610c3": "how_much_think_cost_self_supply",
            "p610c4": "how_much_think_cost_family_member",
            "p610c5": "how_much_think_cost_from_third_home",
            "p610c6": "how_much_think_cost_donated",
            "p610c7": "how_much_think_cost_other",
            
            #Variables deflectadas
            "d610b": "total_amount_paid_annualized",
            "d610c": "how_much_think_cost_annualized",
            "d610c2": "how_much_think_cost_self_consumption_annualized",
            "d610c3": "how_much_think_cost_self_supply_annualized",
            "d610c4": "how_much_think_cost_family_member_annualized",
            "d610c5": "how_much_think_cost_from_third_home_annualized",
            "d610c6": "how_much_think_cost_donated_annualized",
            "d610c7": "how_much_think_cost_other_annualized",
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
            "ubigeo":                                             "String",
            "dominio":                                            "UInt8",
            "estrato":                                            "UInt8",
            "product_service_id":                                 "UInt8",
            "total_amount_paid_annualized":                       "UInt32",
            "how_much_think_cost_annualized":                     "UInt32",
            "how_much_think_cost_self_consumption_annualized":    "UInt32",
            "how_much_think_cost_self_supply_annualized":         "UInt32",
            "how_much_think_cost_family_member_annualized":       "UInt32",
            "how_much_think_cost_from_third_home_annualized":     "UInt32",
            "how_much_think_cost_donated_annualized":             "UInt32",
            "how_much_think_cost_other_annualized":               "UInt32",
            "did_home_get_product_service":                       "UInt8",
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
            "housing_survey_furniture", db_connector, if_exists="append", pk=["ubigeo", "year"], dtype=dtype,
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
            'url': '../../data/enh/enaho01-{}-610.dta'.format(year),
            'year': year
        })