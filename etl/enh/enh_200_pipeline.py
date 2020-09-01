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
        batch_tt = ['codperso', 'ubigeo', 'dominio', 'estrato', 'p207', 'p208a', 'p208b', 'p209', 'facpob07']

        # Loading dataframe stata step
        df = pd.read_stata(params.get('url'), columns = batch_tt)

        # Getting values of year for the survey
        df["year"] = int(params.get('year'))

        # Deleting empty spaces
        df["estrato"] = df["estrato"].str.strip()
        df["p209"] = df["p209"].str.strip()

        # Replace step
        df["p207"].replace({
            "hombre" : 1,
            "mujer" : 2
        }, inplace= True)

        df["p209"].replace({
            "conviviente" : 1,
            "casado(a)" : 2,
            "casado (a)" : 2,
            "viudo(a)" : 3,
            "viudo (a)" : 3,
            "divorciado(a)" : 4,
            "divorciado (a)" : 4,
            "separado(a)" : 5,
            "separado (a)" : 5,
            "soltero(a)" : 6,
            "soltero (a)" :6 
        }, inplace= True)

        df["dominio"].replace({
            "costa norte" : 1,
            "costa centro" : 2,
            "costa sur" : 3,
            "sierra norte" : 4,
            "sierra centro" : 5,
            "sierra sur" : 6,
            "selva" : 7,
            "lima metropolitana" : 8
        }, inplace= True)

        df["estrato"].replace({
            "de 500 000 a más habitantes" : 1,
            "de 100 000 a 499 999 habitantes" : 2,
            "de 50 000 a 99 999 habitantes" : 3,
            "de 20 000 a 49 999 habitantes" : 4,
            "de 2 000 a 19 999 habitantes" : 5,
            "de 500 a 1 999 habitantes" : 6,
            "Área de empadronamiento rural (AER) compuesto" : 7,
            "Área de empadronamiento rural (AER) simple" : 8,
            "Área de empadronamiento rural (aer) compuesto" : 7,
            "Área de empadronamiento rural (aer) simple" : 8,
            "centros poblados con más de 100,000 viviendas" : 1,
            "centros poblados de 20,001 a 100,000 viviendas" : 2,
            "centros poblados de 10,001 a 20,000 viviendas" : 3,
            "centros poblados de 4,001 a 10,000 viviendas" : 4,
            "centros poblados de 401 a 4,000 viviendas" : 5,
            "centros poblados con menos de 401 viviendas" : 6,
            "Área de empadronamiento rural - aer compuesto" : 7,
            "Área de empadronamiento rural - aer simple" : 8,
            "mayor de 100,000 viviendas" : 1,
            "de 20,001 a 100,000 viviendas" : 2,
            "de 10,001 a 20,000 viviendas" : 3,
            "de 4,001 a 10,000 viviendas" : 4,
            "de 401 a 4,000 viviendas" : 5,
            "con menos de 401 viviendas" : 6,
            "401 a 4,000 viviendas" : 5,
            "menos de 401 viviendas" : 6
        }, inplace= True)

        # Renaming columns to an understandable name
        df = df.rename(columns={
            "p207": "gender_id",
            "p208a": "age_years",
            "p208b": "age_months",
            "p209": "civil_status",
            "facpob07": "factor07"
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
            "codperso":                         "String",
            "ubigeo":                           "String",
            "dominio":                          "UInt8",
            "estrato":                          "UInt8",
            "gender_id":                        "UInt8",
            "age_years":                        "UInt8",
            "age_months":                       "UInt16",
            "civil_status":                     "UInt8",
            "facpob07":                         "UInt32",
            "year":                             "UInt16"
        }

        transform_step = TransformStep()

        load_step = LoadStep(
            "housing_survey_population", db_connector, if_exists="append", pk=["ubigeo", "year", "codperso"], dtype=dtype, 
            nullable_list=["gender_id", "age_years", "age_months", "civil_status"]
        )

        return [transform_step, load_step]

if __name__ == "__main__":

    data = glob.glob('../../data/enh/*.dta')

    pp = ENHPipeline()
    for year in range(2014, 2018 + 1):
    #for year in range(2018, 2018 + 1):
        pp.run({
            'url': '../../data/enh/enaho01-{}-200.dta'.format(year),
            'year': year
        })