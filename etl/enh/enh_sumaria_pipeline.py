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
        batch_tt = ['nconglome', 'conglome', 'vivienda', 'hogar', 'ubigeo',
                    'dominio', 'estrato', 'percepho', 'mieperho', 'totmieho', 'ia01hd', 'ia02hd',
                    'ingbruhd', 'ingnethd', 'pagesphd', 'ingindhd', 'ingauthd',
                    'insedthd', 'insedlhd', 'paesechd', 'ingseihd', 'isecauhd',
                    'ingexthd', 'ingtrahd', 'ingtexhd', 'ingrenhd', 'ingoexhd',
                    'g05hd', 'ig06hd', 'g05hd1', 'ig06hd1', 'g05hd2', 'ig06hd2', 'g05hd3', 'ig06hd3',
                    'g05hd4', 'ig06hd4', 'g05hd5', 'ig06hd5', 'g05hd6', 'ig06hd6', 'g07hd', 'ig08hd',
                    'ig03hd1', 'ig03hd2', 'ig03hd3', 'ig03hd4',
                    'sg23', 'sig24', 'sg25', 'sig26', 'ga03hd',
                    'sg42', 'sg42d', 'sg421',  'sg42d1', 'sg422', 'sg42d2', 'sg423', 'sg42d3',
                    'ingtprhd', 'ingtpuhd', 'ingtpu01', 
                    'gru11hd', 'gru12hd1', 'gru12hd2', 'gru13hd1', 'gru13hd2', 'gru13hd3', 'gru13hd4',
                    'gru14hd', 'gru14hd1', 'gru14hd2', 'gru14hd3', 'gru14hd4', 'gru14hd5', 'gru14hd6',
                    'gru21hd', 'gru22hd1', 'gru22hd2', 'gru23hd1', 'gru23hd2', 'gru23hd3', 'gru24hd',
                    'gru31hd', 'gru32hd1', 'gru32hd2', 'gru33hd1', 'gru33hd2', 'gru33hd3', 'gru34hd', 
                    'gru41hd', 'gru42hd1', 'gru42hd2', 'gru43hd1', 'gru43hd2', 'gru43hd3', 'gru44hd', 
                    'gru51hd', 'gru52hd1', 'gru53hd1', 'gru53hd2', 'gru53hd3', 'gru53hd4', 'gru54hd', 
                    'gru61hd', 'gru62hd1', 'gru62hd2', 'gru63hd1', 'gru63hd2', 'gru63hd3', 'gru64hd', 
                    'gru71hd', 'gru72hd1', 'gru72hd2', 'gru73hd1', 'gru73hd2', 'gru73hd3', 'gru74hd', 
                    'gru81hd', 'gru82hd1', 'gru82hd2', 'gru83hd1', 'gru83hd2', 'gru83hd3', 'gru83hd4',
                    'gru84hd', 'gru84hd1', 'gru85hd1', 'gru86hd1', 'gru86hd2', # 'gru87hd',
                    'ingotrhd', 'gashog1d',
                    'gashog21', 'gashog22', 'gashog23', 'gashog24', 'gashog26', 'gashog25',
                    'ld', 'linpe', 'linea', 'pobreza', 'estrsocial', 'factor07']

        batch_2016 = ['nconglome', 'conglome', 'vivienda', 'hogar', 'ubigeo',
                      'dominio', 'estrato', 'percepho', 'mieperho', 'totmieho', 'ia01hd', 'ia02hd',
                      'ingbruhd', 'ingnethd', 'pagesphd', 'ingindhd', 'ingauthd',
                      'insedthd', 'insedlhd', 'paesechd', 'ingseihd', 'isecauhd',
                      'ingexthd', 'ingtrahd', 'ingtexhd', 'ingrenhd', 'ingoexhd',
                      'g05hd', 'ig06hd',   'g05hd1', 'ig06hd1',   'g05hd2', 'ig06hd2',   'g05hd3', 'ig06hd3',  
                      'g05hd4', 'ig06hd4',   'g05hd5', 'ig06hd5',   'g05hd6', 'ig06hd6',   'g07hd', 'ig08hd',
                      'ig03hd1', 'ig03hd2', 'ig03hd3', 'ig03hd4',
                      'sg23', 'sig24', 'sg25', 'sig26', 'ga03hd',
                      'sg42', 'sg42d', 'sg421',  'sg42d1', 'sg422', 'sg42d2', 'sg423', 'sg42d3',
                      'ingtprhd', 'ingtpuhd', 'ingtpu01', 
                      'gru11hd', 'gru12hd1', 'gru12hd2', 'gru13hd1', 'gru13hd2', 'gru13hd3', 'gru13hd4',
                      'gru14hd', 'gru14hd1', 'gru14hd2', 'gru14hd3', 'gru14hd4', 'gru14hd5', 'gru14hd6',
                      'gru21hd', 'gru22hd1', 'gru22hd2', 'gru23hd1', 'gru23hd2', 'gru23hd3', 'gru24hd',
                      'gru31hd', 'gru32hd1', 'gru32hd2', 'gru33hd1', 'gru33hd2', 'gru33hd3', 'gru34hd', 
                      'gru41hd', 'gru42hd1', 'gru42hd2', 'gru43hd1', 'gru43hd2', 'gru43hd3', 'gru44hd', 
                      'gru51hd', 'gru52hd1', 'gru53hd1', 'gru53hd2', 'gru53hd3', 'gru53hd4', 'gru54hd', 
                      'gru61hd', 'gru62hd1', 'gru62hd2', 'gru63hd1', 'gru63hd2', 'gru63hd3', 'gru64hd', 
                      'gru71hd', 'gru72hd1', 'gru72hd2', 'gru73hd1', 'gru73hd2', 'gru73hd3', 'gru74hd', 
                      'gru81hd', 'gru82hd1', 'gru82hd2', 'gru83hd1', 'gru83hd2', 'gru83hd3', 'gru83hd4',
                      'gru84hd', 'gru84hd1', 'gru85hd1', 'gru86hd1', 'gru86hd2', # 'gru87hd',
                      'ingotrhd', 'gashog1d', 'ld', 'linpe', 'linea', 'pobreza', 'estrsocial', 'factor07']

        batch_2015 = ['conglome', 'vivienda', 'hogar', 'ubigeo',
                      'dominio', 'estrato', 'percepho', 'mieperho', 'totmieho', 'ia01hd', 'ia02hd',
                      'ingbruhd', 'ingnethd', 'pagesphd', 'ingindhd', 'ingauthd',
                      'insedthd', 'insedlhd', 'paesechd', 'ingseihd', 'isecauhd',
                      'ingexthd', 'ingtrahd', 'ingtexhd', 'ingrenhd', 'ingoexhd',
                      'g05hd', 'ig06hd',   'g05hd1', 'ig06hd1',   'g05hd2', 'ig06hd2',   'g05hd3', 'ig06hd3',
                      'g05hd4', 'ig06hd4',   'g05hd5', 'ig06hd5',   'g05hd6', 'ig06hd6',   'g07hd', 'ig08hd',
                      'ig03hd1', 'ig03hd2', 'ig03hd3', 'ig03hd4',
                      'sg23', 'sig24', 'sg25', 'sig26', 'ga03hd',
                      'sg42', 'sg42d', 'sg421',  'sg42d1', 'sg422', 'sg42d2', 'sg423', 'sg42d3',
                      'ingtprhd', 'ingtpuhd', 'ingtpu01', 
                      'gru11hd', 'gru12hd1', 'gru12hd2', 'gru13hd1', 'gru13hd2', 'gru13hd3', 'gru13hd4',
                      'gru14hd', 'gru14hd1', 'gru14hd2', 'gru14hd3', 'gru14hd4', 'gru14hd5', 'gru14hd6',
                      'gru21hd', 'gru22hd1', 'gru22hd2', 'gru23hd1', 'gru23hd2', 'gru23hd3', 'gru24hd',
                      'gru31hd', 'gru32hd1', 'gru32hd2', 'gru33hd1', 'gru33hd2', 'gru33hd3', 'gru34hd', 
                      'gru41hd', 'gru42hd1', 'gru42hd2', 'gru43hd1', 'gru43hd2', 'gru43hd3', 'gru44hd', 
                      'gru51hd', 'gru52hd1', 'gru53hd1', 'gru53hd2', 'gru53hd3', 'gru53hd4', 'gru54hd', 
                      'gru61hd', 'gru62hd1', 'gru62hd2', 'gru63hd1', 'gru63hd2', 'gru63hd3', 'gru64hd', 
                      'gru71hd', 'gru72hd1', 'gru72hd2', 'gru73hd1', 'gru73hd2', 'gru73hd3', 'gru74hd', 
                      'gru81hd', 'gru82hd1', 'gru82hd2', 'gru83hd1', 'gru83hd2', 'gru83hd3', 'gru83hd4',
                      'gru84hd', 'gru84hd1', 'gru85hd1', 'gru86hd1', 'gru86hd2', # 'gru87hd',
                      'ingotrhd', 'gashog1d', 'ld', 'linpe', 'linea', 'pobreza', 'estrsocial', 'factor07']

        # Loading dataframe stata step
        try: 
            df = pd.read_stata(params.get('url'), columns = batch_tt)
        except:
            try: 
                df = pd.read_stata(params.get('url'), columns = batch_2016)
            except:
                df = pd.read_stata(params.get('url'), columns = batch_2015)

        # Excel spreadsheet for replace text to id step
        df_labels = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSla-szgKx1SGwW5YglxN7qyB92eDCpSkBhvfQi0QMIjIIgcp20pTSzC6D5P5uOkwqzev2iLtHSPNIQ/pub?output=xlsx"

        # Getting values of year for the survey
        df["year"] = int(params.get('year'))



        # Replace step 
        df["estrsocial"].replace({
            "a" : 1,
            "b" : 2,
            "c" : 3,
            "d" : 4,
            "e" : 5,
            "rural" : 6
        }, inplace= True)

        df["pobreza"].replace({
            "pobre extremo" : 1,
            "pobre no extremo" : 2,
            "pobreno extremo" : 2,
            "no pobre" : 3,
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
            "de 500,000 a más habitantes" : 1,
            "de 100 000 a 499 999 habitantes" : 2,
            "de 100,000 a 499,999 habitantes" : 2,
            "de 50 000 a 99 999 habitantes" : 3,
            "de 50,000 a 99,999 habitantes" : 3,
            "de 20 000 a 49 999 habitantes" : 4,
            "de 20,000 a 49,999 habitantes" : 4,
            "de 2 000 a 19 999 habitantes" : 5,
            "de 2,000 a 19,999 habitantes" : 5,
            "de 500 a 1 999 habitantes" : 6,
            "de 500 a 1,999 habitantes" : 6,
            "mayor de 100,000 viviendas" : 1,
            "de 20,001 a 100,000 viviendas" : 2,
            "de 10,001 a 20,000 viviendas" : 3,
            "de 4,001 a 10,000 viviendas" : 4,
            "de 401 a 4,000 viviendas" : 5,
            "401 a 4,000 viviendas" : 5,
            "con menos de 401 viviendas" : 6,
            "menos de 401 viviendas" : 6,
            "Área de empadronamiento rural (aer) compuesto" : 7,
            "Área de empadronamiento rural - aer compuesto" : 7,
            "Área de empadronamiento rural (aer) simple" : 8,
            "Área de empadronamiento rural - aer simple" : 8,
            "centros poblados con más de 100,000 viviendas" : 1,
            "centros poblados de 20,001 a 100,000 viviendas" : 2,
            "centros poblados de 10,001 a 20,000 viviendas" : 3,
            "centros poblados de 4,001 a 10,000 viviendas" : 4,
            "centros poblados de 401 a 4,000 viviendas" : 5,
            "centros poblados con menos de 401 viviendas" : 6
        }, inplace= True)

        # Renaming columns to an understandable name (Placeholder)

        # Changing type columns step
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
            "nconglome":    "String",
            "conglome":    "String",
            "vivienda":    "String",
            "hogar":    "String",
            "ubigeo":    "String",
            "dominio":    "UInt8",
            "estrato":    "UInt8",
            "percepho":    "UInt8",
            "mieperho":    "UInt8",
            "totmieho":    "UInt8",
            "ia01hd":    "Float32",
            "ia02hd":    "Float32",
            "ingbruhd":    "Float64",
            "ingnethd":    "Float64",
            "pagesphd":    "UInt32",
            "ingindhd":    "Float64",
            "ingauthd":    "UInt32",
            "insedthd":    "Float64",
            "insedlhd":    "Float64",
            "paesechd":    "UInt16",
            "ingseihd":    "Float64",
            "isecauhd":    "UInt16",
            "ingexthd":    "UInt32",
            "ingtrahd":    "UInt32",
            "ingtexhd":    "UInt32",
            "ingrenhd":    "UInt32",
            "ingoexhd":    "UInt32",
            "g05hd":    "Float64",
            "ig06hd":    "Float32",
            "g05hd1":    "Float32",
            "ig06hd1":    "Float32",
            "g05hd2":    "Float32",
            "ig06hd2":    "Float32",
            "g05hd3":    "Float32",
            "ig06hd3":    "Float32",
            "g05hd4":    "UInt8",
            "ig06hd4":    "UInt8",
            "g05hd5":    "Float32",
            "ig06hd5":    "Float32",
            "g05hd6":    "Float32",
            "ig06hd6":    "Float32",
            "g07hd":    "Float32",
            "ig08hd":    "Float32",
            "ig03hd1":    "UInt32",
            "ig03hd2":    "UInt32",
            "ig03hd3":    "UInt32",
            "ig03hd4":    "UInt32",
            "sg23":    "Float32",
            "sig24":    "Float32",
            "sg25":    "Float32",
            "sig26":    "Float32",
            "ga03hd":    "Float64",
            "sg42":    "Float32",
            "sg42d":    "Float32",
            "sg421":    "Float32",
            "sg42d1":    "Float32",
            "sg422":    "Float32",
            "sg42d2":    "Float32",
            "sg423":    "UInt8",
            "sg42d3":    "UInt8",
            "ingtprhd":    "Float64",
            "ingtpuhd":    "Float64",
            "ingtpu01":    "Float32",
            "gru11hd":    "Float64",
            "gru12hd1":    "Float64",
            "gru12hd2":    "Float32",
            "gru13hd1":    "Float32",
            "gru13hd2":    "Float32",
            "gru13hd3":    "Float32",
            "gru13hd4":    "Float32",
            "gru14hd":    "Float32",
            "gru14hd1":    "Float64",
            "gru14hd2":    "Float32",
            "gru14hd3":    "Float32",
            "gru14hd4":    "Float64",
            "gru14hd5":    "Float32",
            "gru14hd6":    "Float32",
            "gru21hd":    "Float32",
            "gru22hd1":    "Float32",
            "gru22hd2":    "Float32",
            "gru23hd1":    "Float32",
            "gru23hd2":    "Float32",
            "gru23hd3":    "Float32",
            "gru24hd":    "Float32",
            "gru31hd":    "Float64",
            "gru32hd1":    "Float32",
            "gru32hd2":    "Float32",
            "gru33hd1":    "Float32",
            "gru33hd2":    "Float32",
            "gru33hd3":    "Float32",
            "gru34hd":    "Float64",
            "gru41hd":    "Float64",
            "gru42hd1":    "Float32",
            "gru42hd2":    "Float32",
            "gru43hd1":    "Float32",
            "gru43hd2":    "Float32",
            "gru43hd3":    "Float32",
            "gru44hd":    "Float32",
            "gru51hd":    "Float64",
            "gru52hd1":    "UInt16",
            "gru53hd1":    "Float64",
            "gru53hd2":    "Float64",
            "gru53hd3":    "UInt16",
            "gru53hd4":    "UInt16",
            "gru54hd":    "Float32",
            "gru61hd":    "Float64",
            "gru62hd1":    "Float32",
            "gru62hd2":    "Float32",
            "gru63hd1":    "Float64",
            "gru63hd2":    "Float64",
            "gru63hd3":    "Float32",
            "gru64hd":    "Float32",
            "gru71hd":    "Float64",
            "gru72hd1":    "Float32",
            "gru72hd2":    "Float32",
            "gru73hd1":    "Float32",
            "gru73hd2":    "Float64",
            "gru73hd3":    "Float32",
            "gru74hd":    "Float32",
            "gru81hd":    "Float64",
            "gru82hd1":    "Float32",
            "gru82hd2":    "Float32",
            "gru83hd1":    "Float32",
            "gru83hd2":    "Float32",
            "gru83hd3":    "Float32",
            "gru83hd4":    "Float32",
            "gru84hd":    "Float32",
            "gru84hd1":    "Float32",
            "gru85hd1":    "Float32",
            "gru86hd1":    "Float32",
            "gru86hd2":    "Float32",
            "ingotrhd":    "UInt8",
            "gashog1d":    "Float64",
            "gashog21":    "Float64",
            "gashog22":    "Float64",
            "gashog23":    "Float64",
            "gashog24":    "Float64",
            "gashog26":    "Float64",
            "gashog25":    "Float64",
            "ld":    "Float32",
            "linpe":    "Float32",
            "linea":    "Float32",
            "pobreza":    "UInt8",
            "estrsocial":    "UInt8",
            "factor07":    "Float32"

        }

        transform_step = TransformStep()

        load_step = LoadStep(
            "housing_survey_sumaria", db_connector, if_exists="append", pk=["ubigeo", "year"], dtype=dtype,
            nullable_list=[
                "nconglome", "gashog21", "gashog22", "gashog23", "gashog24",
                "gashog25", "gashog26", "estrsocial"
              ]
        )

        return [transform_step, load_step]

if __name__ == "__main__":
    
    data = glob.glob('../../data/enh/*.dta')

    pp = ENHPipeline()
    for year in range(2014, 2018 + 1):
    #for year in range(2018, 2018 + 1):
        pp.run({
            'url': '../../data/enh/sumaria-{}.dta'.format(year),
            'year': year
        })