import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep

COUNTRIES = [
    {"iso2": "xx",
    "iso3": "xxa",
    "country_name": "Unknown",
    "country_name_es": "Otros",
    "continent_id": "x",
    "continent": "Unknown",
    "continent_es": "Otros",
    "id_num": "899",
    "oecd": 0},

    {"iso2": "bw",
    "iso3": "bwa",
    "country_name": "Botswana",
    "country_name_es": "Botswana",
    "continent_id": "af",
    "continent": "Africa",
    "continent_es": "África",
    "id_num": "72",
    "oecd": 0},

    {"iso2": "vi",
    "iso3": "vir",
    "country_name": "Virgin Islands",
    "country_name_es": "Islas Vírgenes",
    "continent_id": "na",
    "continent": "North America",
    "continent_es": "América del Norte",
    "id_num": "850",
    "oecd": 0},

    {"iso2": "sz",
    "iso3": "swz",
    "country_name": "Eswatini",
    "country_name_es": "Esuatini",
    "continent_id": "af",
    "continent": "Africa",
    "continent_es": "África",
    "id_num": "748",
    "oecd": 0},

    {"iso2": "fo",
    "iso3": "fro",
    "country_name": "Faroe Islands",
    "country_name_es": "Islas Feroe",
    "continent_id": "eu",
    "continent": "Europe",
    "continent_es": "Europa",
    "id_num": "234",
    "oecd": 0},

    {"iso2": "pr",
    "iso3": "pri",
    "country_name": "Puerto Rico",
    "country_name_es": "Puerto Rico",
    "continent_id": "na",
    "continent": "North America",
    "continent_es": "América del Norte",
    "id_num": "630",
    "oecd": 0},

    {"iso2": "na",
    "iso3": "nam",
    "country_name": "Namibia",
    "country_name_es": "Namibia",
    "continent_id": "af",
    "continent": "Africa",
    "continent_es": "África",
    "id_num": "516",
    "oecd": 0},

    {"iso2": "mc",
    "iso3": "mco",
    "country_name": "Monaco",
    "country_name_es": "Mónaco",
    "continent_id": "eu",
    "continent": "Europe",
    "continent_es": "Europa",
    "id_num": "492",
    "oecd": 0},

    {"iso2": "li",
    "iso3": "lie",
    "country_name": "Liechtenstein",
    "country_name_es": "Liechtenstein",
    "continent_id": "eu",
    "continent": "Europe",
    "continent_es": "Europa",
    "id_num": "438|1411	",
    "oecd": 0},

    {"iso2": "ls",
    "iso3": "lso",
    "country_name": "Lesotho",
    "country_name_es": "Lesotho",
    "continent_id": "af",
    "continent": "Africa",
    "continent_es": "África",
    "id_num": "426",
    "oecd": 0},

    {"iso2": "re",
    "iso3": "reu",
    "country_name": "Reunion",
    "country_name_es": "Réunion",
    "continent_id": "af",
    "continent": "Africa",
    "continent_es": "África",
    "id_num": "638",
    "oecd": 0},

    {"iso2": "im",
    "iso3": "imn",
    "country_name": "Isle of Man",
    "country_name_es": "Isla de Man",
    "continent_id": "eu",
    "continent": "Europe",
    "continent_es": "Europa",
    "id_num": "833",
    "oecd": 0},

    {"iso2": None,
    "iso3": "chi",
    "country_name": "Channel Islands",
    "country_name_es": "Islas del Canal",
    "continent_id": "eu",
    "continent": "Europe",
    "continent_es": "Europa",
    "id_num": "",
    "oecd": 0},

    {"iso2": "1w",
    "iso3": "wld",
    "country_name": "World",
    "country_name_es": "Mundo",
    "continent_id": "xx",
    "continent": None,
    "continent_es": None,
    "id_num": None,
    "oecd": 0},

    {"iso2": "1d",
    "iso3": "1dx",
    "country_name": "International Waters",
    "country_name_es": "Aguas Internacionales",
    "continent_id": "xx",
    "continent": "Unknown",
    "continent_es": "Otros",
    "id_num": None,
    "oecd": 0},

    {"iso2": "1b",
    "iso3": "1bx",
    "country_name": "Perú's Free Zone",
    "country_name_es": "Zona Franca del Perú",
    "continent_id": "sa",
    "continent": "South America",
    "continent_es": "América del Sur",
    "id_num": None,
    "oecd": 0},

    {"iso2": "aq",
    "iso3": "ata",
    "country_name": "Antarctica",
    "country_name_es": "Antártida",
    "continent_id": "an",
    "continent": "Antarctica",
    "continent_es": "Antártida",
    "id_num": None,
    "oecd": 0},

    {"iso2": "xc",
    "iso3": "xca",
    "country_name": "Others Central America",
    "country_name_es": "Otros Centroamérica",
    "continent_id": "na",
    "continent": "North America",
    "continent_es": "América del Norte",
    "id_num": None,
    "oecd": 0},
    
    {"iso2": "xs",
    "iso3": "xsa",
    "country_name": "Others South America",
    "country_name_es": "Otros América del Sur",
    "continent_id": "sa",
    "continent": "South America",
    "continent_es": "América del Sur",
    "id_num": None,
    "oecd": 0},

    {"iso2": "xn",
    "iso3": "xna",
    "country_name": "Others North America",
    "country_name_es": "Otros América del Norte",
    "continent_id": "na",
    "continent": "North America",
    "continent_es": "América del Norte",
    "id_num": None,
    "oecd": 0},

    {"iso2": "xa",
    "iso3": "xas",
    "country_name": "Others Asia",
    "country_name_es": "Otros Asia",
    "continent_id": "as",
    "continent": "Asia",
    "continent_es": "Asia",
    "id_num": None,
    "oecd": 0},

    {"iso2": "xf",
    "iso3": "xaf",
    "country_name": "Others Africa",
    "country_name_es": "Otros África",
    "continent_id": "af",
    "continent": "Africa",
    "continent_es": "África",
    "id_num": None,
    "oecd": 0},

    {"iso2": "xo",
    "iso3": "xoc",
    "country_name": "Others Oceania",
    "country_name_es": "Otros Oceanía",
    "continent_id": "oc",
    "continent": "Oceania",
    "continent_es": "Oceanía",
    "id_num": None,
    "oecd": 0},

    {"iso2": "xe",
    "iso3": "xeu",
    "country_name": "Others Europe",
    "country_name_es": "Otros Europa",
    "continent_id": "eu",
    "continent": "Europe",
    "continent_es": "Europa",
    "id_num": None,
    "oecd": 0},
]

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        # read data
        # translations
        url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRTAtN97MAri4ZgYyYQcWR_OO8iFbfopAwQhCtdqfb1yxnvo0y_yVc4qLCA0Z-0heKzX-7nWUuT24FV/pub?output=xlsx'
        df = pd.read_excel(url, sheet_name='translations')

        df = df.loc[(df.lang == 'es'), ['origin_id', 'lang', 'name']].copy()
        translations = df.copy()

        # countries
        df = pd.read_excel(url, sheet_name='Country Groupings')
        countries = pd.read_excel(url, sheet_name='countries')

        df.columns = df.columns.str.lower()

        df = df[['id', 'country', 'continent', 'oecd']].copy()

        df.rename(columns={'continent': 'continent_id', 'country': 'country_name', 'id': 'iso3'}, inplace=True)

        df['continent'] = df['continent_id']
        df['continent_es'] = df['continent_id']
        df['iso2'] = df['iso3']
        df['id_num'] = df['iso3']

        continents = {
            'af': 'Africa',
            'na': 'North America',
            'oc': 'Oceania',
            'an': 'Antarctica',
            'as': 'Asia',
            'eu': 'Europe',
            'sa': 'South America'
        }

        continents_es = {
            'af': 'África',
            'na': 'América del Norte',
            'oc': 'Oceanía',
            'an': 'Antártida',
            'as': 'Asia',
            'eu': 'Europa',
            'sa': 'América del Sur'
        }

        df['continent'].replace(continents, inplace=True)
        df['continent_es'].replace(continents_es, inplace=True)
        df['iso2'].replace(dict(zip(countries['id_3char'], countries['id_2char'])), inplace=True)
        df['id_num'].replace(dict(zip(countries['id_3char'], countries['id_num'])), inplace=True)
        df['id_num'] = df['id_num'].astype(str)

        # name es
        df['country_name_es'] = df['continent_id'] + df['iso3']
        df['country_name_es'].replace(dict(zip(translations['origin_id'], translations['name'])), inplace=True)

        df = df.append(COUNTRIES, ignore_index=True)

        df = df.drop_duplicates(subset=['iso3'])

        return df

class CountryPipeline(EasyPipeline):
    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtype = {
            'iso2':                 'String',
            'iso3':                 'String',
            'country_name':         'String',
            'country_name_es':      'String',
            'continent_id':         'String',
            'continent':            'String',
            'continent_es':         'String',
            'oecd':                 'UInt8'
        }

        transform_step = TransformStep()
        load_step = LoadStep('dim_shared_country', db_connector, if_exists='drop', pk=['iso3', 'continent_id'],
                            dtype=dtype, engine='ReplacingMergeTree',
                            nullable_list=['iso2', 'id_num', 'continent', 'continent_es'])

        return [transform_step, load_step]

def run_pipeline(params: dict):
    pp = CountryPipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
