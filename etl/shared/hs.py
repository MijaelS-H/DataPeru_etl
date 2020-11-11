import pandas as pd
import nltk
from os import path
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from etl.helpers import format_text

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        # Read HS files
        hs_codes = pd.read_excel(path.join(params["datasets"],"anexos", "DataExport_11_2_2019__2_56_41.xlsx"), sheet_name="Aplicados_NMF", header=4, usecols="B,C,E,R", names=["year", "hs_level", "id", "hs_name"], dtype={"hs_level": "int", "id": "str", "hs_name": "str"})
        chapter = pd.read_csv(path.join(params["datasets"],"anexos", "hs_2017.csv"), dtype={"Parent.1": "str", "Code.1": "str"})
        chapter_names = pd.read_excel(path.join(params["datasets"],"anexos", "hs6_2012.xlsx"), sheet_name="chapter", usecols="A,D", names=["chapter_id", "chapter_name"], dtype={"chapter_id": "str"})
        hs_2012 = pd.read_excel(path.join(params["datasets"],"anexos", "hs6_2012.xlsx"), sheet_name="hs6", usecols="A,D,E", names=["id", "hs6_name_large", "hs6_name"], dtype={"id": "str"})

        # Rename columns
        chapter = chapter[chapter["Level"] == 2][["Parent.1", "Code.1"]].rename(columns={"Parent.1": "chapter_id", "Code.1": "hs2_id"})
        hs2 = hs_codes[(hs_codes["hs_level"] == 2) & (hs_codes["year"] == 2017)].rename(columns={"hs_name": "hs2_name"}).copy()
        hs4 = hs_codes[(hs_codes["hs_level"] == 4) & (hs_codes["year"] == 2017)].rename(columns={"hs_name": "hs4_name"}).copy()
        hs6 = hs_codes[(hs_codes["hs_level"] == 6) & (hs_codes["year"] == 2017)].rename(columns={"hs_name": "hs6_name"}).copy() 

        hs2.drop(columns=["year"], inplace=True)
        hs4.drop(columns=["year"], inplace=True)
        hs6.drop(columns=["year"], inplace=True)

        # Replace Chapters codes
        chapter.replace({
            "I": "1",
            "II": "2",
            "III": "3",
            "IV": "4",
            "V": "5",
            "VI": "6",
            "VII": "7",
            "VIII": "8",
            "IX": "9",
            "X": "10",
            "XI": "11",
            "XII": "12",
            "XIII": "13",
            "XIV": "14",
            "XV": "15",
            "XVI": "16",
            "XVII": "17",
            "XVIII": "18",
            "XIX": "19",
            "XX": "20",
            "XXI": "21"
        }, inplace=True)

        # Change HS2 column type
        chapter["hs2_id"] = chapter["hs2_id"].str.zfill(2)

        # Add HS 2012 missing product
        hs_2012['id'] = hs_2012['id'].str[-6:]
        hs_2012['hs_level'] = 6
        hs_2012['hs6_name'] = hs_2012.apply(lambda x: x['hs6_name'] if pd.notnull(x['hs6_name']) else x['hs6_name_large'], axis=1)
        hs_2012 = hs_2012[['hs_level', 'id', 'hs6_name']].copy()
        hs6_list = list(hs6.id.unique())
        hs_2012 = hs_2012[~hs_2012['id'].isin(hs6_list)]
        hs6 = hs6.append(hs_2012)

        missing_products = [
            {
                'hs_level': 6,
                'id': '690890',
                'hs6_name': 'Las demás losas y baldosas de cerámica vidriada para pavimentos, hogares o revestimientos; cubos de mosaicos de cerámica vidriada y artículos similares, incluso con soporte'
            },
            {
                'hs_level': 6,
                'id': '690810',
                'hs6_name': 'Baldosas, cubos y artículos similares, rectangulares o no, cuya mayor superficie pueda encerrarse en un cuadrado cuyo lado sea inferior a 7 cm'
            },
            {
                'hs_level': 6,
                'id': '846900',
                'hs6_name': 'Máquinas de escribir, excepto las impresoras de la partida 8443; maquinas procesadoras de textos'
            }
        ]

        hs6 = hs6.append(missing_products)

        hs2["hs2_id"] = hs2.apply(lambda x: x["id"][0:2], axis=1)
        hs4["hs2_id"] = hs4.apply(lambda x: x["id"][0:2], axis=1)
        hs6["hs2_id"] = hs6.apply(lambda x: x["id"][0:2], axis=1)

        # Join Chapter dataframe to lower categories dataframes
        hs2 = hs2.set_index("hs2_id").join(chapter.set_index("hs2_id")).reset_index()
        hs4 = hs4.set_index("hs2_id").join(chapter.set_index("hs2_id")).reset_index()
        hs6 = hs6.set_index("hs2_id").join(chapter.set_index("hs2_id")).reset_index()

        # Drop columns
        hs2.drop(columns=["hs2_id"], inplace=True)
        hs4.drop(columns=["hs2_id"], inplace=True)
        hs6.drop(columns=["hs2_id"], inplace=True)
        
        # Add internal categories
        hs2 = hs2.append([
            {
                "hs_level": 2,
                "id": "98",
                "hs2_name": "Mercancías con tratamiento especial",
                "chapter_id": 99
            }
        ]).reset_index(drop=True)

        hs4 = hs4.append([
            {
                "hs_level": 4,
                "id": "9801",
                "hs4_name": "Equipaje y menaje de casa",
                "chapter_id": 99
            },
            {
                "hs_level": 4,
                "id": "9802",
                "hs4_name": "Bienes de uso personal o exclusivo del destinatario",
                "chapter_id": 99
            },
            {
                "hs_level": 4,
                "id": "9803",
                "hs4_name": "Muestras comerciales de valor insignificante y materiales de publicidad impresos",
                "chapter_id": 99
            },
            {
                "hs_level": 4,
                "id": "9804",
                "hs4_name": "Mercancías que cuentan con Resolución Liberatoria o Nota Protocolar, excepto vehículos automóviles",
                "chapter_id": 99
            },
            {
                "hs_level": 4,
                "id": "9805",
                "hs4_name": "Mercancías para atender las necesidades de las zonas afectadas por desastres naturales, declaradas en estado de emergencia",
                "chapter_id": 99
            },
            {
                "hs_level": 4,
                "id": "9806",
                "hs4_name": "Mercancías de ayuda humanitaria que ingresen al amparo del artículo 2º de la Ley Nº 29081",
                "chapter_id": 99
            },
            {
                "hs_level": 4,
                "id": "9810",
                "hs4_name": "Envíos postales",
                "chapter_id": 99
            },
            {
                "hs_level": 4,
                "id": "9809",
                "hs4_name": "Envíos de entrega rápida",
                "chapter_id": 99
            },
            {
                "hs_level": 4,
                "id": "9620",
                "hs4_name": "Monopies, Bipods, Trípodes y Artículos Similares",
                "chapter_id": 20
            },
            {
                "hs_level": 4,
                "id": "6908",
                "hs4_name": "Baldosas y baldosas de cerámica esmaltada para pavimentos, hogares o revestimientos cubos de mosaicos de cerámica vidriada y artículos similares, incluso con soporte",
                "chapter_id": 13
            },
            {
                "hs_level": 4,
                "id": "8469",
                "hs4_name": "Máquinas de escribir, excepto las impresoras de la partida 8443; maquinas procesadoras de textos",
                "chapter_id": 16
            }
        ]).reset_index(drop=True)

        hs6 = hs6.append([
            {
                "chapter_id": 99,
                "hs_level": 6,
                "id": "980100",
                "hs6_name": "Equipaje y menaje de casa"
            },
            {
                "chapter_id": 99,
                "hs_level": 6,
                "id": "980200",
                "hs6_name": "Bienes de uso personal o exclusivo del destinatario"
            },
            {
                "chapter_id": 99,
                "hs_level": 6,
                "id": "980300",
                "hs6_name": "Muestras comerciales de valor insignificante y materiales de publicidad impresos"
            },
            {
                "chapter_id": 99,
                "hs_level": 6,
                "id": "980400",
                "hs6_name": "Mercancías que cuentan con Resolución Liberatoria o Nota Protocolar, excepto vehículos automóviles"
            },
            {
                "chapter_id": 99,
                "hs_level": 6,
                "id": "980500",
                "hs6_name": "Mercancías para atender las necesidades de las zonas afectadas por desastres naturales, declaradas en estado de emergencia"
            },
            {
                "chapter_id": 99,
                "hs_level": 6,
                "id": "980600",
                "hs6_name": "Mercancías de ayuda humanitaria que ingresen al amparo del artículo 2º de la Ley Nº 29081"
            },
            {
                "chapter_id": 99,
                "hs_level": 6,
                "id": "981000",
                "hs6_name": "Envíos postales"
            },
            {
                "chapter_id": 99,
                "hs_level": 6,
                "id": "980900",
                "hs6_name": "Envíos de entrega rápida"
            }
        ]).reset_index(drop=True)

        ran = {
            "hs2": 2, 
            "hs4": 4
        }

        for k, v in ran.items():
            ids = hs6["id"].astype("str").str[:v].astype("str")
            hs6["{}_id".format(k)] = ids

        chapter.drop(columns=["hs2_id"], inplace=True)
        chapter.drop_duplicates(inplace=True)

        chapter = chapter.set_index("chapter_id").join(chapter_names.set_index("chapter_id")).reset_index()

        chapter["chapter_name"] = chapter["chapter_name"].str.capitalize()

        chapter = chapter.append([
            {
                "chapter_id": 99,
                "chapter_name": "Complementario Nacional"
            }
        ]).reset_index(drop=True)

        hs2.drop(columns=["hs_level", "chapter_id"], inplace=True)
        hs4.drop(columns=["hs_level", "chapter_id"], inplace=True)
        hs6.drop(columns=["hs_level"], inplace=True)

        hs2.rename(columns={"id": "hs2_id"}, inplace=True)
        hs4.rename(columns={"id": "hs4_id"}, inplace=True)

        # Join Chapters, HS2, HS4 and HS6 dataframes

        ran = {
            "hs2": hs2,
            "hs4": hs4,
            "chapter": chapter
        }

        for k, v in ran.items():
            hs6 = pd.merge(hs6, v, on='{}_id'.format(k))

        hs6.rename(columns={'id': 'hs6_id'}, inplace=True)
        
        hs6 = hs6[["chapter_id", "chapter_name", "hs2_id", "hs2_name", "hs4_id", "hs4_name", "hs6_id", "hs6_name"]].copy()

        text_cols = ["chapter_name", "hs2_name", "hs4_name", "hs6_name"]

        nltk.download('stopwords')
        stopwords_es = nltk.corpus.stopwords.words('spanish')
        df = format_text(hs6, text_cols, stopwords=stopwords_es)

        # Removes dots at the end of category names
        df['hs4_name'] = df['hs4_name'].apply(lambda x: x[0: -1] if x[-1] == '.' else x)
        df['hs6_name'] = df['hs6_name'].apply(lambda x: x[0: -1] if x[-1] == '.' else x)

        df = df.drop_duplicates(subset=['hs6_id'])

        return df

class HSPipeline(EasyPipeline):
    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtype = {
            'chapter_id':       'UInt8',
            'chapter_name':     'String',
            'hs2_id':           'String',
            'hs2_name':         'String',
            'hs4_id':           'String',
            'hs4_name':         'String',
            'hs6_id':           'String',
            'hs6_name':         'String'
        }

        transform_step = TransformStep()
        load_step = LoadStep('dim_shared_hs', db_connector, if_exists='drop', pk=['hs6_id'], dtype=dtype)

        return [transform_step, load_step]

def run_pipeline(params: dict):
    pp = HSPipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
