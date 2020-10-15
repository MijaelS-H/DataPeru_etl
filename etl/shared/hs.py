import pandas as pd
import nltk
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
        hs_codes = pd.read_excel('../../../datasets/anexos/DataExport_11_2_2019__2_56_41.xlsx', sheet_name="Aplicados_NMF", header=4, usecols="B,C,E,R", names=["year", "hs_level", "id", "hs_name"], dtype={"hs_level": "int", "id": "str", "hs_name": "str"})
        chapter = pd.read_csv('../../../datasets/anexos/hs_2017.csv', dtype={"Parent.1": "str", "Code.1": "str"})
        chapter_names = pd.read_excel('../../../datasets/anexos/hs6_2012.xlsx', sheet_name="chapter", usecols="A,D", names=["chapter_id", "chapter_name"], dtype={"chapter_id": "str"})

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
        chapter["hs2_id"] = chapter["hs2_id"].astype('int')

        hs2["hs2_id"] = hs2.apply(lambda x: int(x["id"][0:2]), axis=1)
        hs4["hs2_id"] = hs4.apply(lambda x: int(x["id"][0:2]), axis=1)
        hs6["hs2_id"] = hs6.apply(lambda x: int(x["id"][0:2]), axis=1)

        # Join Chapter dataframe to lower categories dataframes
        hs2 = hs2.set_index("hs2_id").join(chapter.set_index("hs2_id")).reset_index()
        hs4 = hs4.set_index("hs2_id").join(chapter.set_index("hs2_id")).reset_index()
        hs6 = hs6.set_index("hs2_id").join(chapter.set_index("hs2_id")).reset_index()

        # Drop columns
        hs2.drop(columns=["hs2_id"], inplace=True)
        hs4.drop(columns=["hs2_id"], inplace=True)
        hs6.drop(columns=["hs2_id"], inplace=True)

        hs2["id"] = hs2["chapter_id"].astype("str") + hs2["id"]
        hs4["id"] = hs4["chapter_id"].astype("str") + hs4["id"]
        hs6["id"] = hs6["chapter_id"].astype("str") + hs6["id"]
        
        # Add internal categories

        hs2 = hs2.append([
            {
                "hs_level": 2,
                "id": "9998",
                "hs2_name": "Mercancías con tratamiento especial",
                "chapter_id": "99"
            }
        ]).reset_index(drop=True)

        hs4 = hs4.append([
            {
                "hs_level": 4,
                "id": "999801",
                "hs4_name": "Equipaje y menaje de casa",
                "chapter_id": "99"
            },
            {
                "hs_level": 4,
                "id": "999802",
                "hs4_name": "Bienes de uso personal o exclusivo del destinatario",
                "chapter_id": "99"
            },
            {
                "hs_level": 4,
                "id": "999803",
                "hs4_name": "Muestras comerciales de valor insignificante y materiales de publicidad impresos",
                "chapter_id": "99"
            },
            {
                "hs_level": 4,
                "id": "999804",
                "hs4_name": "Mercancías que cuentan con Resolución Liberatoria o Nota Protocolar, excepto vehículos automóviles",
                "chapter_id": "99"
            },
            {
                "hs_level": 4,
                "id": "999805",
                "hs4_name": "Mercancías para atender las necesidades de las zonas afectadas por desastres naturales, declaradas en estado de emergencia",
                "chapter_id": "99"
            },
            {
                "hs_level": 4,
                "id": "999806",
                "hs4_name": "Mercancías de ayuda humanitaria que ingresen al amparo del artículo 2º de la Ley Nº 29081",
                "chapter_id": "99"
            },
            {
                "hs_level": 4,
                "id": "999810",
                "hs4_name": "Envíos postales",
                "chapter_id": "99"
            },
            {
                "hs_level": 4,
                "id": "999809",
                "hs4_name": "Envíos de entrega rápida",
                "chapter_id": "99"
            },
            {
                "hs_level": 4,
                "id": "209620",
                "hs4_name": "Monopies, Bipods, Trípodes y Artículos Similares",
                "chapter_id": "20"
            }
        ]).reset_index(drop=True)

        hs6 = hs6.append([
            {
                "chapter_id": 99,
                "hs_level": 6,
                "id": "99980100",
                "hs6_name": "Equipaje y menaje de casa"
            },
            {
                "chapter_id": 99,
                "hs_level": 6,
                "id": "99980200",
                "hs6_name": "Bienes de uso personal o exclusivo del destinatario"
            },
            {
                "chapter_id": 99,
                "hs_level": 6,
                "id": "99980300",
                "hs6_name": "Muestras comerciales de valor insignificante y materiales de publicidad impresos"
            },
            {
                "chapter_id": 99,
                "hs_level": 6,
                "id": "99980400",
                "hs6_name": "Mercancías que cuentan con Resolución Liberatoria o Nota Protocolar, excepto vehículos automóviles"
            },
            {
                "chapter_id": 99,
                "hs_level": 6,
                "id": "99980500",
                "hs6_name": "Mercancías para atender las necesidades de las zonas afectadas por desastres naturales, declaradas en estado de emergencia"
            },
            {
                "chapter_id": 99,
                "hs_level": 6,
                "id": "99980600",
                "hs6_name": "Mercancías de ayuda humanitaria que ingresen al amparo del artículo 2º de la Ley Nº 29081"
            },
            {
                "chapter_id": 99,
                "hs_level": 6,
                "id": "99981000",
                "hs6_name": "Envíos postales"
            },
            {
                "chapter_id": 99,
                "hs_level": 6,
                "id": "99980900",
                "hs6_name": "Envíos de entrega rápida"
            }
        ]).reset_index(drop=True)

        ran = {
            "hs2": 4, 
            "hs4": 6, 
            "chapter": 2
        }

        for k, v in ran.items():
            ids = hs6["id"].astype("str").str.zfill(8).str[:v].astype("int")
            hs6["{}_id".format(k)] = ids

        chapter.drop(columns=["hs2_id"], inplace=True)
        chapter.drop_duplicates(inplace=True)

        chapter = chapter.set_index("chapter_id").join(chapter_names.set_index("chapter_id")).reset_index()

        chapter["chapter_name"] = chapter["chapter_name"].str.capitalize()

        chapter = chapter.append([
            {
                "chapter_id": "99",
                "chapter_name": "Complementario Nacional"
            }
        ]).reset_index(drop=True)

        hs2.drop(columns=["hs_level", "chapter_id"], inplace=True)
        hs4.drop(columns=["hs_level", "chapter_id"], inplace=True)
        hs6.drop(columns=["hs_level"], inplace=True)

        chapter["chapter_id"] = chapter["chapter_id"].astype("int")
        hs2["id"] = hs2["id"].astype("int")
        hs4["id"] = hs4["id"].astype("int")

        hs2.rename(columns={"id": "hs2_id"}, inplace=True)
        hs4.rename(columns={"id": "hs4_id"}, inplace=True)

        # Join Chapters, HS2, HS4 and HS6 dataframes

        ran = {
            "hs2": hs2, 
            "hs4": hs4,            
            "chapter": chapter
        }

        for k, v in ran.items():
            base = "{}_name".format(k)
            hs6[base] = hs6.set_index("{}_id".format(k)).join(v.set_index("{}_id".format(k))).reset_index()["{}_name".format(k)]

        hs6.rename(columns={'id': 'hs6_id'}, inplace=True)
        
        hs6 = hs6[["chapter_id", "chapter_name", "hs2_id", "hs2_name", "hs4_id", "hs4_name", "hs6_id", "hs6_name"]].copy()

        hs6.drop_duplicates(subset=["hs6_id"], inplace=True)

        text_cols = ["chapter_name", "hs2_name", "hs4_name", "hs6_name"]

        nltk.download('stopwords')
        stopwords_es = nltk.corpus.stopwords.words('spanish')
        df = format_text(hs6, text_cols, stopwords=stopwords_es)

        # Removes dots at the end of category names
        df['hs4_name'] = df['hs4_name'].apply(lambda x: x[0: -1] if x[-1] == '.' else x)
        df['hs6_name'] = df['hs6_name'].apply(lambda x: x[0: -1] if x[-1] == '.' else x)

        return df

class HSPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'chapter_id':       'UInt8',
            'chapter_name':     'String',    
            'hs2_id':           'UInt16',          
            'hs2_name':         'String',        
            'hs4_id':           'UInt32',          
            'hs4_name':         'String',        
            'hs6_id':           'UInt32',          
            'hs6_name':         'String'        
        }

        transform_step = TransformStep()
        load_step = LoadStep('dim_shared_hs', db_connector, if_exists='drop', pk=['hs6_id'], 
            dtype=dtype, engine='ReplacingMergeTree', 
            nullable_list=[]
        )

        return [transform_step, load_step]

if __name__ == '__main__':
    pp = HSPipeline()
    pp.run({})
    