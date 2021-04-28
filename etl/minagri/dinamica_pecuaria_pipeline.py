from os import path
import shutil
import nltk
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from etl.consistency import AggregatorStep
from etl.helpers import format_text
from etl.helpers import clean_tables

from .dinamica_pecuaria_static import (DTYPES, PRIMARY_KEYS, RENAME_COLUMNS,
                                       REPLACE_VALUES, URL_PECUARIA)


class MoveStep(PipelineStep):
    def run_step(self, prev, params):
        shutil.move(prev, path.join(params.get("datasets"), "downloads", params.get("url")))

        pass

class DownloadPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(name="url", dtype=str),
            Parameter(name="force_download", dtype=bool)
        ]
        
    @staticmethod
    def steps(params):

        download_step = DownloadStep(
            connector="minagri",
            connector_path="etl/minagri/conns.yaml",
            force=params.get("force_download")
        )

        move_step = MoveStep()

        return [download_step, move_step]

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        # Reads 02. DINAMICA PECUARIA file
        df = pd.read_excel(path.join(params.get("datasets"), "downloads", params.get("url")), dtype='str')

        # Rename columns to unique name
        df.rename(columns=RENAME_COLUMNS, inplace=True)

        # Creates and standarizes columns
        df['department_id'] = df['department_id'].str[0:2]
        df['month_id'] = df['year'] + df['month']

        # Replace values in each column
        for item in REPLACE_VALUES:
            df[item].replace(REPLACE_VALUES[item], inplace=True)

        # Creates product dimension table
        dim = df[['producto_name']].copy().drop_duplicates()
        dim["producto_id"] = range(1,len(dim['producto_name'].unique())+1)

        # Merges fact table and dimension table
        df = pd.merge(df, dim, on='producto_name')

        df = df[['producto_id', 'department_id', 'month_id', 'produccion', 'produccion_unidad']].copy()

        df['produccion'] = df['produccion'].astype(float)

        if params.get('level') == 'dimension_table':
            text_cols = ['producto_name']

            nltk.download('stopwords')
            stopwords_es = nltk.corpus.stopwords.words('spanish')
            dim = format_text(dim, text_cols, stopwords=stopwords_es)

            return dim
        
        return df


class MINAGRIPecuariaPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(name='level', dtype=str),
            Parameter(name='table_name', dtype=str),
            Parameter(name='url', dtype=str)
        ]

    @staticmethod
    def steps(params):
        level = params["level"]
        table_name = params["table_name"]
        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))

        transform_step = TransformStep()
        agg_step = AggregatorStep(table_name, measures=["produccion"])
        load_step = LoadStep(table_name, db_connector, if_exists='append', 
                             pk=PRIMARY_KEYS[level], dtype=DTYPES[level],
                             nullable_list=[])

        if level == "fact_table":

            return [transform_step, load_step]

        else:

            return [transform_step, load_step]

def run_pipeline(params: dict):
    clean_tables("dim_shared_dinamica_pecuaria", params.get("connector"))
    clean_tables("minagri_dinamica_pecuaria", params.get("connector"))

    download_pp = DownloadPipeline()
    pp = MINAGRIPecuariaPipeline()

    levels = {
        "dimension_table": "dim_shared_dinamica_pecuaria",
        "fact_table": "minagri_dinamica_pecuaria"
    }

    for url in URL_PECUARIA:
        for k, v in levels.items():
            download_params = pp_params = {"url": url, "force_download": True}
            download_params.update(params)
            download_pp.run(download_params)

            pp_params = {"level": k, "table_name": v, "url": url}
            pp_params.update(params)
            pp.run(pp_params)


if __name__ == "__main__":
    import sys
    from os import path
    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
