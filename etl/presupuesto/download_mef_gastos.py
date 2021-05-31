
import os
import csv
import pandas as pd
from zipfile import ZipFile
from io import StringIO
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from .static import URL_GASTO, GASTO_DTYPES_COLS
from etl.helpers import clean_tables

class UnzipStep(PipelineStep):
    def run_step(self, prev, params):

        """" "prev" returns the url path from the downloaded file at previous """

        print('Current file: {}'.format(prev))

        with ZipFile(prev, "r") as data:
            print("Extracting {}".format(params.get("url")))
            data.extractall(os.path.join(params.get("datasets"), "downloads"))

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        """ "prev" returns a list with ZipExtFile -> access the first (and only) file
            -> read the file (bytes) -> decode bytes -> parse to csv """
        #data = StringIO([x for x in prev][0].read().decode())

        df = params.get("chunk")

        df.columns = df.columns.str.lower()

        # drop bad row at the end of the file
        # old_shape = df.shape[0]
        df.dropna(subset=["tipo_gobierno_nombre"], inplace=True)
        # assert old_shape - 1 == df.shape[0], "DROPNA ERROR!"

        df = df[["ano_eje", "mes_eje", "tipo_gobierno", "tipo_gobierno_nombre",
                "division_funcional", "division_funcional_nombre",
                "sector", "sector_nombre", "pliego", "pliego_nombre", "sec_ejec", "ejecutora", "ejecutora_nombre",
                "departamento_ejecutora", "provincia_ejecutora", "distrito_ejecutora",
                "programa_ppto", "programa_ppto_nombre", "producto_proyecto", "producto_proyecto_nombre",
                "funcion", "funcion_nombre", "departamento_meta", "departamento_meta_nombre",
                "monto_pia", "monto_pim", "monto_devengado"]].copy()

        # month_id
        df["month_id"] = (df["ano_eje"].astype(int).astype(str) + \
                          df["mes_eje"].astype(int).astype(str).str.zfill(2)).astype(int)

        # geo id
        df["district_id"] = df["departamento_ejecutora"].astype(int).astype(str).str.zfill(2) + \
                            df["provincia_ejecutora"].astype(int).astype(str).str.zfill(2) + \
                            df["distrito_ejecutora"].astype(int).astype(str).str.zfill(2)

        # pliego
        df["pliego"] = df["pliego"].astype("str")

        # departamento
        df["departamento_meta"] = df["departamento_meta"].astype("str")

        df.drop(columns=["ano_eje", "mes_eje", "departamento_ejecutora",
                         "provincia_ejecutora", "distrito_ejecutora"], inplace=True)

        df["version"] = params.get("url")

        return df

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
            connector="mef",
            connector_path="etl/presupuesto/conns.yaml",
            force=params.get("force_download")
        )

        unzip_step = UnzipStep()

        return [download_step, unzip_step]

class TempGastosPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open(params.get("connector")))

        dtypes = GASTO_DTYPES_COLS

        transform_step = TransformStep()

        load_step = LoadStep("temp_mef_gastos", db_connector, if_exists="append", pk=["district_id"], dtype=dtypes)

        return(transform_step, load_step)

def run_pipeline(params: dict):
    # drop table before run all
    clean_tables("temp_mef_gastos", params.get("connector"))

    pp = DownloadPipeline()
    pp2 = TempGastosPipeline()

    for url in URL_GASTO[:-2]:
        remaining_download_tries = 5
        while remaining_download_tries > 0:
            try:
                pp_params = {"url": url, "force_download": True}
                pp_params.update(params)
                pp.run(pp_params)
        
                data = os.path.join(params.get("datasets"), "downloads", "{}.csv".format(url[:-4]))

                print("Ingesting {}".format(data))

                for chunk in pd.read_csv(data, iterator=True, chunksize=10**4):
                    pp2_params = {"chunk": chunk, "url": url}
                    pp2_params.update(params)
                    pp2.run(
                    pp2_params
                    )

                print("Removing {}".format(data))

                os.remove(data)
                remaining_download_tries = 0
                break

            except Exception as e:
                print("Error downloading {} file. Attempt {}/5".format(url, 6 - remaining_download_tries))
                print("Error: {}".format(e))
                remaining_download_tries = remaining_download_tries - 1
                continue

    # force download on the last 2 files
    for url in URL_GASTO[-2:]:
        remaining_download_tries = 5
        while remaining_download_tries > 0:
            try:
                pp_params = {"url": url, "force_download": True}
                pp_params.update(params)
                pp.run(pp_params)
        
                data = os.path.join(params.get("datasets"), "downloads", "{}.csv".format(url[:-4]))

                print("Ingesting {}".format(data))

                for chunk in pd.read_csv(data, iterator=True, chunksize=10**4):
                    pp2_params = {"chunk": chunk, "url": url}
                    pp2_params.update(params)
                    pp2.run(
                    pp2_params
                    )

                print("Removing {}".format(data))

                os.remove(data)
                remaining_download_tries = 0
                break

            except Exception as e:
                print("Error downloading {} file. Attempt {}/5".format(url, 6 - remaining_download_tries))
                print("Error: {}".format(e))
                remaining_download_tries = remaining_download_tries - 1
                continue

if __name__ == "__main__":
    import sys
    from os import path
    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })