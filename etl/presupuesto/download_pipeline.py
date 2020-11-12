import os
import time
from pathlib import Path

import pandas as pd
import requests
from bamboo_lib.logger import logger


def download_gastos(download_folder: Path):
    """Descarga datos del presupuesto de gastos."""
    df = pd.read_excel('https://docs.google.com/spreadsheets/d/e/2PACX-1vQTPqvMRgIPRjVJs4CGMbjbrQYtfxNgFRpTDtsUMNJXieR1VYd6cJ-O9TcGCsj2Vg/pub?output=xlsx')
    df.dropna(how='all', inplace=True)
    df.columns = ['source', 'sector', 'url']
    df['url'] = 'http://www.mef.gob.pe/datos_abiertos/CSV_Datos_Abiertos/' + df['url']

    now = time.time()

    for item in df.to_dict(orient='records'):
        url = item['url'].rstrip("/")
        filename = os.path.basename(url)
        if not filename.endswith(".csv"):
            logger.warning("Malformed target URL, not a CSV file: %s", url)

        target_path = download_folder.joinpath(filename)
        if target_path.exists() and target_path.is_file():
            file_stat = target_path.stat()
            if now - file_stat.st_mtime < 86400: # in the last 24 hours
                logger.debug("DOWNLOAD SKIPPED: %s %s", item['source'], url)
                continue

        try:
            temp = pd.read_csv(url, encoding='latin-1')
            temp.to_csv(target_path, index=False)
            logger.debug("DOWNLOAD SUCCESS: %s %s", item['source'], url)
        except Exception as err:
            logger.error("DOWNLOAD ERROR: %s\n  %s %s", err, item['source'], url)


def download_ingresos(download_folder: Path):
    """Descarga datos del presupuesto de ingresos."""
    urls = {
        'ING_comparacion_2014_2020_gn_sectores.csv': 'https://datosabiertos.mef.gob.pe/datasets/185602-comparacion-de-ingreso-del-gobierno-nacional-2014-2020.download',
        'ING_comparacion_2014_2020_gob_regionales.csv': 'https://datosabiertos.mef.gob.pe/datasets/185603-comparacion-de-ingreso-de-los-gobiernos-regionales-2014-2020.download',
        'ING_comparacion_2014_2020_gob_locales.csv': 'https://datosabiertos.mef.gob.pe/datasets/185604-comparacion-de-ingreso-de-los-gobiernos-locales-2014-2020.download'
    }

    for filename, url in urls.items():
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0"}
        res = requests.get(url, headers=headers)
        try:
            res.raise_for_status()
            target_path = download_folder.joinpath(filename)
            with target_path.open('w+b') as fio:
                fio.write(res.content)
            logger.debug("DOWNLOAD SUCCESS: %s", url)
        except Exception as err:
            logger.error("DOWNLOAD ERROR: %s\n  %s", err, url)


def run_pipeline(params: dict):
    download_folder = Path(params["datasets"]).joinpath("download")
    download_folder.mkdir(exist_ok=True)

    download_ingresos(download_folder)
    download_gastos(download_folder)


if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline({
        "datasets": sys.argv[1],
    })
