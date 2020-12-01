import os
import time
from pathlib import Path
from selenium import webdriver
from etl.helpers import wait_for_downloads

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
    urls = [
        'https://datosabiertos.mef.gob.pe/datasets/185602-comparacion-de-ingreso-del-gobierno-nacional-2014-2020.download',
        'https://datosabiertos.mef.gob.pe/datasets/185603-comparacion-de-ingreso-de-los-gobiernos-regionales-2014-2020.download',
        'https://datosabiertos.mef.gob.pe/datasets/185604-comparacion-de-ingreso-de-los-gobiernos-locales-2014-2020.download'
    ]

    executable_path = os.getcwd().split('/etl/')[0] + '/chromedriver'

    for url in urls:
        try:
            chrome_options = webdriver.ChromeOptions()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--window-size=800, 640")
            chrome_options.add_argument("--disable-notifications")
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--verbose')
            chrome_options.add_experimental_option("prefs", {
                    "download.default_directory": os.path.abspath(download_folder),
                    "download.prompt_for_download": False,
                    "download.directory_upgrade": True,
                    "safebrowsing_for_trusted_sources_enabled": False,
                    "safebrowsing.enabled": False
            })

            driver = webdriver.Chrome(options=chrome_options, executable_path=executable_path)
            driver.get(url)
            wait_for_downloads(os.path.abspath(download_folder))
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
