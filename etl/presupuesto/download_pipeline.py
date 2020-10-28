
import os
import time
import json
import pandas as pd
from static import DATA_FOLDER
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from etl.helpers import downloads_done

os.makedirs('{}'.format(DATA_FOLDER), exist_ok=True)

# Presupuesto gastos
df = pd.read_excel('https://docs.google.com/spreadsheets/d/e/2PACX-1vQTPqvMRgIPRjVJs4CGMbjbrQYtfxNgFRpTDtsUMNJXieR1VYd6cJ-O9TcGCsj2Vg/pub?output=xlsx')
df.dropna(how='all', inplace=True)
df.columns = ['source', 'sector', 'url']
df['url'] = 'http://www.mef.gob.pe/datos_abiertos/CSV_Datos_Abiertos/' + df['url']

errors = []
temp = pd.DataFrame()
for ele in df.to_dict(orient='records'):
    try: 
        temp = pd.read_csv(ele['url'], encoding='latin-1')
        temp.to_csv('{}/{}'.format(DATA_FOLDER, ele['url'].split('/')[-1]), index=False)
        print('current:', ele['source'], ele['url'])
    except:
        errors.append(ele['url'])
        print('ERROR:', ele['source'], ele['url'])
    time.sleep(5)


# Presupuesto ingresos
urls = ['https://datosabiertos.mef.gob.pe/datasets/185602-comparacion-de-ingreso-del-gobierno-nacional-2014-2020.download',
       'https://datosabiertos.mef.gob.pe/datasets/185603-comparacion-de-ingreso-de-los-gobiernos-regionales-2014-2020.download',
       'https://datosabiertos.mef.gob.pe/datasets/185604-comparacion-de-ingreso-de-los-gobiernos-locales-2014-2020.download']

executable_path = os.getcwd().split('/etl/')[0] + '/chromedriver'

for ele in urls:
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--window-size=1440, 900")
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--verbose')
    chrome_options.add_experimental_option("prefs", {
            "download.default_directory": os.getcwd().split('dataperu-etl/')[0] + DATA_FOLDER.split('../../../')[1],
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing_for_trusted_sources_enabled": False,
            "safebrowsing.enabled": False
    })

    driver = webdriver.Chrome(options=chrome_options, executable_path=executable_path)
    driver.get(ele)

    print('Waiting for download to finish: {}'.format(ele))
    downloads_done(DATA_FOLDER)