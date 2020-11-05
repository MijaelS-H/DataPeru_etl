
import os
import time
import json
import pandas as pd
from static import DATA_FOLDER
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

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
urls = {'ING_comparacion_2014_2020_gn_sectores.csv': 'https://datosabiertos.mef.gob.pe/datasets/185602-comparacion-de-ingreso-del-gobierno-nacional-2014-2020.download',
        'ING_comparacion_2014_2020_gob_regionales.csv': 'https://datosabiertos.mef.gob.pe/datasets/185603-comparacion-de-ingreso-de-los-gobiernos-regionales-2014-2020.download',
        'ING_comparacion_2014_2020_gob_locales.csv': 'https://datosabiertos.mef.gob.pe/datasets/185604-comparacion-de-ingreso-de-los-gobiernos-locales-2014-2020.download'}

for file_name, url in urls.items():
    print('current:', url)
    os.system('wget -cO - {} > {}/{}'.format(url, DATA_FOLDER, file_name))
    time.sleep(5)
