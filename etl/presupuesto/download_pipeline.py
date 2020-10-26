
import os
import time
import json
import requests
import pandas as pd
from static import DATA_FOLDER

os.makedirs('{}'.format(DATA_FOLDER), exist_ok=True)

# presupuesto gastos
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

# presupuesto ingresos
urls = ['https://datosabiertos.mef.gob.pe/datasets/185602-comparacion-de-ingreso-del-gobierno-nacional-2014-2020.download',
       'https://datosabiertos.mef.gob.pe/datasets/185603-comparacion-de-ingreso-de-los-gobiernos-regionales-2014-2020.download',
       'https://datosabiertos.mef.gob.pe/datasets/185604-comparacion-de-ingreso-de-los-gobiernos-locales-2014-2020.download']

for url in urls:
    temp = pd.read_csv(url, encoding='latin-1')
    temp.to_csv('{}/{}.csv'.format(DATA_FOLDER, url.split('/')[-1]), index=False)