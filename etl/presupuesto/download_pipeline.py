
import os
import time
import json
import requests
import pandas as pd
from static import DATA_FOLDER

os.makedirs('{}'.format(DATA_FOLDER), exist_ok=True)

API_KEY = os.environ.get('API_KEY')

queries = {'{}/gobierno_nacional.csv'.format(DATA_FOLDER) : 'http://api.datosabiertos.mef.gob.pe/api/v2/datastreams/COMPA-DEL-GOBIE-NACIO-2016/data.json/?auth_key={}&limit=50'.format(API_KEY),
           '{}/gobierno_regional.csv'.format(DATA_FOLDER): 'http://api.datosabiertos.mef.gob.pe/api/v2/datastreams/COMPA-DE-GASTO-DEL-GOBIE/data.json/?auth_key={}&limit=50'.format(API_KEY),
           '{}/gobierno_local.csv'.format(DATA_FOLDER): 'http://api.datosabiertos.mef.gob.pe/api/v2/datastreams/COMPA-DEL-GOBIE-NACIO-53583/data.json/?auth_key={}&limit=50'.format(API_KEY)}

for file_name, query in queries.items():
    print('current:', file_name)
    r = requests.get(query)
    data= r.json()['endpoint']
    df = pd.read_csv(data, encoding='latin-1')
    df.to_csv(file_name, index=False)
    time.sleep(5)


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