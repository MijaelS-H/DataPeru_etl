import requests
import pandas as pd 
from pandas.io.json import json_normalize

MONTHS_DICT = {
    'Enero' : '01',
    'Febrero' : '02',
    'Marzo' : '03',
    'Abril' : '04',
    'Mayo' : '05',
    'Junio' : '06',
    'Julio' : '07',
    'Agosto' : '08',
    'Septiembre' : '09',
    'Octubre' : '10',
    'Noviembre' : '11',
    'Diciembre' : '12',
}



def query(indicador, year):
    query = {
        "indicador" : indicador,
        "anio" : year
    }
    r = requests.post('http://infocultura.cultura.pe/infocultura/restservice/', json = query)
    df = pd.DataFrame(r.json()['data'])
    df['anio'] = year
    return df

parameters = ['expresiones','preservacion','sitiosArqVis','vistCompArq','vistTipoPub',
            'certiBienPatri','alertaNacio','alertaLima','musVist','salasVist','vistMusMua',
            'vistMusTip','vistSalTip','vistMus','vistSala','puntCult','asisPreEle','porPreEsp','',
            'preEspMes','benTallInd','puebInd','locPueb','locDep','alerRac']