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
    'Setiembre' : '09',
    'Octubre' : '10',
    'Noviembre' : '11',
    'Diciembre' : '12',
}

ACTIVIDADES_REPLACE_DICT = {
    'insp_tec' : 'Inspecciones Técnicas', 
    'eval_tec': 'Evaluaciones Técnicas', 
    'intervencion': 'Intervención',
    'determ_medidas_emergencia' : 'Determinación de Medidas de Emergencia', 
    'elab_prop_interv' : 'Elaboración de Propuestas de Intervención', 
    'asist_tecnica' : 'Asistencia Técnica',
    'superv_obras' : 'Supervisión de Obras', 
    'elab_prop_reglamentacion' : 'Elaboración de Propuestas de Reglamentación', 
    'entidades' : 'Entidades'
}

TOTALES_REPLACE_DICT = {
    'Total Ninos Nac' : 'Total Niños Nacional',
    'Total Ninos Ext' : 'Total Niños Extranjero',
    'Total Estudiantes Nac' : 'Total Estudiantes Nacional',
    'Total Estudiantes Ext' : 'Total Estudiantes Extranjero', 
    'Total Adultos Nac' : 'Total Adultos Nacional',
    'Total Adultos Ext' : 'Total Adultos Extranjero',
    'Total Adu Bolesp Nac' : 'Total Adultos Nacional', 
    'Total Adu Bolesp Ext' : 'Total Adultos Extranjero',
    'Total Est Bolesp Nac' : 'Total Estudiantes Nacional', 
    'Total Est Bolesp Ext' : 'Total Estudiantes Extranjero', 
    'Total Nin Bolesp Nac' : 'Total Niños Nacional', 
    'Total Nin Bolesp Ext' : 'Total Niños Extranjero',
    'Total Mil Bolesp Nac' : 'Total Militares Nacional',  
    'Total Mil Bolesp Ext' : 'Total Militares Extranjero', 
    'Total Adm Bolesp Nac' : 'Total Adultos Mayores Nacional', 
    'Total Adm Bolesp Ext' : 'Total Adultos Mayores Extranjero', 
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