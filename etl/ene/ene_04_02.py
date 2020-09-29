import pandas as pd
from simpledbf import Dbf5
from bamboo_lib.models import PipelineStep


class Module42Step(PipelineStep):
    def run_step(self, prev, params):

      # df, mod2_1, mod2_2, mod4_1, mod4_2, mod5_1 = prev
      df, mod2_1, mod2_2 = prev

      file_data = '../../data/ene/04_MÓDULO_V_VI_2.dbf'

      dbf = Dbf5(file_data, codec='latin-1')
      mod5_2 = dbf.to_dataframe()
      mod5_2.columns = [x.lower() for x in mod5_2.columns]

      # 04_MÓDULO_V_VI_2
      mod5_2 = mod5_2[[
        'iruc', 
        # 'm6p15', 
        'm6p19', 
        'm6p24_1', 
        'm6p24_2', 
        'm6p24_3', 
        'm6p24_4',
        'm6p24_5', 
        'm6p24_6', 
        'm6p24_7', 
        'm6p24_8', 
        'm6p24_9', 
        'm6p24_10',
        'm6p24_11', 
        'm6p24_12'
        ]].copy()

      columns = {
        'iruc': 'company_id',
        # 'm6p15': 'utiliza_servicio_correo_postal_2014', # options
        'm6p19': 'realiza_exportaciones_2014', # options
        'm6p24_1': 'dificultad_exportar_falta_informacion_procesos', # 1,0
        'm6p24_2': 'dificultad_exportar_costos_logisticos', # 1,0
        'm6p24_3': 'dificultad_exportar_identificacion_mercados_compradores', # 1,0
        'm6p24_4': 'dificultad_exportar_acceso_financiamiento_operaciones', # 1,0
        'm6p24_5': 'dificultad_exportar_cumplir_normas_requisitos_calidad', # 1,0
        'm6p24_6': 'dificultad_exportar_cumplir_requisitos_cantidad_compradores', # 1,0
        'm6p24_7': 'dificultad_exportar_retrasos_causados_transporte', # 1,0
        'm6p24_8': 'dificultad_exportar_procedimientos_aduaneros', # 1,0
        'm6p24_9': 'dificultad_exportar_retrasos_aduana', # 1,0
        'm6p24_10': 'dificultad_exportar_barreras_arancelarias_extranjero', # 1,0
        'm6p24_11': 'dificultad_exportar_corrupcion_fronteras', # 1,0
        'm6p24_12': 'dificultad_exportar_ninguna' # 1,0
        }

      mod5_2.rename(columns=columns, inplace=True)
      mod5_2['company_id'] = mod5_2['company_id'].astype(int)
      # mod5_2['utiliza_servicio_correo_postal_2014'] = mod5_2['utiliza_servicio_correo_postal_2014'].astype(pd.Int16Dtype())
      mod5_2['realiza_exportaciones_2014'] = mod5_2['realiza_exportaciones_2014'].astype(pd.Int16Dtype())
      mod5_2['dificultad_exportar_falta_informacion_procesos'] = mod5_2['dificultad_exportar_falta_informacion_procesos'].astype(pd.Int16Dtype())
      mod5_2['dificultad_exportar_costos_logisticos'] = mod5_2['dificultad_exportar_costos_logisticos'].astype(pd.Int16Dtype())
      mod5_2['dificultad_exportar_identificacion_mercados_compradores'] = mod5_2['dificultad_exportar_identificacion_mercados_compradores'].astype(pd.Int16Dtype())
      mod5_2['dificultad_exportar_acceso_financiamiento_operaciones'] = mod5_2['dificultad_exportar_acceso_financiamiento_operaciones'].astype(pd.Int16Dtype())
      mod5_2['dificultad_exportar_cumplir_normas_requisitos_calidad'] = mod5_2['dificultad_exportar_cumplir_normas_requisitos_calidad'].astype(pd.Int16Dtype())
      mod5_2['dificultad_exportar_cumplir_requisitos_cantidad_compradores'] = mod5_2['dificultad_exportar_cumplir_requisitos_cantidad_compradores'].astype(pd.Int16Dtype())
      mod5_2['dificultad_exportar_retrasos_causados_transporte'] = mod5_2['dificultad_exportar_retrasos_causados_transporte'].astype(pd.Int16Dtype())
      mod5_2['dificultad_exportar_procedimientos_aduaneros'] = mod5_2['dificultad_exportar_procedimientos_aduaneros'].astype(pd.Int16Dtype())
      mod5_2['dificultad_exportar_retrasos_aduana'] = mod5_2['dificultad_exportar_retrasos_aduana'].astype(pd.Int16Dtype())
      mod5_2['dificultad_exportar_barreras_arancelarias_extranjero'] = mod5_2['dificultad_exportar_barreras_arancelarias_extranjero'].astype(pd.Int16Dtype())
      mod5_2['dificultad_exportar_corrupcion_fronteras'] = mod5_2['dificultad_exportar_corrupcion_fronteras'].astype(pd.Int16Dtype())
      mod5_2['dificultad_exportar_ninguna'] = mod5_2['dificultad_exportar_ninguna'].astype(pd.Int16Dtype())

      # return df, mod2_1, mod2_2, mod4_1, mod4_2, mod5_1, mod5_2
      return df, mod2_1, mod2_2, mod5_2