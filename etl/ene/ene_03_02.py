import pandas as pd
from simpledbf import Dbf5
from bamboo_lib.models import PipelineStep


class Module32Step(PipelineStep):
    def run_step(self, prev, params):

      df, mod2_1, mod2_2, mod4_1 = prev

      file_data = '../../data/ene/03_MÓDULO_IV_2.dbf'

      dbf = Dbf5(file_data, codec='latin-1')
      mod4_2 = dbf.to_dataframe()
      mod4_2.columns = [x.lower() for x in mod4_2.columns]

      # 03_MÓDULO_IV_2
      mod4_2 = mod4_2[[
        'iruc', 'm4p59', 'm4p70_1', 'm4p70_2', 'm4p70_3', 'm4p70_4', 'm4p70_5', 'm4p70_6',
        'm4p70_7', 'm4p70_8', 'm4p70_9', 'm4p70_10', 'm4p70_11', 'm4p70_12', 'm4p70_13',
        'm4p70_14', 'm4p70_15', 'm4p70_15_o'
        ]].copy()

      columns = {
        'iruc': 'company_id',
        'm4p59': 'realiza_ventas_internet_exterior_2014', # options
        'm4p70_1': 'contrato_servicios_asesoria_juridica', # 1,0
        'm4p70_2': 'contrato_servicios_asesoria_economica_financiera', # 1,0
        'm4p70_3': 'contrato_servicios_auditoria_contable_financiera', # 1,0
        'm4p70_4': 'contrato_servicios_contabilidad_asesoria_contable', # 1,0
        'm4p70_5': 'contrato_servicios_soporte_informatico', # 1,0
        'm4p70_6': 'contrato_servicios_mensajeria', # 1,0
        'm4p70_7': 'contrato_servicios_alquiler_maquinaria_mantenimiento', # 1,0
        'm4p70_8': 'contrato_servicios_limpieza', # 1,0
        'm4p70_9': 'contrato_servicios_cobro_clientes', # 1,0
        'm4p70_10': 'contrato_servicios_asistencia_temas_ambientales', # 1,0
        'm4p70_11': 'contrato_servicios_tramites_aduana', # 1,0
        'm4p70_12': 'contrato_servicios_etiquetado_empaquetado_embalaje', # 1,0
        'm4p70_13': 'contrato_servicios_marketing', # 1,0
        'm4p70_14': 'contrato_servicios_seguridad', # 1,0
        'm4p70_15': 'contrato_servicios_otro', # 1,0
        'm4p70_15_o': 'contrato_servicios_otro_respuesta_abierta' # texto
        }

      mod4_2.rename(columns=columns, inplace=True)
      mod4_2['company_id'] = mod4_2['company_id'].astype(int)
      mod4_2['realiza_ventas_internet_exterior_2014'] = mod4_2['realiza_ventas_internet_exterior_2014'].astype(pd.Int16Dtype())
      mod4_2['contrato_servicios_asesoria_juridica'] = mod4_2['contrato_servicios_asesoria_juridica'].astype(pd.Int16Dtype())
      mod4_2['contrato_servicios_asesoria_economica_financiera'] = mod4_2['contrato_servicios_asesoria_economica_financiera'].astype(pd.Int16Dtype())
      mod4_2['contrato_servicios_auditoria_contable_financiera'] = mod4_2['contrato_servicios_auditoria_contable_financiera'].astype(pd.Int16Dtype())
      mod4_2['contrato_servicios_contabilidad_asesoria_contable'] = mod4_2['contrato_servicios_contabilidad_asesoria_contable'].astype(pd.Int16Dtype())
      mod4_2['contrato_servicios_soporte_informatico'] = mod4_2['contrato_servicios_soporte_informatico'].astype(pd.Int16Dtype())
      mod4_2['contrato_servicios_mensajeria'] = mod4_2['contrato_servicios_mensajeria'].astype(pd.Int16Dtype())
      mod4_2['contrato_servicios_alquiler_maquinaria_mantenimiento'] = mod4_2['contrato_servicios_alquiler_maquinaria_mantenimiento'].astype(pd.Int16Dtype())
      mod4_2['contrato_servicios_limpieza'] = mod4_2['contrato_servicios_limpieza'].astype(pd.Int16Dtype())
      mod4_2['contrato_servicios_cobro_clientes'] = mod4_2['contrato_servicios_cobro_clientes'].astype(pd.Int16Dtype())
      mod4_2['contrato_servicios_asistencia_temas_ambientales'] = mod4_2['contrato_servicios_asistencia_temas_ambientales'].astype(pd.Int16Dtype())
      mod4_2['contrato_servicios_tramites_aduana'] = mod4_2['contrato_servicios_tramites_aduana'].astype(pd.Int16Dtype())
      mod4_2['contrato_servicios_etiquetado_empaquetado_embalaje'] = mod4_2['contrato_servicios_etiquetado_empaquetado_embalaje'].astype(pd.Int16Dtype())
      mod4_2['contrato_servicios_marketing'] = mod4_2['contrato_servicios_marketing'].astype(pd.Int16Dtype())
      mod4_2['contrato_servicios_seguridad'] = mod4_2['contrato_servicios_seguridad'].astype(pd.Int16Dtype())
      mod4_2['contrato_servicios_otro'] = mod4_2['contrato_servicios_otro'].astype(pd.Int16Dtype())

      return df, mod2_1, mod2_2, mod4_1, mod4_2
