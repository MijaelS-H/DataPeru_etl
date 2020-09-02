import pandas as pd
from simpledbf import Dbf5
from bamboo_lib.models import PipelineStep


class Module41Step(PipelineStep):
    def run_step(self, prev, params):

      df, mod2_1, mod2_2, mod4_1, mod4_2 = prev

      file_data = '../../data/ene/04_MÓDULO_V_VI_1.dbf'

      dbf = Dbf5(file_data, codec='latin-1')
      mod5_1 = dbf.to_dataframe()
      mod5_1.columns = [x.lower() for x in mod5_1.columns]

      # 04_MÓDULO_V_VI_1
      mod5_1 = mod5_1[[
        'iruc', 'm5p1_1a', 'm5p1_1b', 'm5p1_1c', 'm5p1_2a', 'm5p1_2b', 'm5p1_2c', 
        'm5p1_3a', 'm5p1_3b', 'm5p1_3c', 'm5p1_4a', 'm5p1_4b', 'm5p1_4c', 'm5p1_5a',
        'm5p1_5b', 'm5p1_5c', 'm5p1_6a', 'm5p1_6b', 'm5p1_6c', 'm5p1_7a', 'm5p1_7_o',
        'm5p1_7b', 'm5p1_7c', 'm5p2_1', 'm5p2_2', 'm5p2_3', 'm5p3_1', 'm5p3_2', 'm5p3_3',
        'm5p3_4', 'm5p3_5', 'm5p3_6', 'm5p3_7', 'm5p3_8', 'm5p3_8_o', 'm5p3_9', 'm5p4',
        'm5p8', 'm5p9_1', 'm5p9_2', 'm5p9_3', 'm5p9_4', 'm5p9_5', 'm5p9_6', 'm5p9_7',
        'm5p9_8', 'm5p9_9', 'm5p9_10', 'm5p9_11', 'm5p9_12', 'm5p9_13', 'm5p9_14',
        'm5p9_15', 'm5p12_1', 'm5p12_2', 'm5p12_3', 'm5p12_4', 'm5p12_5', 'm5p13_1',
        'm5p13_2', 'm5p13_3', 'm5p13_4', 'm5p17'
        ]].copy()

      columns = {
        'iruc': 'company_id',
        'm5p1_1a': 'dispone_computadora_escritorio', # options
        'm5p1_1b': 'cantidad_computadora_escritorio', # measure
        'm5p1_1c': 'antiguedad_computadora_escritorio', # options
        'm5p1_2a': 'dispone_computadora_portatil', # options
        'm5p1_2b': 'cantidad_computadora_portatil', # measure
        'm5p1_2c': 'antiguedad_computadora_portatil', # options
        'm5p1_3a': 'dispone_multifuncional', # options
        'm5p1_3b': 'cantidad_multifuncional', # measure
        'm5p1_3c': 'antiguedad_multifuncional', # options
        'm5p1_4a': 'dispone_impresora', # options
        'm5p1_4b': 'cantidad_impresora', # measure
        'm5p1_4c': 'antiguedad_impresora',# options
        'm5p1_5a': 'dispone_escaner', # options
        'm5p1_5b': 'cantidad_escaner', # measure
        'm5p1_5c': 'antiguedad_escaner',# options
        'm5p1_6a': 'dispone_smartphone', # options
        'm5p1_6b': 'cantidad_smartphone', # measure
        'm5p1_6c': 'antiguedad_smartphone', # options
        'm5p1_7a': 'otro_dispone', # options
        'm5p1_7_o': 'otro_dispone_especifique', # texto
        'm5p1_7b': 'cantidad_otro_dispone', # measure
        'm5p1_7c': 'antiguedad_otro_dispone', # options
        'm5p2_1': 'utiliza_pagina_web', # options
        'm5p2_2': 'utiliza_redes_sociales', # options
        'm5p2_3': 'utiliza_linkedin', # options
        'm5p3_1': 'tiene_software_contable', # 1,0
        'm5p3_2': 'tiene_software_ventas', # 1,0
        'm5p3_3': 'tiene_software_personal', # 1,0 RRHH?
        'm5p3_4': 'tiene_software_finanzas', # 1,0
        'm5p3_5': 'tiene_software_logistica', # 1,0
        'm5p3_6': 'tiene_software_produccion', # 1,0
        'm5p3_7': 'tiene_software_soporte_informatico', # 1,0
        'm5p3_8': 'tiene_software_otro', # 1,0
        'm5p3_8_o': 'tiene_software_otro_especifique', # 1,0
        'm5p3_9': 'tiene_software_ninguno', # 1,0
        'm5p4': 'porcentaje_trabajadores_utiliza_computadoras', # measure
        'm5p8': 'porcentaje_trabajadores_utiliza_servicio_internet', # measure
        'm5p9_1': 'utiliza_internet_para_busqueda_productos_servicios', # 1,0
        'm5p9_2': 'utiliza_internet_para_busqueda_organismos_gubernamentales', # 1,0
        'm5p9_3': 'utiliza_internet_para_busqueda_informacion_investigacion_desarrollo', # 1,0
        'm5p9_4': 'utiliza_internet_para_otra_busqueda_informacion', # 1,0
        'm5p9_5': 'utiliza_internet_para_comunicaciones', # 1,0
        'm5p9_6': 'utiliza_internet_para_operaciones_banca_elecctronica', # 1,0
        'm5p9_7': 'utiliza_internet_para_tramites_organismos_gubernamentales', # 1,0
        'm5p9_8': 'utiliza_internet_para_servicio_soporte_cliente', # 1,0
        'm5p9_9': 'utiliza_internet_para_ventas_bienes_servicios', # 1,0
        'm5p9_10': 'utiliza_internet_para_promocionar_productos_servicios', # 1,0
        'm5p9_11': 'utiliza_internet_para_capacitacion_personal', # 1,0
        'm5p9_12': 'utiliza_internet_para_video_conferencia', # 1,0
        'm5p9_13': 'utiliza_internet_para_emision_facturas_electronicas', # 1,0
        'm5p9_14': 'utiliza_internet_para_servicios_computacion_nube', # 1,0
        'm5p9_15': 'utiliza_internet_otro', # 1,0
        'm5p12_1': 'razon_no_servicio_internet_no_necesita_no_utilidad_empresa', # 1,0
        'm5p12_2': 'razon_no_servicio_internet_desconoce_usarlo', # 1,0
        'm5p12_3': 'razon_no_servicio_internet_no_rentable_caro', # 1,0
        'm5p12_4': 'razon_no_servicio_internet_no_seguro', # 1,0
        'm5p12_5': 'razon_no_servicio_internet_otro', # 1,0
        'm5p13_1': 'razon_no_dispone_computadoras_no_necesita_no_utilidad_empresa', 
        'm5p13_2': 'razon_no_dispone_computadoras_desconoce_usarlo', 
        'm5p13_3': 'razon_no_dispone_computadoras_no_rentable_caro', 
        'm5p13_4': 'razon_no_dispone_computadoras_otro',
        'm5p17': 'empresa_dispuso_telefonos_moviles'
        }

      mod5_1.rename(columns=columns, inplace=True)
      mod5_1['company_id'] = mod5_1['company_id'].astype(int)
      mod5_1['dispone_computadora_escritorio'] = mod5_1['dispone_computadora_escritorio'].astype(pd.Int16Dtype())
      mod5_1['cantidad_computadora_escritorio'] = mod5_1['cantidad_computadora_escritorio'].astype(pd.Int32Dtype())
      mod5_1['antiguedad_computadora_escritorio'] = mod5_1['antiguedad_computadora_escritorio'].astype(pd.Int16Dtype())
      mod5_1['dispone_computadora_portatil'] = mod5_1['dispone_computadora_portatil'].astype(pd.Int16Dtype())
      mod5_1['cantidad_computadora_portatil'] = mod5_1['cantidad_computadora_portatil'].astype(pd.Int32Dtype())
      mod5_1['antiguedad_computadora_portatil'] = mod5_1['antiguedad_computadora_portatil'].astype(pd.Int16Dtype())
      mod5_1['dispone_multifuncional'] = mod5_1['dispone_multifuncional'].astype(pd.Int16Dtype())
      mod5_1['cantidad_multifuncional'] = mod5_1['cantidad_multifuncional'].astype(pd.Int32Dtype())
      mod5_1['antiguedad_multifuncional'] = mod5_1['antiguedad_multifuncional'].astype(pd.Int16Dtype())
      mod5_1['dispone_impresora'] = mod5_1['dispone_impresora'].astype(pd.Int16Dtype())
      mod5_1['cantidad_impresora'] = mod5_1['cantidad_impresora'].astype(pd.Int32Dtype())
      mod5_1['antiguedad_impresora'] = mod5_1['antiguedad_impresora'].astype(pd.Int16Dtype())
      mod5_1['dispone_escaner'] = mod5_1['dispone_escaner'].astype(pd.Int16Dtype()) 
      mod5_1['cantidad_escaner'] = mod5_1['cantidad_escaner'].astype(pd.Int32Dtype())
      mod5_1['antiguedad_escaner'] = mod5_1['antiguedad_escaner'].astype(pd.Int16Dtype())
      mod5_1['dispone_smartphone'] = mod5_1['dispone_smartphone'].astype(pd.Int16Dtype())
      mod5_1['cantidad_smartphone'] = mod5_1['cantidad_smartphone'].astype(pd.Int32Dtype())
      mod5_1['antiguedad_smartphone'] = mod5_1['antiguedad_smartphone'].astype(pd.Int16Dtype())
      mod5_1['otro_dispone'] = mod5_1['otro_dispone'].astype(pd.Int16Dtype())
      mod5_1['cantidad_otro_dispone'] = mod5_1['cantidad_otro_dispone'].astype(pd.Int32Dtype())
      mod5_1['antiguedad_otro_dispone'] = mod5_1['antiguedad_otro_dispone'].astype(pd.Int16Dtype())
      mod5_1['utiliza_pagina_web'] = mod5_1['utiliza_pagina_web'].astype(pd.Int16Dtype())
      mod5_1['utiliza_redes_sociales'] = mod5_1['utiliza_redes_sociales'].astype(pd.Int16Dtype()) 
      mod5_1['utiliza_linkedin'] = mod5_1['utiliza_linkedin'].astype(pd.Int16Dtype()) 
      mod5_1['tiene_software_contable'] = mod5_1['tiene_software_contable'].astype(pd.Int16Dtype())
      mod5_1['tiene_software_ventas'] = mod5_1['tiene_software_ventas'].astype(pd.Int16Dtype()) 
      mod5_1['tiene_software_personal'] = mod5_1['tiene_software_personal'].astype(pd.Int16Dtype())
      mod5_1['tiene_software_finanzas'] = mod5_1['tiene_software_finanzas'].astype(pd.Int16Dtype()) 
      mod5_1['tiene_software_logistica'] = mod5_1['tiene_software_logistica'].astype(pd.Int16Dtype())
      mod5_1['tiene_software_produccion'] = mod5_1['tiene_software_produccion'].astype(pd.Int16Dtype()) 
      mod5_1['tiene_software_soporte_informatico'] = mod5_1['tiene_software_soporte_informatico'].astype(pd.Int16Dtype())
      mod5_1['tiene_software_otro'] = mod5_1['tiene_software_otro'].astype(pd.Int16Dtype()) 
      mod5_1['tiene_software_ninguno'] = mod5_1['tiene_software_ninguno'].astype(pd.Int16Dtype())
      mod5_1['utiliza_internet_para_busqueda_productos_servicios'] = mod5_1['utiliza_internet_para_busqueda_productos_servicios'].astype(pd.Int16Dtype())
      mod5_1['utiliza_internet_para_busqueda_organismos_gubernamentales'] = mod5_1['utiliza_internet_para_busqueda_organismos_gubernamentales'].astype(pd.Int16Dtype())
      mod5_1['utiliza_internet_para_busqueda_informacion_investigacion_desarrollo'] = mod5_1['utiliza_internet_para_busqueda_informacion_investigacion_desarrollo'].astype(pd.Int16Dtype())
      mod5_1['utiliza_internet_para_otra_busqueda_informacion'] = mod5_1['utiliza_internet_para_otra_busqueda_informacion'].astype(pd.Int16Dtype())
      mod5_1['utiliza_internet_para_comunicaciones'] = mod5_1['utiliza_internet_para_comunicaciones'].astype(pd.Int16Dtype())
      mod5_1['utiliza_internet_para_operaciones_banca_elecctronica'] = mod5_1['utiliza_internet_para_operaciones_banca_elecctronica'].astype(pd.Int16Dtype())
      mod5_1['utiliza_internet_para_tramites_organismos_gubernamentales'] = mod5_1['utiliza_internet_para_tramites_organismos_gubernamentales'].astype(pd.Int16Dtype())
      mod5_1['utiliza_internet_para_servicio_soporte_cliente'] = mod5_1['utiliza_internet_para_servicio_soporte_cliente'].astype(pd.Int16Dtype())
      mod5_1['utiliza_internet_para_ventas_bienes_servicios'] = mod5_1['utiliza_internet_para_ventas_bienes_servicios'].astype(pd.Int16Dtype())
      mod5_1['utiliza_internet_para_promocionar_productos_servicios'] = mod5_1['utiliza_internet_para_promocionar_productos_servicios'].astype(pd.Int16Dtype())
      mod5_1['utiliza_internet_para_capacitacion_personal'] = mod5_1['utiliza_internet_para_capacitacion_personal'].astype(pd.Int16Dtype())
      mod5_1['utiliza_internet_para_video_conferencia'] = mod5_1['utiliza_internet_para_video_conferencia'].astype(pd.Int16Dtype())
      mod5_1['utiliza_internet_para_emision_facturas_electronicas'] = mod5_1['utiliza_internet_para_emision_facturas_electronicas'].astype(pd.Int16Dtype())
      mod5_1['utiliza_internet_para_servicios_computacion_nube'] = mod5_1['utiliza_internet_para_servicios_computacion_nube'].astype(pd.Int16Dtype())
      mod5_1['utiliza_internet_otro'] = mod5_1['utiliza_internet_otro'].astype(pd.Int16Dtype())
      mod5_1['razon_no_servicio_internet_no_necesita_no_utilidad_empresa'] = mod5_1['razon_no_servicio_internet_no_necesita_no_utilidad_empresa'].astype(pd.Int16Dtype())
      mod5_1['razon_no_servicio_internet_desconoce_usarlo'] = mod5_1['razon_no_servicio_internet_desconoce_usarlo'].astype(pd.Int16Dtype())
      mod5_1['razon_no_servicio_internet_no_rentable_caro'] = mod5_1['razon_no_servicio_internet_no_rentable_caro'].astype(pd.Int16Dtype())
      mod5_1['razon_no_servicio_internet_no_seguro'] = mod5_1['razon_no_servicio_internet_no_seguro'].astype(pd.Int16Dtype())
      mod5_1['razon_no_servicio_internet_otro'] = mod5_1['razon_no_servicio_internet_otro'].astype(pd.Int16Dtype())
      mod5_1['razon_no_dispone_computadoras_no_necesita_no_utilidad_empresa'] = mod5_1['razon_no_dispone_computadoras_no_necesita_no_utilidad_empresa'].astype(pd.Int16Dtype())
      mod5_1['razon_no_dispone_computadoras_desconoce_usarlo'] = mod5_1['razon_no_dispone_computadoras_desconoce_usarlo'].astype(pd.Int16Dtype())
      mod5_1['razon_no_dispone_computadoras_no_rentable_caro'] = mod5_1['razon_no_dispone_computadoras_no_rentable_caro'].astype(pd.Int16Dtype())
      mod5_1['razon_no_dispone_computadoras_otro'] = mod5_1['razon_no_dispone_computadoras_otro'].astype(pd.Int16Dtype())
      mod5_1['empresa_dispuso_telefonos_moviles'] = mod5_1['empresa_dispuso_telefonos_moviles'].astype(pd.Int16Dtype())

      return df, mod2_1, mod2_2, mod4_1, mod4_2, mod5_1
