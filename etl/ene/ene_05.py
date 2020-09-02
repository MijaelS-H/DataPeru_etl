import pandas as pd
from simpledbf import Dbf5
from bamboo_lib.models import PipelineStep


class Module5Step(PipelineStep):
    def run_step(self, prev, params):

      df, mod2_1, mod2_2, mod4_1, mod4_2, mod5_1, mod5_2 = prev

      file_data = '../../data/ene/05_MÓDULO_VII_VIII_IX.dbf'

      dbf = Dbf5(file_data, codec='latin-1')
      mod6 = dbf.to_dataframe()
      mod6.columns = [x.lower() for x in mod6.columns]

      # 05_MÓDULO_VII_VIII_IX
      mod6 = mod6[[
        'iruc', 'm7p6_1', 'm7p6_2', 'm7p6_3', 'm7p6_4', 'm7p6_5', 'm7p6_6', 'm7p6_7',
        'm7p6_8', 'm7p6_9', 'm8p1', 'm8p2', 'm8p4', 'm8p5', 'm8p8', 'm8p9', 'm8p10',
        'm8p11', 'm8p12', 'm8p13', 'm8p14', 'm8p15', 'm8p16', 'm8p17', 'm8p18', 'm8p18_o',
        'm8p19', 'm8p20', 'm8p21', 'm8p22', 'm8p23', 'm8p24', 'm8p25', 'm8p26', 'm8p27',
        'm8p27_o', 'm8p28', 'm8p29', 'm8p32', 'm8p33', 'm8p34', 'm8p35', 'm8p40', 'm8p42',
        'm8p43', 'm8p46', 'm8p47', 'm8p48', 'm8p49', 'm8p50', 'm8p53_1', 'm8p53_2', 'm8p53_3',
        'm8p53_4', 'm8p53_5', 'm8p53_6', 'm8p53_7', 'm8p53_8', 'm8p53_9', 'm8p53_10','m8p53_11',  
        'm8p53_12', 'm8p53_13', 'm8p53_14', 'm8p53_15', 'm8p53_16', 'm8p53_17', 'm8p53_18',
        'm8p53_19', 'm8p53_20', 'm8p53_21', 'm8p53_22'
        ]].copy()

      columns = {
        'iruc': 'company_id',
        'm7p6_1': 'criterio_elegir_credito_prestamo_tasas_interes', # 1,0
        'm7p6_2': 'criterio_elegir_credito_prestamo_plazo_pago', # 1,0
        'm7p6_3': 'criterio_elegir_credito_prestamo_garantias_solicitadas', # 1,0
        'm7p6_4': 'criterio_elegir_credito_prestamo_prestigio_entidad_financiera', # 1,0
        'm7p6_5': 'criterio_elegir_credito_prestamo_recomendacion_terceros', # 1,0
        'm7p6_6': 'criterio_elegir_credito_prestamo_beneficios_entidad', # 1,0
        'm7p6_7': 'criterio_elegir_credito_prestamo_rapidez_otorgamiento', # 1,0
        'm7p6_8': 'criterio_elegir_credito_prestamo_periodio_gracia', # 1,0
        'm7p6_9': 'criterio_elegir_credito_prestamo_otro', # 1,0
        'm8p1': 'duracion_contrato_trabajadores_2014', # options 
        'm8p2': 'califica_requisitos_mtpe_contratacion_trabajadores_plazo_fijo', # options
        'm8p4': 'n_trabajadores_promedio_plazo_indeterminado', # measure
        'm8p5': 'califica_requisitos_mtpe_contratacion_trabajadores_plazo_indeterminado', # options
        'm8p8': 'conocimiento_minimo_requerido_trabajadores_discapacidad_contratar', # 1,0
        'm8p9': 'califica_porcentaje_contratacion_personas_discapacidad', # options
        'm8p10': 'califica_exigencias_infraestructura_personas_discapacidad', # options
        'm8p11': 'califica_cumplimiento_empresas_obligaciones_seguridad_salud', # options
        'm8p12': 'califica_requisitos_tercerizacion_laboral_empresas', # options
        'm8p13': 'afecta_decision_contratar_pago_beneficios_sociales_tercerizacion_laboral', # options
        'm8p14': 'califica_cumplimiento_obligaciones_proteccion_maternidad', # options
        'm8p15': 'conoce_sunafil', # options
        'm8p16': 'califica_labor_fiscalizacion_regulacion_laboral_sunafil', # options
        'm8p17': 'cuenta_instrumentos_gestion_ambiental', # options
        'm8p18': 'motivo_aplicar_instrumentos_gestion_ambiental_su_rubro', # options
        'm8p18_o': 'motivo_aplicar_instrumentos_gestion_ambiental_su_rubro_respuesta_abierta', # options
        'm8p19': 'califica_procedimientos_obtencion_declaracion_impacto_ambiental', # options
        'm8p20': 'califica_procedimientos_obtencion_estudio_impacto_ambiental', # options
        'm8p21': 'califica_procedimientos_obtencion_declaracion_ambiental_actividades_curso', # options
        'm8p22': 'califica_procedimientos_obtencion_programa_adecuacion_manejo_ambiental', # options
        'm8p23': 'califica_actividades_fiscalizacion_sancion_materia_regulacion_ambiental_2014', # options (PRODUCE, MINEM, etc.)
        'm8p24': 'califica_actividades_fiscalizacion_sancion_materia_regulacion_ambiental_oefa', # options  Organismo de Evaluación y Fiscalización Ambiental
        'm8p25': 'califica_actividades_fiscalizacion_sancion_materia_regulacion_ambiental_osinfor', # options 
        'm8p26': 'califica_actividades_fiscalizacion_sancion_materia_regulacion_ambiental_gobierno_regional', # options
        'm8p27': 'regimen_tributario_empresa_2014', # options
        'm8p27_o': 'regimen_tributario_empresa_2014_respuesta_abierta', # texto
        'm8p28': 'califica_procedimientos_obligacion_impuestos_nacionales', # options (IR, IGV, ISC, etc.)
        'm8p29': 'califica_procedimientos_pago_tributo_municipal', # options (predial, alcabala, vehicular, arbitrios, etc.)
        'm8p32': 'califica_actividades_fiscalizacion_sancion_materia_tributos_nacionales_2014', # options
        'm8p33': 'califica_actividades_fiscalizacion_sancion_materia_tributos_municipalidad_jurisdiccion_2014', # options
        'm8p34': 'dias_demora_obtener_licencia_funcionamiento', # options
        'm8p35': 'califica_procedimientos_obtencion_licencia_funcionamimento', # options
        'm8p40': 'ha_solicitado_autorizacion_sectorial', # options
        'm8p42': 'califica_procedimientos_obtencion_autorizacion_sectorial', # options
        'm8p43': 'requerimientos_diferente_permiso_sectorial_pago', # options
        'm8p46': 'obtuvo_certificado_seguridad_defensa_civil', # options
        'm8p47': 'dias_demora_obtener_certificado_seguridad_defensa_civil',  # options
        'm8p48': 'tipo_certificado_seguridad_defensa_civil',  # options
        'm8p49': 'califica_procedimiento_obtener_certificado_seguridad_defensa_civil', # options
        'm8p50': 'empresa_atendio_inspeccion_tecnicas_seguridad_defensa_civil', # options
        'm8p53_1': 'factor_limita_crecimiento_empresa_financiamiento', # 1,0 
        'm8p53_2': 'factor_limita_crecimiento_empresa_proceso_productivo_poco_automatizado', # 1,0 
        'm8p53_3': 'factor_limita_crecimiento_empresa_falta_mano_obra_calificada', # 1,0 
        'm8p53_4': 'factor_limita_crecimiento_empresa_falta_repuesto_servicio_tecnico_maquinaria', # 1,0 
        'm8p53_5': 'factor_limita_crecimiento_empresa_demanda_limitada', # 1,0 
        'm8p53_6': 'factor_limita_crecimiento_empresa_falta_insumos_nacionales', # 1,0 
        'm8p53_7': 'factor_limita_crecimiento_empresa_falta_insumos_importados', # 1,0 
        'm8p53_8': 'factor_limita_crecimiento_empresa_falta_energia_electrica', # 1,0 
        'm8p53_9': 'factor_limita_crecimiento_empresa_falta_informacion_tecnologica', # 1,0 
        'm8p53_10': 'factor_limita_crecimiento_empresa_falta_informacion_mercados', # 1,0 
        'm8p53_11': 'factor_limita_crecimiento_empresa_excesiva_regulacion_laboral', # 1,0 
        'm8p53_12': 'factor_limita_crecimiento_empresa_excesiva_regulacion_tributaria', # 1,0 
        'm8p53_13': 'factor_limita_crecimiento_empresa_excesiva_regulacion_ambiental', # 1,0 
        'm8p53_14': 'factor_limita_crecimiento_empresa_excesiva_regulacion_licencia_funcionamiento_construccion', # 1,0 
        'm8p53_15': 'factor_limita_crecimiento_empresa_excesiva_regulacion_defensa_civil', # 1,0 
        'm8p53_16': 'factor_limita_crecimiento_empresa_excesiva_regulacion_tramites_sectoriales_autorizaciones', # 1,0 
        'm8p53_17': 'factor_limita_crecimiento_empresa_corrupcion_funcionarios_publicos', # 1,0 
        'm8p53_18': 'factor_limita_crecimiento_empresa_contrabando', # 1,0 
        'm8p53_19': 'factor_limita_crecimiento_empresa_exceso_cargas_tributarias', # 1,0 
        'm8p53_20': 'factor_limita_crecimiento_empresa_informalidad', # 1,0 
        'm8p53_21': 'factor_limita_crecimiento_empresa_otro', # 1,0 
        'm8p53_22': 'factor_limita_crecimiento_empresa_ninguno' # 1,0 
      }

      # Transform ranking options to 0, 1, nan, 0 does not appear as a valid option
      for col in [
        'm8p53_1', 'm8p53_2', 'm8p53_3', 'm8p53_4', 'm8p53_5', 'm8p53_6', 'm8p53_7', 'm8p53_8',
        'm8p53_9', 'm8p53_10','m8p53_11', 'm8p53_12', 'm8p53_13', 'm8p53_14', 'm8p53_15', 'm8p53_16',
        'm8p53_17', 'm8p53_18', 'm8p53_19', 'm8p53_20', 'm8p53_21', 'm8p53_22'
        ]:
        mod6.loc[(~mod6[col].isna()) & (mod6[col] != 0), col] = 1

      mod6.rename(columns=columns, inplace=True)
      mod6['company_id'] = mod6['company_id'].astype(int)
      mod6['criterio_elegir_credito_prestamo_tasas_interes'] = mod6['criterio_elegir_credito_prestamo_tasas_interes'].astype(pd.Int16Dtype())
      mod6['criterio_elegir_credito_prestamo_plazo_pago'] = mod6['criterio_elegir_credito_prestamo_plazo_pago'].astype(pd.Int16Dtype())
      mod6['criterio_elegir_credito_prestamo_garantias_solicitadas'] = mod6['criterio_elegir_credito_prestamo_garantias_solicitadas'].astype(pd.Int16Dtype())
      mod6['criterio_elegir_credito_prestamo_prestigio_entidad_financiera'] = mod6['criterio_elegir_credito_prestamo_prestigio_entidad_financiera'].astype(pd.Int16Dtype())
      mod6['criterio_elegir_credito_prestamo_recomendacion_terceros'] = mod6['criterio_elegir_credito_prestamo_recomendacion_terceros'].astype(pd.Int16Dtype())
      mod6['criterio_elegir_credito_prestamo_beneficios_entidad'] = mod6['criterio_elegir_credito_prestamo_beneficios_entidad'].astype(pd.Int16Dtype())
      mod6['criterio_elegir_credito_prestamo_rapidez_otorgamiento'] = mod6['criterio_elegir_credito_prestamo_rapidez_otorgamiento'].astype(pd.Int16Dtype())
      mod6['criterio_elegir_credito_prestamo_periodio_gracia'] = mod6['criterio_elegir_credito_prestamo_periodio_gracia'].astype(pd.Int16Dtype())
      mod6['criterio_elegir_credito_prestamo_otro'] = mod6['criterio_elegir_credito_prestamo_otro'].astype(pd.Int16Dtype())
      mod6['duracion_contrato_trabajadores_2014'] = mod6['duracion_contrato_trabajadores_2014'].astype(pd.Int16Dtype())
      mod6['califica_requisitos_mtpe_contratacion_trabajadores_plazo_fijo'] = mod6['califica_requisitos_mtpe_contratacion_trabajadores_plazo_fijo'].astype(pd.Int16Dtype())
      mod6['n_trabajadores_promedio_plazo_indeterminado'] = mod6['n_trabajadores_promedio_plazo_indeterminado'].astype(pd.Int32Dtype())
      mod6['califica_requisitos_mtpe_contratacion_trabajadores_plazo_indeterminado'] = mod6['califica_requisitos_mtpe_contratacion_trabajadores_plazo_indeterminado'].astype(pd.Int16Dtype())
      mod6['conocimiento_minimo_requerido_trabajadores_discapacidad_contratar'] = mod6['conocimiento_minimo_requerido_trabajadores_discapacidad_contratar'].astype(pd.Int16Dtype())
      mod6['califica_porcentaje_contratacion_personas_discapacidad'] = mod6['califica_porcentaje_contratacion_personas_discapacidad'].astype(pd.Int16Dtype())
      mod6['califica_exigencias_infraestructura_personas_discapacidad'] = mod6['califica_exigencias_infraestructura_personas_discapacidad'].astype(pd.Int16Dtype())
      mod6['califica_cumplimiento_empresas_obligaciones_seguridad_salud'] = mod6['califica_cumplimiento_empresas_obligaciones_seguridad_salud'].astype(pd.Int16Dtype())
      mod6['califica_requisitos_tercerizacion_laboral_empresas'] = mod6['califica_requisitos_tercerizacion_laboral_empresas'].astype(pd.Int16Dtype())
      mod6['afecta_decision_contratar_pago_beneficios_sociales_tercerizacion_laboral'] = mod6['afecta_decision_contratar_pago_beneficios_sociales_tercerizacion_laboral'].astype(pd.Int16Dtype())
      mod6['califica_cumplimiento_obligaciones_proteccion_maternidad'] = mod6['califica_cumplimiento_obligaciones_proteccion_maternidad'].astype(pd.Int16Dtype())
      mod6['conoce_sunafil'] = mod6['conoce_sunafil'].astype(pd.Int16Dtype())
      mod6['califica_labor_fiscalizacion_regulacion_laboral_sunafil'] = mod6['califica_labor_fiscalizacion_regulacion_laboral_sunafil'].astype(pd.Int16Dtype())
      mod6['cuenta_instrumentos_gestion_ambiental'] = mod6['cuenta_instrumentos_gestion_ambiental'].astype(pd.Int16Dtype())
      mod6['motivo_aplicar_instrumentos_gestion_ambiental_su_rubro'] = mod6['motivo_aplicar_instrumentos_gestion_ambiental_su_rubro'].astype(pd.Int16Dtype())
      mod6['califica_procedimientos_obtencion_declaracion_impacto_ambiental'] = mod6['califica_procedimientos_obtencion_declaracion_impacto_ambiental'].astype(pd.Int16Dtype())
      mod6['califica_procedimientos_obtencion_estudio_impacto_ambiental'] = mod6['califica_procedimientos_obtencion_estudio_impacto_ambiental'].astype(pd.Int16Dtype())
      mod6['califica_procedimientos_obtencion_declaracion_ambiental_actividades_curso'] = mod6['califica_procedimientos_obtencion_declaracion_ambiental_actividades_curso'].astype(pd.Int16Dtype())
      mod6['califica_procedimientos_obtencion_programa_adecuacion_manejo_ambiental'] = mod6['califica_procedimientos_obtencion_programa_adecuacion_manejo_ambiental'].astype(pd.Int16Dtype())
      mod6['califica_actividades_fiscalizacion_sancion_materia_regulacion_ambiental_2014'] = mod6['califica_actividades_fiscalizacion_sancion_materia_regulacion_ambiental_2014'].astype(pd.Int16Dtype())
      mod6['califica_actividades_fiscalizacion_sancion_materia_regulacion_ambiental_oefa'] = mod6['califica_actividades_fiscalizacion_sancion_materia_regulacion_ambiental_oefa'].astype(pd.Int16Dtype())
      mod6['califica_actividades_fiscalizacion_sancion_materia_regulacion_ambiental_osinfor'] = mod6['califica_actividades_fiscalizacion_sancion_materia_regulacion_ambiental_osinfor'].astype(pd.Int16Dtype())
      mod6['califica_actividades_fiscalizacion_sancion_materia_regulacion_ambiental_gobierno_regional'] = mod6['califica_actividades_fiscalizacion_sancion_materia_regulacion_ambiental_gobierno_regional'].astype(pd.Int16Dtype())
      mod6['regimen_tributario_empresa_2014'] = mod6['regimen_tributario_empresa_2014'].astype(pd.Int16Dtype())
      mod6['califica_procedimientos_obligacion_impuestos_nacionales'] = mod6['califica_procedimientos_obligacion_impuestos_nacionales'].astype(pd.Int16Dtype())
      mod6['califica_procedimientos_pago_tributo_municipal'] = mod6['califica_procedimientos_pago_tributo_municipal'].astype(pd.Int16Dtype())
      mod6['califica_actividades_fiscalizacion_sancion_materia_tributos_nacionales_2014'] = mod6['califica_actividades_fiscalizacion_sancion_materia_tributos_nacionales_2014'].astype(pd.Int16Dtype())
      mod6['califica_actividades_fiscalizacion_sancion_materia_tributos_municipalidad_jurisdiccion_2014'] = mod6['califica_actividades_fiscalizacion_sancion_materia_tributos_municipalidad_jurisdiccion_2014'].astype(pd.Int16Dtype())
      mod6['dias_demora_obtener_licencia_funcionamiento'] = mod6['dias_demora_obtener_licencia_funcionamiento'].astype(pd.Int16Dtype())
      mod6['califica_procedimientos_obtencion_licencia_funcionamimento'] = mod6['califica_procedimientos_obtencion_licencia_funcionamimento'].astype(pd.Int16Dtype())
      mod6['ha_solicitado_autorizacion_sectorial'] = mod6['ha_solicitado_autorizacion_sectorial'].astype(pd.Int16Dtype())
      mod6['califica_procedimientos_obtencion_autorizacion_sectorial'] = mod6['califica_procedimientos_obtencion_autorizacion_sectorial'].astype(pd.Int16Dtype())
      mod6['requerimientos_diferente_permiso_sectorial_pago'] = mod6['requerimientos_diferente_permiso_sectorial_pago'].astype(pd.Int16Dtype())
      mod6['obtuvo_certificado_seguridad_defensa_civil'] = mod6['obtuvo_certificado_seguridad_defensa_civil'].astype(pd.Int16Dtype())
      mod6['dias_demora_obtener_certificado_seguridad_defensa_civil'] = mod6['dias_demora_obtener_certificado_seguridad_defensa_civil'].astype(pd.Int16Dtype())
      mod6['tipo_certificado_seguridad_defensa_civil'] = mod6['tipo_certificado_seguridad_defensa_civil'].astype(pd.Int16Dtype())
      mod6['califica_procedimiento_obtener_certificado_seguridad_defensa_civil'] = mod6['califica_procedimiento_obtener_certificado_seguridad_defensa_civil'].astype(pd.Int16Dtype())
      mod6['empresa_atendio_inspeccion_tecnicas_seguridad_defensa_civil'] = mod6['empresa_atendio_inspeccion_tecnicas_seguridad_defensa_civil'].astype(pd.Int16Dtype())
      mod6['factor_limita_crecimiento_empresa_financiamiento'] = mod6['factor_limita_crecimiento_empresa_financiamiento'].astype(pd.Int16Dtype())
      mod6['factor_limita_crecimiento_empresa_proceso_productivo_poco_automatizado'] = mod6['factor_limita_crecimiento_empresa_proceso_productivo_poco_automatizado'].astype(pd.Int16Dtype())
      mod6['factor_limita_crecimiento_empresa_falta_mano_obra_calificada'] = mod6['factor_limita_crecimiento_empresa_falta_mano_obra_calificada'].astype(pd.Int16Dtype())
      mod6['factor_limita_crecimiento_empresa_falta_repuesto_servicio_tecnico_maquinaria'] = mod6['factor_limita_crecimiento_empresa_falta_repuesto_servicio_tecnico_maquinaria'].astype(pd.Int16Dtype())
      mod6['factor_limita_crecimiento_empresa_demanda_limitada'] = mod6['factor_limita_crecimiento_empresa_demanda_limitada'].astype(pd.Int16Dtype())
      mod6['factor_limita_crecimiento_empresa_falta_insumos_nacionales'] = mod6['factor_limita_crecimiento_empresa_falta_insumos_nacionales'].astype(pd.Int16Dtype())
      mod6['factor_limita_crecimiento_empresa_falta_insumos_importados'] = mod6['factor_limita_crecimiento_empresa_falta_insumos_importados'].astype(pd.Int16Dtype())
      mod6['factor_limita_crecimiento_empresa_falta_energia_electrica'] = mod6['factor_limita_crecimiento_empresa_falta_energia_electrica'].astype(pd.Int16Dtype())
      mod6['factor_limita_crecimiento_empresa_falta_informacion_tecnologica'] = mod6['factor_limita_crecimiento_empresa_falta_informacion_tecnologica'].astype(pd.Int16Dtype())
      mod6['factor_limita_crecimiento_empresa_falta_informacion_mercados'] = mod6['factor_limita_crecimiento_empresa_falta_informacion_mercados'].astype(pd.Int16Dtype())
      mod6['factor_limita_crecimiento_empresa_excesiva_regulacion_laboral'] = mod6['factor_limita_crecimiento_empresa_excesiva_regulacion_laboral'].astype(pd.Int16Dtype())
      mod6['factor_limita_crecimiento_empresa_excesiva_regulacion_tributaria'] = mod6['factor_limita_crecimiento_empresa_excesiva_regulacion_tributaria'].astype(pd.Int16Dtype())
      mod6['factor_limita_crecimiento_empresa_excesiva_regulacion_ambiental'] = mod6['factor_limita_crecimiento_empresa_excesiva_regulacion_ambiental'].astype(pd.Int16Dtype())
      mod6['factor_limita_crecimiento_empresa_excesiva_regulacion_licencia_funcionamiento_construccion'] = mod6['factor_limita_crecimiento_empresa_excesiva_regulacion_licencia_funcionamiento_construccion'].astype(pd.Int16Dtype())
      mod6['factor_limita_crecimiento_empresa_excesiva_regulacion_defensa_civil'] = mod6['factor_limita_crecimiento_empresa_excesiva_regulacion_defensa_civil'].astype(pd.Int16Dtype())
      mod6['factor_limita_crecimiento_empresa_excesiva_regulacion_tramites_sectoriales_autorizaciones'] = mod6['factor_limita_crecimiento_empresa_excesiva_regulacion_tramites_sectoriales_autorizaciones'].astype(pd.Int16Dtype())
      mod6['factor_limita_crecimiento_empresa_corrupcion_funcionarios_publicos'] = mod6['factor_limita_crecimiento_empresa_corrupcion_funcionarios_publicos'].astype(pd.Int16Dtype())
      mod6['factor_limita_crecimiento_empresa_contrabando'] = mod6['factor_limita_crecimiento_empresa_contrabando'].astype(pd.Int16Dtype())
      mod6['factor_limita_crecimiento_empresa_exceso_cargas_tributarias'] = mod6['factor_limita_crecimiento_empresa_exceso_cargas_tributarias'].astype(pd.Int16Dtype())
      mod6['factor_limita_crecimiento_empresa_informalidad'] = mod6['factor_limita_crecimiento_empresa_informalidad'].astype(pd.Int16Dtype())
      mod6['factor_limita_crecimiento_empresa_otro'] = mod6['factor_limita_crecimiento_empresa_otro'].astype(pd.Int16Dtype())
      mod6['factor_limita_crecimiento_empresa_ninguno'] = mod6['factor_limita_crecimiento_empresa_ninguno'].astype(pd.Int16Dtype())

      return df, mod2_1, mod2_2, mod4_1, mod4_2, mod5_1, mod5_2, mod6
