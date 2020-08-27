
import pandas as pd
from simpledbf import Dbf5
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep

class ReadStep(PipelineStep):
  def run_step(self, prev, params):

    data = {
      'df': '../../data/ene/01_EMPRESA_IDENTIFICA.dbf',
      'mod2_1': '../../data/ene/02_MÓDULO_I_II_III_1.dbf',
      'mod2_2': '../../data/ene/02_MÓDULO_I_II_III_2.dbf',
      'mod4_1': '../../data/ene/03_MÓDULO_IV_1.dbf',
      'mod4_2': '../../data/ene/03_MÓDULO_IV_2.dbf',
      'mod5_1': '../../data/ene/04_MÓDULO_V_VI_1.dbf',
      'mod5_2': '../../data/ene/04_MÓDULO_V_VI_2.dbf',
      'mod6': '../../data/ene/05_MÓDULO_VII_VIII_IX.dbf'
        }

    for file_name, file_data in data.items():
       dbf = Dbf5(file_data, codec='latin-1')
       df = dbf.to_dataframe()
       df.columns = [x.lower() for x in df.columns]
       data[file_name] = df
    return data.values()

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
      df, mod2_1, mod2_2, mod4_1, mod4_2, mod5_1, mod5_2, mod6 = prev

      # 01_EMPRESA_IDENTIFICA
      df['district_id'] = df['ccdd'].astype(str) + df['ccpp'].astype(str) + df['ccdi'].astype(str)
      
      df = df[[
        'iruc', 'district_id', 'latitud', 'longitud',
        'c1', 'c2', 'c3', 'c4', 'c14_cod', 'c15a_cod',
        'c15b_cod', 'c15c_cod', 'c15d_cod', 'c16', 'c16_o', 
        'c20', 'factor_exp'
        ]].copy()
      
      columns = {
        'iruc': 'company_id',
        'district_id': 'district_id',
        'c1': 'ruc',
        'c2': 'razon_social',
        'c3': 'nombre_comercial',
        'c4': 'anio_inicio_actividades',
        'c14_cod': 'ciiu_rev4',
        'c15a_cod': 'ciiu_rev4_a',
        'c15b_cod': 'ciiu_rev4_b',
        'c15c_cod': 'ciiu_rev4_c',
        'c15d_cod': 'ciiu_rev4_d',
        'c16': 'tipo_sociedad',
        'c16_o': 'tipo_sociedad_text',
        'c20': 'rango_ventas_2014',
        'factor_exp': 'factor_expansion'
        }

      df.rename(columns=columns, inplace=True)
      df = df[list(columns.values())].copy()
      df['company_id'] = df['company_id'].astype(int)

      # 02_MÓDULO_I_II_III_1
      mod2_1 = mod2_1[['iruc',
        'm1p2', 'm1p3', 'm1p4', 'm1p5', 'm1p6_1', 'm1p6_2', 'm1p6_3', 'm1p6_4',
        'm1p6_5', 'm1p6_6', 'm1p6_7', 'm1p6_8', 'm1p6_9', 'm1p7_1', 'm1p7_2',
        'm1p7_3', 'm1p8', 'm1p10', 'm1p12_1', 'm1p12_2', 'm1p12_3', 'm1p12_4',
        'm1p12_5', 'm1p12_6', 'm1p12_7', 'm1p13', 'm1p14', 'm1p15', 'm1p17_1',
        'm1p17_2', 'm1p17_3', 'm1p17_4', 'm1p17_5', 'm1p17_6', 'm1p17_7', 'm1p17_8',
        'm1p19', 'm1p20', 'm2p10', 'm2p11_1', 'm2p11_2', 'm2p11_3', 'm2p11_4',
        'm2p11_5', 'm2p11_6', 'm2p11_7', 'm2p11_8', 'm2p11_8_o', 'm2p11_9', 'm2p17',
        'm2p18_1', 'm2p18_2', 'm2p18_3', 'm2p18_4', 'm2p18_5', 'm2p18_6', 'm2p18_7', 
        'm2p18_8', 'm2p18_9', 'm3p1_1', 'm3p1_1a', 'm3p1_1b', 'm3p1_1c', 'm3p1_1d', 
        'm3p1_2', 'm3p1_2a', 'm3p1_2b', 'm3p1_2c', 'm3p1_2d', 'm3p1_3', 'm3p1_3a', 
        'm3p1_3b', 'm3p1_3c', 'm3p1_3d','m3p1_4', 'm3p1_4a', 'm3p1_4b', 'm3p1_4c',
        'm3p1_4d', 'm3p1_5', 'm3p1_5a', 'm3p1_5b', 'm3p1_5c', 'm3p1_5d', 'm3p1_6', 
        'm3p1_6a', 'm3p1_6b', 'm3p1_6c', 'm3p1_7', 'm3p1_7a', 'm3p1_7b', 'm3p1_7c'
        ]].copy()

      columns = {
        'iruc': 'company_id',
        'm1p2': 'plan_negocios',
        'm1p3': 'credito_inicio_operaciones',
        'm1p4': 'institucion_credito',
        'm1p5': 'areas_funcionales_identificar',
        'm1p6_1': 'recursos_humanos', 
        'm1p6_2': 'logistica_aprovisionamiento', 
        'm1p6_3': 'comercializacion', 
        'm1p6_4': 'contabilidad', 
        'm1p6_5': 'produccion', 
        'm1p6_6': 'direccion_gerencia', 
        'm1p6_7': 'area_legal', 
        'm1p6_8': 'soporte_informatico', 
        'm1p6_9': 'otro',
        'm1p7_1': 'participa_mercado_internacional', 
        'm1p7_2': 'participa_mercado_nacional', 
        'm1p7_3': 'participa_mercado_local', 
        'm1p8': 'mercado_principal', 
        'm1p10': 'considera_competencia_existe', 
        'm1p12_1': 'competencia_informal_precio', 
        'm1p12_2': 'competencia_informal_calidad', 
        'm1p12_3': 'competencia_informal_grado_diferenciacion', 
        'm1p12_4': 'competencia_informal_tiempo_entrega', 
        'm1p12_5': 'competencia_informal_promociones_descuentos', 
        'm1p12_6': 'competencia_informal_servicio_cliente', 
        'm1p12_7': 'competencia_informal_otro',
        'm1p13': 'pertenece_organizacion_fin_empresarial', 
        'm1p14': 'anio_incorporacion_organizacion_fin_empresarial', 
        'm1p15': 'tipo_organizacion_fin_empresarial',
        'm1p17_1': 'acceso_info_negociar_proveedores', 
        'm1p17_2': 'acceso_info_servicios_financieros', 
        'm1p17_3': 'acceso_info_acceso_mercados', 
        'm1p17_4': 'acceso_info_empresarial', 
        'm1p17_5': 'acceso_capacitacion_asis_tecnica', 
        'm1p17_6': 'acceso_info_vigilancia_limpieza', 
        'm1p17_7': 'acceso_info_infraestructura',
        'm1p17_8': 'acceso_info_otro',
        'm1p19': 'utilidad_relacion_asociatividad_2014', 
        'm1p20': 'no_pertenece_organizacion_agrupacion', 
        'm2p10': 'local_electricidad_publica',
        'm2p11_1': 'interrupciones_electricas_programadas', 
        'm2p11_2': 'interrupciones_electricas_no_programadas', 
        'm2p11_3': 'dispo_servicio_electrico_horas_dias', # solo por horas/dias
        'm2p11_4': 'demora_no_atienden_reclamos_servicio_electrico', # centros de atencion y pago
        'm2p11_5': 'costo_elevado_servicio_electrico',
        'm2p11_6': 'intensidad_corriente_electrica_variable',
        'm2p11_7': 'errores_facturacion_servicio_electrico',
        'm2p11_8': 'otro_problema_servicio_electrico',
        'm2p11_8_o': 'otro_problema_servicio_electrico_nombre',
        'm2p11_9': 'no_problema_servicio_electrico',
        'm2p17': 'cuenta_agua_red_publica',
        'm2p18_1': 'interrupciones_suministro_agua_programadas',
        'm2p18_2': 'interrupciones_suministro_agua_no_programadas',
        'm2p18_3': 'dispo_suministro_agua_horas_dias',
        'm2p18_4': 'demora_no_atienden_reclamos_suministro_agua',
        'm2p18_5': 'costo_elevado_suministro_agua',
        'm2p18_6': 'intensidad_suministro_agua_variable',
        'm2p18_7': 'errores_facturacion_servicio_agua',
        'm2p18_8': 'mala_calidad_agua',
        'm2p18_9': 'otro_problema_servicio_suministro_agua',
        'm3p1_1': 'tiene_ejecutivos', 
        'm3p1_1a': 'n_ejecutivos_total', # comprobar la suma
        'm3p1_1b': 'n_ejecutivos_hombres', 
        'm3p1_1c': 'n_ejecutivos_mujeres', 
        'm3p1_1d': 'ejecutivos_remuneracion_promedio_mensual',
        'm3p1_2': 'tiene_empleados_permanentes', # 1 anio >= contrato
        'm3p1_2a': 'n_empleados_permanentes_total', 
        'm3p1_2b': 'n_empleados_permanentes_hombres', 
        'm3p1_2c': 'n_empleados_permanentes_mujeres', 
        'm3p1_2d': 'empleados_permanentes_remuneracion_promedio_mensual',# nuevos soles
        'm3p1_3': 'tiene_obreros_permanentes', # 1 anio >= contrato
        'm3p1_3a': 'n_obreros_permanentes_total', 
        'm3p1_3b': 'n_obreros_permanentes_hombres', 
        'm3p1_3c': 'n_obreros_permanentes_mujeres', 
        'm3p1_3d': 'obreros_permanentes_remuneracion_promedio_mensual', # nuevos soles
        'm3p1_4': 'tiene_empleados_eventuales', # 1 anio < contrato
        'm3p1_4a': 'n_empleados_eventuales_total', 
        'm3p1_4b': 'n_empleados_eventuales_hombres', 
        'm3p1_4c': 'n_empleados_eventuales_mujeres', 
        'm3p1_4d': 'empleados_eventuales_remuneracion_promedio_mensual', # nuevos soles
        'm3p1_5': 'tiene_obreros_eventuales', # 1 anio < contrato
        'm3p1_5a': 'n_obreros_eventuales_total', 
        'm3p1_5b': 'n_obreros_eventuales_hombres', 
        'm3p1_5c': 'n_obreros_eventuales_mujeres', 
        'm3p1_5d': 'obreros_eventuales_remuneracion_promedio_mensual', # nuevos soles
        'm3p1_6': 'tiene_total_1_2_3_4_5', # suma de los de arriba
        'm3p1_6a': 'n_total_1_2_3_4_5_total', 
        'm3p1_6b': 'n_total_1_2_3_4_5_hombres', 
        'm3p1_6c': 'n_total_1_2_3_4_5_mujeres',
        'm3p1_7': 'tiene_propietarios', 
        'm3p1_7a': 'n_propietarios_total', 
        'm3p1_7b': 'n_propietarios_hombres', 
        'm3p1_7c': 'n_propietarios_mujeres'
        }

      mod2_1.rename(columns=columns, inplace=True)
      mod2_1['company_id'] = mod2_1['company_id'].astype(int)

      # 02_MÓDULO_I_II_III_2
      mod2_2 = mod2_2[[
        'iruc', 'm3p1_8', 'm3p1_8a', 'm3p1_8b', 'm3p1_8c', 'm3p1_9', 'm3p1_9a', 'm3p1_9b',
        'm3p1_9c', 'm3p1_10', 'm3p1_10a', 'm3p1_10b', 'm3p1_10c', 'm3p1_11', 'm3p1_11a',
        'm3p1_11b', 'm3p1_11c', 'm3p1_12', 'm3p1_12a', 'm3p1_12b', 'm3p1_12c', 'm3p1_13',
        'm3p1_13a', 'm3p1_13b', 'm3p1_13c', 'm3p2_1', 'm3p2_2', 'm3p2_3', 'm3p2_4', 'm3p2_5',
        'm3p2_6', 'm3p2_7', 'm3p2_8', 'm3p5', 'm3p6', 'm3p7', 'm3p8_1', 'm3p8_2', 'm3p8_3',
        'm3p8_4', 'm3p8_5', 'm3p9', 'm3p10_1', 'm3p10_2', 'm3p10_3', 'm3p10_4', 'm3p10_5',
        'm3p10_6', 'm3p10_7', 'm3p13', 'm3p14_1', 'm3p14_2', 'm3p14_3', 'm3p14_4', 'm3p14_5',
        'm3p14_6', 'm3p14_7', 'm3p14_8', 'm3p14_9', 'm3p14_10', 'm3p14_11', 'm3p15_1', 'm3p15_2',
        'm3p15_3', 'm3p15_4', 'm3p15_5', 'm3p20', 'm3p21_1', 'm3p21_2', 'm3p21_3', 'm3p21_4',
        'm3p21_5', 'm3p21_6', 'm3p21_7', 'm3p21_8', 'm3p21_9', 'm3p21_10', 'm3p26_1', 'm3p26_2',
        'm3p26_3', 'm3p26_4', 'm3p26_5', 'm3p26_6', 'm3p26_7', 'm3p26_8', 'm3p30', 'm3p31', 'm3p32'
        ]].copy()

      columns = {
        'iruc': 'company_id',
        'm3p1_8': 'tiene_familiares_no_remunerados', # 1,0
        'm3p1_8a': 'n_familiares_no_remunerados_total', # measure
        'm3p1_8b': 'n_familiares_no_remunerados_hombres', # measure
        'm3p1_8c': 'n_familiares_no_remunerados_mujeres', # measure
        'm3p1_9': 'tiene_practicantes', # 1,0
        'm3p1_9a': 'n_practicantes_total', # measure
        'm3p1_9b': 'n_practicantes_hombres', # measure
        'm3p1_9c': 'n_practicantes_mujeres', # measure
        'm3p1_10': 'tiene_personal_servicios_honorarios', # 1,0
        'm3p1_10a': 'n_personal_servicios_honorarios_total', # measure
        'm3p1_10b': 'n_personal_servicios_honorarios_hombres', # measure
        'm3p1_10c': 'n_personal_servicios_honorarios_mujeres', # measure
        'm3p1_11': 'tiene_comisionistas', # 1,0
        'm3p1_11a': 'n_comisionistas_total', # measure
        'm3p1_11b': 'n_comisionistas_hombres', # measure
        'm3p1_11c': 'n_comisionistas_mujeres', # measure
        'm3p1_12': 'tiene_total_7_8_9_10_11', # 1,0
        'm3p1_12a': 'n_total_7_8_9_10_11_total', # measure
        'm3p1_12b': 'n_total_7_8_9_10_11_hombres', # measure
        'm3p1_12c': 'n_total_7_8_9_10_11_mujeres', # measure
        'm3p1_13': 'tiene_personal_servicios_intermediacion', # 1,0
        'm3p1_13a': 'n_personal_servicios_intermediacion_total', # measure
        'm3p1_13b': 'n_personal_servicios_intermediacion_hombres', # measure
        'm3p1_13c': 'n_personal_servicios_intermediacion_mujeres', # measure
        'm3p2_1': 'n_trabajadores_postgrado', # measure
        'm3p2_2': 'n_trabajadores_universitaria_completa', # measure
        'm3p2_3': 'n_trabajadores_universitaria_incompleta', # measure
        'm3p2_4': 'n_trabajadores_tecnico_completa', # measure
        'm3p2_5': 'n_trabajadores_tecnico_incompleta', # measure
        'm3p2_6': 'n_trabajadores_secundaria_primaria', # measure
        'm3p2_7': 'n_trabajadores_inicial_sin_nivel', # measure
        'm3p2_8': 'n_nivel_estudios_total', # measure
        'm3p5': 'conoce_modalidad_teletrabajo', # options
        'm3p6': 'teletrabajo_adecuado_empresa', # options
        'm3p7': 'entrega_incentivos_trabajadores_buenos_resultados', # options
        'm3p8_1': 'entrega_incentivos_economicos', # 1,0
        'm3p8_2': 'otorga_ascensos', # 1,0
        'm3p8_3': 'otorga_capacitaciones', # 1,0
        'm3p8_4': 'otorga_viajes', # 1,0
        'm3p8_5': 'otorga_otro_incentivo', # 1,0
        'm3p9': 'requiere_contratar_personal_2014',
        'm3p10_1': 'contratar_personal_innovacion_teconologica', # 1,0
        'm3p10_2': 'contratar_personal_estacionalidad', # 1,0
        'm3p10_3': 'contratar_personal_renuncia_personal', # 1,0
        'm3p10_4': 'contratar_personal_nueva_linea_negocio', # 1,0
        'm3p10_5': 'contratar_personal_expansion_empresa', # 1,0
        'm3p10_6': 'contratar_personal_jubilacion', # 1,0
        'm3p10_7': 'contratar_personal_otro', # 1,0
        'm3p13': 'dificultad_contratar_trabajadores_2014', # options
        'm3p14_1': 'dificultad_contratar_escasez_postulante', # 1,0
        'm3p14_2': 'dificultad_contratar_formacion_academica', # 1,0
        'm3p14_3': 'dificultad_contratar_experiencia', # 1,0
        'm3p14_4': 'dificultad_contratar_habilidades_personales_deseadas', # 1,0
        'm3p14_5': 'dificultad_contratar_informacion_confiable_postulante', # 1,0
        'm3p14_6': 'dificultad_contratar_periodo_contrato', # 1,0
        'm3p14_7': 'dificultad_contratar_remuneraciones', # 1,0
        'm3p14_8': 'dificultad_contratar_destino_geografico', # 1,0
        'm3p14_9': 'dificultad_contratar_jornada_laboral', # 1,0
        'm3p14_10': 'dificultad_contratar_busqueda_postulantes', # 1,0
        'm3p14_11': 'dificultad_contratar_otro', # 1,0
        'm3p15_1': 'dificil_conseguir_directivo_general', # 1,0
        'm3p15_2': 'dificil_conseguir_profesionales', # 1,0
        'm3p15_3': 'dificil_conseguir_tecnicos', # 1,0
        'm3p15_4': 'dificil_conseguir_operarios_calificados', # 1,0
        'm3p15_5': 'dificil_conseguir_operarios_no_calificados', # 1,0
        'm3p20': 'trabajadores_capacitacion_2014', # options
        'm3p21_1': 'brinda_capacitacion_centros_formacion_sectoriales', # 1,0
        'm3p21_2': 'brinda_capacitacion_universidad_instituto_publico', # 1,0
        'm3p21_3': 'brinda_capacitacion_universidad_instituto_privado', # 1,0
        'm3p21_4': 'brinda_capacitacion_institucion_publica', # 1,0
        'm3p21_5': 'brinda_capacitacion_centro_innovacion_tecnologica', # 1,0
        'm3p21_6': 'brinda_capacitacion_camara_comercio', # 1,0
        'm3p21_7': 'brinda_capacitacion_proveedores_empresa', # 1,0
        'm3p21_8': 'brinda_capacitacion_propia_empresa_casa_matriz', # 1,0
        'm3p21_9': 'brinda_capacitacion_instructor_externo', # 1,0
        'm3p21_10': 'brinda_capacitacion_otro', # 1,0
        'm3p26_1': 'n_trabajadores_capacitaciones_idiomas', # measure
        'm3p26_2': 'n_trabajadores_capacitaciones_gestion_empresarial', # measure
        'm3p26_3': 'n_trabajadores_capacitaciones_seguridad_salud_ocupacional', # measure
        'm3p26_4': 'n_trabajadores_capacitaciones_tic', # measure
        'm3p26_5': 'n_trabajadores_capacitaciones_habilidades_socio_emocionales', # measure
        'm3p26_6': 'n_trabajadores_capacitaciones_temas_tecnicos_productivos', # measure
        'm3p26_7': 'n_trabajadores_capacitaciones_marketing', # measure
        'm3p26_8': 'n_trabajadores_capacitaciones_otro', # measure
        'm3p30': 'razon_no_realiza_capacitaciones', # options
        'm3p31': 'conoce_credito_beneficio_tributario_gastos_capacitacion', # options
        'm3p32': 'utiliza_utilizaria_dicho_credito'
        }

      mod2_2.rename(columns=columns, inplace=True)
      mod2_2['company_id'] = mod2_2['company_id'].astype(int)

      # 03_MÓDULO_IV_1
      mod4_1 = mod4_1[[
        'iruc', 'm4p1', 'm4p2', 'm4p8', 'm4p9', 'm4p16', 'm4p17', 'm4p18', 
        'm4p19', 'm4p20_1', 'm4p20_2', 'm4p20_3', 'm4p20_4', 'm4p20_5',
        'm4p31', 'm4p32_1', 'm4p32_2', 'm4p32_3', 'm4p32_4', 'm4p32_5', 
        'm4p32_6', 'm4p33', 'm4p46', 'm4p47', 'm4p53', 'm4p54'
        ]].copy()

      columns = {
        'iruc': 'company_id',
        'm4p1': 'modalidad_realizar_compras_2014', # options
        'm4p2': 'publicidad_conoce_proveedores', # options
        'm4p8': 'pedido_proveedor_internacional_2014', # options
        'm4p9': 'razon_pedido_insumos_proveedor_internacional_2014', # options
        'm4p16': 'razon_no_realiza_compras_proveedor_internacional', # options
        'm4p17': 'realiza_compras_internet_empresa_2014', # options
        'm4p18': 'portentaje_total_compras_internet', # measure
        'm4p19': 'frecuencia_compras_internet_2014', # options
        'm4p20_1': 'medio_pago_compras_internet_tarjeta_credito', # 1,0
        'm4p20_2': 'medio_pago_compras_internet_transferencia', # 1,0
        'm4p20_3': 'medio_pago_compras_internet_billetera_electronica', # 1,0 E-Wallet, Paypal
        'm4p20_4': 'medio_pago_compras_internet_contra_entrega_terminal_de_pago', # 1,0
        'm4p20_5': 'medio_pago_compras_internet_otro', # 1,0
        'm4p31': 'tecnologia_predominante_proceso_produccion', # options
        'm4p32_1': 'fuente_energia_utiliza_electricidad', # 1,0
        'm4p32_2': 'fuente_energia_utiliza_diesel', # 1,0
        'm4p32_3': 'fuente_energia_utiliza_carbon', # 1,0
        'm4p32_4': 'fuente_energia_utiliza_gas_natural', # 1,0
        'm4p32_5': 'fuente_energia_utiliza_gas_licuado', # 1,0
        'm4p32_6': 'fuente_energia_utiliza_otro', # 1,0
        'm4p33': 'porcentaje_utiliza_capacidad_instalada_2014', # measure
        'm4p46': 'modalidad_venta_2014', # options
        'm4p47': 'realiza_estudios_mercado_para_promocion_producto_servicio_2014', # options
        'm4p53': 'realiza_ventas_internet_2014', # options
        'm4p54': 'porcentaje_ventas_internet_2014' # measure
      }

      mod4_1.rename(columns=columns, inplace=True)
      mod4_1['company_id'] = mod4_1['company_id'].astype(int)

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
        'm4p70_15_o': 'contrato_servicios_otro_respuesta_abierta'
        } # texto

      mod4_2.rename(columns=columns, inplace=True)
      mod4_2['company_id'] = mod4_2['company_id'].astype(int)

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

      # 04_MÓDULO_V_VI_2
      mod5_2 = mod5_2[[
        'iruc', 'm6p15', 'm6p19', 'm6p24_1', 'm6p24_2', 'm6p24_3', 'm6p24_4',
        'm6p24_5', 'm6p24_6', 'm6p24_7', 'm6p24_8', 'm6p24_9', 'm6p24_10',
        'm6p24_11', 'm6p24_12'
        ]].copy()

      columns = {
        'iruc': 'company_id',
        'm6p15': 'utiliza_servicio_correo_postal_2014', # options
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

      # transform ranking options to 0, 1, nan, 0 does not appear as a valid option
      for col in [
        'm8p53_1', 'm8p53_2', 'm8p53_3', 'm8p53_4', 'm8p53_5', 'm8p53_6', 'm8p53_7', 'm8p53_8',
        'm8p53_9', 'm8p53_10','m8p53_11', 'm8p53_12', 'm8p53_13', 'm8p53_14', 'm8p53_15', 'm8p53_16',
        'm8p53_17', 'm8p53_18', 'm8p53_19', 'm8p53_20', 'm8p53_21', 'm8p53_22'
        ]:
        mod6.loc[(~mod6[col].isna()) & (mod6[col] != 0), col] = 1

      mod6.rename(columns=columns, inplace=True)
      mod6['company_id'] = mod6['company_id'].astype(int)

      return df, mod2_1, mod2_2, mod4_1, mod4_2, mod5_1, mod5_2, mod6

class JoinStep(PipelineStep):
    def run_step(self, prev, params):
      df, mod2_1, mod2_2, mod4_1, mod4_2, mod5_1, mod5_2, mod6 = prev

      for dataset in [mod2_1, mod2_2, mod4_1, mod4_2, mod5_1, mod5_2, mod6]:
         df = df.join(dataset, on='company_id', rsuffix='_joined')
         df.drop(columns=['company_id_joined'], inplace=True)
         print(df.shape)

      df['year'] = 2015

      print(list(df.columns))

      return df

class EncuestaEmpresasPipeline(EasyPipeline):
    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        """dtypes = {
            'time_id':         'UInt32',
            'country_id':      'String',
            'trade_flow_id':   'UInt8',
            'department_id':   'UInt8',
            'hs_master_id':    'UInt32',
            'hs_original_id':  'String',
            'hs_revision':     'String',
            'company_id':      'UInt32',
            'municipality_id': 'UInt32',
            'value':           'UInt64',
        }"""

        read_step = ReadStep()
        transform_step = TransformStep()
        join_step = JoinStep()
        load_step = LoadStep(
          'encuesta_empresas', connector=db_connector, if_exists='drop',
          pk=['district_id', 'company_id', 'year'] #, dtype=dtypes
          , nullable_list=['ciiu_rev4', 'ciiu_rev4_a', 'ciiu_rev_b']
        )

        return [read_step, transform_step, join_step, load_step]

if __name__ == '__main__':
   pp = EncuestaEmpresasPipeline()
   pp.run({})