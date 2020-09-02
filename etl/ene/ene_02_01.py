import pandas as pd
from simpledbf import Dbf5
from bamboo_lib.models import PipelineStep


class Module21Step(PipelineStep):
    def run_step(self, prev, params):

      df = prev

      file_data = '../../data/ene/02_MÓDULO_I_II_III_1.dbf'

      dbf = Dbf5(file_data, codec='latin-1')
      mod2_1 = dbf.to_dataframe()
      mod2_1.columns = [x.lower() for x in mod2_1.columns]

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
      mod2_1['plan_negocios'] = mod2_1['plan_negocios'].astype(pd.Int16Dtype())
      mod2_1['credito_inicio_operaciones'] = mod2_1['credito_inicio_operaciones'].astype(pd.Int16Dtype())                           
      mod2_1['institucion_credito'] = mod2_1['institucion_credito'].astype(pd.Int16Dtype())                                   
      mod2_1['areas_funcionales_identificar'] = mod2_1['areas_funcionales_identificar'].astype(pd.Int16Dtype())                        
      mod2_1['recursos_humanos'] = mod2_1['recursos_humanos'].astype(pd.Int16Dtype())                                      
      mod2_1['logistica_aprovisionamiento'] = mod2_1['logistica_aprovisionamiento'].astype(pd.Int16Dtype())                           
      mod2_1['comercializacion'] = mod2_1['comercializacion'].astype(pd.Int16Dtype())                                     
      mod2_1['contabilidad'] = mod2_1['contabilidad'].astype(pd.Int16Dtype())                                          
      mod2_1['produccion'] = mod2_1['produccion'].astype(pd.Int16Dtype())                                            
      mod2_1['direccion_gerencia'] = mod2_1['direccion_gerencia'].astype(pd.Int16Dtype())                                    
      mod2_1['area_legal'] = mod2_1['area_legal'].astype(pd.Int16Dtype())                                           
      mod2_1['soporte_informatico'] = mod2_1['soporte_informatico'].astype(pd.Int16Dtype())                                   
      mod2_1['otro'] = mod2_1['otro'].astype(pd.Int16Dtype())                                                  
      mod2_1['participa_mercado_internacional'] = mod2_1['participa_mercado_internacional'].astype(pd.Int16Dtype())                      
      mod2_1['participa_mercado_nacional'] = mod2_1['participa_mercado_nacional'].astype(pd.Int16Dtype())                            
      mod2_1['participa_mercado_local'] = mod2_1['participa_mercado_local'].astype(pd.Int16Dtype())                              
      mod2_1['mercado_principal'] = mod2_1['mercado_principal'].astype(pd.Int16Dtype())                                     
      mod2_1['considera_competencia_existe'] = mod2_1['considera_competencia_existe'].astype(pd.Int16Dtype())                         
      mod2_1['competencia_informal_precio'] = mod2_1['competencia_informal_precio'].astype(pd.Int16Dtype())                           
      mod2_1['competencia_informal_calidad'] = mod2_1['competencia_informal_calidad'].astype(pd.Int16Dtype())                         
      mod2_1['competencia_informal_grado_diferenciacion'] = mod2_1['competencia_informal_grado_diferenciacion'].astype(pd.Int16Dtype())            
      mod2_1['competencia_informal_tiempo_entrega'] = mod2_1['competencia_informal_tiempo_entrega'].astype(pd.Int16Dtype())                  
      mod2_1['competencia_informal_promociones_descuentos'] = mod2_1['competencia_informal_promociones_descuentos'].astype(pd.Int16Dtype())          
      mod2_1['competencia_informal_servicio_cliente'] = mod2_1['competencia_informal_servicio_cliente'].astype(pd.Int16Dtype())                 
      mod2_1['competencia_informal_otro'] = mod2_1['competencia_informal_otro'].astype(pd.Int16Dtype())                            
      mod2_1['pertenece_organizacion_fin_empresarial'] = mod2_1['pertenece_organizacion_fin_empresarial'].astype(pd.Int16Dtype())               
      mod2_1['anio_incorporacion_organizacion_fin_empresarial'] = mod2_1['anio_incorporacion_organizacion_fin_empresarial'].astype(pd.Int16Dtype())
      mod2_1['tipo_organizacion_fin_empresarial'] = mod2_1['tipo_organizacion_fin_empresarial'].astype(pd.Int16Dtype())                     
      mod2_1['acceso_info_negociar_proveedores'] = mod2_1['acceso_info_negociar_proveedores'].astype(pd.Int16Dtype())                       
      mod2_1['acceso_info_servicios_financieros'] = mod2_1['acceso_info_servicios_financieros'].astype(pd.Int16Dtype())                     
      mod2_1['acceso_info_acceso_mercados'] = mod2_1['acceso_info_acceso_mercados'].astype(pd.Int16Dtype())                          
      mod2_1['acceso_info_empresarial'] = mod2_1['acceso_info_empresarial'].astype(pd.Int16Dtype())                               
      mod2_1['acceso_capacitacion_asis_tecnica'] = mod2_1['acceso_capacitacion_asis_tecnica'].astype(pd.Int16Dtype())                     
      mod2_1['acceso_info_vigilancia_limpieza'] = mod2_1['acceso_info_vigilancia_limpieza'].astype(pd.Int16Dtype())                       
      mod2_1['acceso_info_infraestructura'] = mod2_1['acceso_info_infraestructura'].astype(pd.Int16Dtype())                          
      mod2_1['acceso_info_otro'] = mod2_1['acceso_info_otro'].astype(pd.Int16Dtype())                                      
      mod2_1['utilidad_relacion_asociatividad_2014'] = mod2_1['utilidad_relacion_asociatividad_2014'].astype(pd.Int16Dtype())                 
      mod2_1['no_pertenece_organizacion_agrupacion'] = mod2_1['no_pertenece_organizacion_agrupacion'].astype(pd.Int16Dtype())                  
      mod2_1['local_electricidad_publica'] = mod2_1['local_electricidad_publica'].astype(pd.Int16Dtype())                           
      mod2_1['interrupciones_electricas_programadas'] = mod2_1['interrupciones_electricas_programadas'].astype(pd.Int16Dtype())                
      mod2_1['interrupciones_electricas_no_programadas'] = mod2_1['interrupciones_electricas_no_programadas'].astype(pd.Int16Dtype())             
      mod2_1['dispo_servicio_electrico_horas_dias'] = mod2_1['dispo_servicio_electrico_horas_dias'].astype(pd.Int16Dtype())                  
      mod2_1['demora_no_atienden_reclamos_servicio_electrico'] = mod2_1['demora_no_atienden_reclamos_servicio_electrico'].astype(pd.Int16Dtype())       
      mod2_1['costo_elevado_servicio_electrico'] = mod2_1['costo_elevado_servicio_electrico'].astype(pd.Int16Dtype())                     
      mod2_1['intensidad_corriente_electrica_variable'] = mod2_1['intensidad_corriente_electrica_variable'].astype(pd.Int16Dtype())              
      mod2_1['errores_facturacion_servicio_electrico'] = mod2_1['errores_facturacion_servicio_electrico'].astype(pd.Int16Dtype())               
      mod2_1['otro_problema_servicio_electrico'] = mod2_1['otro_problema_servicio_electrico'].astype(pd.Int16Dtype())                     
      mod2_1['no_problema_servicio_electrico'] = mod2_1['no_problema_servicio_electrico'].astype(pd.Int16Dtype())                        
      mod2_1['cuenta_agua_red_publica'] = mod2_1['cuenta_agua_red_publica'].astype(pd.Int16Dtype())                              
      mod2_1['interrupciones_suministro_agua_programadas'] = mod2_1['interrupciones_suministro_agua_programadas'].astype(pd.Int16Dtype())           
      mod2_1['interrupciones_suministro_agua_no_programadas'] = mod2_1['interrupciones_suministro_agua_no_programadas'].astype(pd.Int16Dtype())        
      mod2_1['dispo_suministro_agua_horas_dias'] = mod2_1['dispo_suministro_agua_horas_dias'].astype(pd.Int16Dtype())                     
      mod2_1['demora_no_atienden_reclamos_suministro_agua'] = mod2_1['demora_no_atienden_reclamos_suministro_agua'].astype(pd.Int16Dtype())          
      mod2_1['costo_elevado_suministro_agua'] = mod2_1['costo_elevado_suministro_agua'].astype(pd.Int16Dtype())                         
      mod2_1['intensidad_suministro_agua_variable'] = mod2_1['intensidad_suministro_agua_variable'].astype(pd.Int16Dtype())                  
      mod2_1['errores_facturacion_servicio_agua'] = mod2_1['errores_facturacion_servicio_agua'].astype(pd.Int16Dtype())                     
      mod2_1['mala_calidad_agua'] = mod2_1['mala_calidad_agua'].astype(pd.Int16Dtype())                                    
      mod2_1['otro_problema_servicio_suministro_agua'] = mod2_1['otro_problema_servicio_suministro_agua'].astype(pd.Int16Dtype())                
      mod2_1['tiene_ejecutivos'] = mod2_1['tiene_ejecutivos'].astype(pd.Int16Dtype())                                     
      mod2_1['n_ejecutivos_total'] = mod2_1['n_ejecutivos_total'].astype(pd.Int32Dtype())                                    
      mod2_1['n_ejecutivos_hombres'] = mod2_1['n_ejecutivos_hombres'].astype(pd.Int32Dtype())                                  
      mod2_1['n_ejecutivos_mujeres'] = mod2_1['n_ejecutivos_mujeres'].astype(pd.Int32Dtype())                                 
      mod2_1['tiene_empleados_permanentes'] = mod2_1['tiene_empleados_permanentes'].astype(pd.Int16Dtype())                          
      mod2_1['n_empleados_permanentes_total'] = mod2_1['n_empleados_permanentes_total'].astype(pd.Int32Dtype())                        
      mod2_1['n_empleados_permanentes_hombres'] = mod2_1['n_empleados_permanentes_hombres'].astype(pd.Int32Dtype())                       
      mod2_1['n_empleados_permanentes_mujeres'] = mod2_1['n_empleados_permanentes_mujeres'].astype(pd.Int32Dtype())                      
      mod2_1['tiene_obreros_permanentes'] = mod2_1['tiene_obreros_permanentes'].astype(pd.Int16Dtype())                            
      mod2_1['n_obreros_permanentes_total'] = mod2_1['n_obreros_permanentes_total'].astype(pd.Int32Dtype())                          
      mod2_1['n_obreros_permanentes_hombres'] = mod2_1['n_obreros_permanentes_hombres'].astype(pd.Int32Dtype())                         
      mod2_1['n_obreros_permanentes_mujeres'] = mod2_1['n_obreros_permanentes_mujeres'].astype(pd.Int32Dtype())                        
      mod2_1['tiene_empleados_eventuales'] = mod2_1['tiene_empleados_eventuales'].astype(pd.Int16Dtype())                           
      mod2_1['n_empleados_eventuales_total'] = mod2_1['n_empleados_eventuales_total'].astype(pd.Int32Dtype())                         
      mod2_1['n_empleados_eventuales_hombres'] = mod2_1['n_empleados_eventuales_hombres'].astype(pd.Int32Dtype())                        
      mod2_1['n_empleados_eventuales_mujeres'] = mod2_1['n_empleados_eventuales_mujeres'].astype(pd.Int32Dtype())                       
      mod2_1['tiene_obreros_eventuales'] = mod2_1['tiene_obreros_eventuales'].astype(pd.Int16Dtype())                             
      mod2_1['n_obreros_eventuales_total'] = mod2_1['n_obreros_eventuales_total'].astype(pd.Int32Dtype())                           
      mod2_1['n_obreros_eventuales_hombres'] = mod2_1['n_obreros_eventuales_hombres'].astype(pd.Int32Dtype())                          
      mod2_1['n_obreros_eventuales_mujeres'] = mod2_1['n_obreros_eventuales_mujeres'].astype(pd.Int32Dtype())                         
      mod2_1['tiene_total_1_2_3_4_5'] = mod2_1['tiene_total_1_2_3_4_5'].astype(pd.Int16Dtype())                                                             
      mod2_1['n_total_1_2_3_4_5_hombres'] = mod2_1['n_total_1_2_3_4_5_hombres'].astype(pd.Int32Dtype())                             
      mod2_1['n_total_1_2_3_4_5_mujeres'] = mod2_1['n_total_1_2_3_4_5_mujeres'].astype(pd.Int32Dtype())                            
      mod2_1['tiene_propietarios'] = mod2_1['tiene_propietarios'].astype(pd.Int16Dtype())                                   
      mod2_1['n_propietarios_total'] = mod2_1['n_propietarios_total'].astype(pd.Int32Dtype())                                  
      mod2_1['n_propietarios_hombres'] = mod2_1['n_propietarios_hombres'].astype(pd.Int32Dtype())                               
      mod2_1['n_propietarios_mujeres'] = mod2_1['n_propietarios_mujeres'].astype(pd.Int32Dtype())

      return df, mod2_1
