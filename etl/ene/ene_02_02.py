import pandas as pd
from simpledbf import Dbf5
from bamboo_lib.models import PipelineStep


class Module22Step(PipelineStep):
    def run_step(self, prev, params):

        df, mod2_1 = prev

        file_data = '../../data/ene/02_MÓDULO_I_II_III_2.dbf'

        dbf = Dbf5(file_data, codec='latin-1')
        mod2_2 = dbf.to_dataframe()
        mod2_2.columns = [x.lower() for x in mod2_2.columns]

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
            'm3p1_8': 'tiene_familiares_no_remunerados',  # 1,0
            'm3p1_8a': 'n_familiares_no_remunerados_total',  # measure
            'm3p1_8b': 'n_familiares_no_remunerados_hombres',  # measure
            'm3p1_8c': 'n_familiares_no_remunerados_mujeres',  # measure
            'm3p1_9': 'tiene_practicantes',  # 1,0
            'm3p1_9a': 'n_practicantes_total',  # measure
            'm3p1_9b': 'n_practicantes_hombres',  # measure
            'm3p1_9c': 'n_practicantes_mujeres',  # measure
            'm3p1_10': 'tiene_personal_servicios_honorarios',  # 1,0
            'm3p1_10a': 'n_personal_servicios_honorarios_total',  # measure
            'm3p1_10b': 'n_personal_servicios_honorarios_hombres',  # measure
            'm3p1_10c': 'n_personal_servicios_honorarios_mujeres',  # measure
            'm3p1_11': 'tiene_comisionistas',  # 1,0
            'm3p1_11a': 'n_comisionistas_total',  # measure
            'm3p1_11b': 'n_comisionistas_hombres',  # measure
            'm3p1_11c': 'n_comisionistas_mujeres',  # measure
            'm3p1_12': 'tiene_total_7_8_9_10_11',  # 1,0
            'm3p1_12a': 'n_total_7_8_9_10_11_total',  # measure
            'm3p1_12b': 'n_total_7_8_9_10_11_hombres',  # measure
            'm3p1_12c': 'n_total_7_8_9_10_11_mujeres',  # measure
            'm3p1_13': 'tiene_personal_servicios_intermediacion',  # 1,0
            'm3p1_13a': 'n_personal_servicios_intermediacion_total',  # measure
            'm3p1_13b': 'n_personal_servicios_intermediacion_hombres',  # measure
            'm3p1_13c': 'n_personal_servicios_intermediacion_mujeres',  # measure
            'm3p2_1': 'n_trabajadores_postgrado',  # measure
            'm3p2_2': 'n_trabajadores_universitaria_completa',  # measure
            'm3p2_3': 'n_trabajadores_universitaria_incompleta',  # measure
            'm3p2_4': 'n_trabajadores_tecnico_completa',  # measure
            'm3p2_5': 'n_trabajadores_tecnico_incompleta',  # measure
            'm3p2_6': 'n_trabajadores_secundaria_primaria',  # measure
            'm3p2_7': 'n_trabajadores_inicial_sin_nivel',  # measure
            'm3p2_8': 'n_nivel_estudios_total',  # measure
            'm3p5': 'conoce_modalidad_teletrabajo',  # options
            'm3p6': 'teletrabajo_adecuado_empresa',  # options
            'm3p7': 'entrega_incentivos_trabajadores_buenos_resultados',  # options
            'm3p8_1': 'entrega_incentivos_economicos',  # 1,0
            'm3p8_2': 'otorga_ascensos',  # 1,0
            'm3p8_3': 'otorga_capacitaciones',  # 1,0
            'm3p8_4': 'otorga_viajes',  # 1,0
            'm3p8_5': 'otorga_otro_incentivo',  # 1,0
            'm3p9': 'requiere_contratar_personal_2014',
            'm3p10_1': 'contratar_personal_innovacion_teconologica',  # 1,0
            'm3p10_2': 'contratar_personal_estacionalidad',  # 1,0
            'm3p10_3': 'contratar_personal_renuncia_personal',  # 1,0
            'm3p10_4': 'contratar_personal_nueva_linea_negocio',  # 1,0
            'm3p10_5': 'contratar_personal_expansion_empresa',  # 1,0
            'm3p10_6': 'contratar_personal_jubilacion',  # 1,0
            'm3p10_7': 'contratar_personal_otro',  # 1,0
            'm3p13': 'dificultad_contratar_trabajadores_2014',  # options
            'm3p14_1': 'dificultad_contratar_escasez_postulante',  # 1,0
            'm3p14_2': 'dificultad_contratar_formacion_academica',  # 1,0
            'm3p14_3': 'dificultad_contratar_experiencia',  # 1,0
            'm3p14_4': 'dificultad_contratar_habilidades_personales_deseadas',  # 1,0
            'm3p14_5': 'dificultad_contratar_informacion_confiable_postulante',  # 1,0
            'm3p14_6': 'dificultad_contratar_periodo_contrato',  # 1,0
            'm3p14_7': 'dificultad_contratar_remuneraciones',  # 1,0
            'm3p14_8': 'dificultad_contratar_destino_geografico',  # 1,0
            'm3p14_9': 'dificultad_contratar_jornada_laboral',  # 1,0
            'm3p14_10': 'dificultad_contratar_busqueda_postulantes',  # 1,0
            'm3p14_11': 'dificultad_contratar_otro',  # 1,0
            'm3p15_1': 'dificil_conseguir_directivo_general',  # 1,0
            'm3p15_2': 'dificil_conseguir_profesionales',  # 1,0
            'm3p15_3': 'dificil_conseguir_tecnicos',  # 1,0
            'm3p15_4': 'dificil_conseguir_operarios_calificados',  # 1,0
            'm3p15_5': 'dificil_conseguir_operarios_no_calificados',  # 1,0
            'm3p20': 'trabajadores_capacitacion_2014',  # options
            'm3p21_1': 'brinda_capacitacion_centros_formacion_sectoriales',  # 1,0
            'm3p21_2': 'brinda_capacitacion_universidad_instituto_publico',  # 1,0
            'm3p21_3': 'brinda_capacitacion_universidad_instituto_privado',  # 1,0
            'm3p21_4': 'brinda_capacitacion_institucion_publica',  # 1,0
            'm3p21_5': 'brinda_capacitacion_centro_innovacion_tecnologica',  # 1,0
            'm3p21_6': 'brinda_capacitacion_camara_comercio',  # 1,0
            'm3p21_7': 'brinda_capacitacion_proveedores_empresa',  # 1,0
            'm3p21_8': 'brinda_capacitacion_propia_empresa_casa_matriz',  # 1,0
            'm3p21_9': 'brinda_capacitacion_instructor_externo',  # 1,0
            'm3p21_10': 'brinda_capacitacion_otro',  # 1,0
            'm3p26_1': 'n_trabajadores_capacitaciones_idiomas',  # measure
            'm3p26_2': 'n_trabajadores_capacitaciones_gestion_empresarial',  # measure
            'm3p26_3': 'n_trabajadores_capacitaciones_seguridad_salud_ocupacional',  # measure
            'm3p26_4': 'n_trabajadores_capacitaciones_tic',  # measure
            'm3p26_5': 'n_trabajadores_capacitaciones_habilidades_socio_emocionales',  # measure
            'm3p26_6': 'n_trabajadores_capacitaciones_temas_tecnicos_productivos',  # measure
            'm3p26_7': 'n_trabajadores_capacitaciones_marketing',  # measure
            'm3p26_8': 'n_trabajadores_capacitaciones_otro',  # measure
            'm3p30': 'razon_no_realiza_capacitaciones',  # options
            'm3p31': 'conoce_credito_beneficio_tributario_gastos_capacitacion',  # options
            'm3p32': 'utiliza_utilizaria_dicho_credito'
        }

        mod2_2.rename(columns=columns, inplace=True)
        mod2_2['company_id'] = mod2_2['company_id'].astype(int)
        mod2_2['tiene_familiares_no_remunerados'] = mod2_2['tiene_familiares_no_remunerados'].astype(pd.Int16Dtype())
        mod2_2['n_familiares_no_remunerados_total'] = mod2_2['n_familiares_no_remunerados_total'].astype(pd.Int32Dtype())
        mod2_2['n_familiares_no_remunerados_hombres'] = mod2_2['n_familiares_no_remunerados_hombres'].astype(pd.Int32Dtype())
        mod2_2['n_familiares_no_remunerados_mujeres'] = mod2_2['n_familiares_no_remunerados_mujeres'].astype(pd.Int32Dtype()) 
        mod2_2['tiene_practicantes'] = mod2_2['tiene_practicantes'].astype(pd.Int16Dtype())
        mod2_2['n_practicantes_total'] = mod2_2['n_practicantes_total'].astype(pd.Int32Dtype()) 
        mod2_2['n_practicantes_hombres'] = mod2_2['n_practicantes_hombres'].astype(pd.Int32Dtype())
        mod2_2['n_practicantes_mujeres'] = mod2_2['n_practicantes_mujeres'].astype(pd.Int32Dtype()) 
        mod2_2['tiene_personal_servicios_honorarios'] = mod2_2['tiene_personal_servicios_honorarios'].astype(pd.Int16Dtype())
        mod2_2['n_personal_servicios_honorarios_total'] = mod2_2['n_personal_servicios_honorarios_total'].astype(pd.Int32Dtype())
        mod2_2['n_personal_servicios_honorarios_hombres'] = mod2_2['n_personal_servicios_honorarios_hombres'].astype(pd.Int32Dtype())
        mod2_2['n_personal_servicios_honorarios_mujeres'] = mod2_2['n_personal_servicios_honorarios_mujeres'].astype(pd.Int32Dtype()) 
        mod2_2['tiene_comisionistas'] = mod2_2['tiene_comisionistas'].astype(pd.Int16Dtype())
        mod2_2['n_comisionistas_total'] = mod2_2['n_comisionistas_total'].astype(pd.Int32Dtype()) 
        mod2_2['n_comisionistas_hombres'] = mod2_2['n_comisionistas_hombres'].astype(pd.Int32Dtype())
        mod2_2['n_comisionistas_mujeres'] = mod2_2['n_comisionistas_mujeres'].astype(pd.Int32Dtype()) 
        mod2_2['tiene_total_7_8_9_10_11'] = mod2_2['tiene_total_7_8_9_10_11'].astype(pd.Int16Dtype())
        mod2_2['n_total_7_8_9_10_11_total'] = mod2_2['n_total_7_8_9_10_11_total'].astype(pd.Int32Dtype()) 
        mod2_2['n_total_7_8_9_10_11_hombres'] = mod2_2['n_total_7_8_9_10_11_hombres'].astype(pd.Int32Dtype())
        mod2_2['n_total_7_8_9_10_11_mujeres'] = mod2_2['n_total_7_8_9_10_11_mujeres'].astype(pd.Int32Dtype())
        mod2_2['tiene_personal_servicios_intermediacion'] = mod2_2['tiene_personal_servicios_intermediacion'].astype(pd.Int16Dtype())
        mod2_2['n_personal_servicios_intermediacion_total'] = mod2_2['n_personal_servicios_intermediacion_total'].astype(pd.Int32Dtype())
        mod2_2['n_personal_servicios_intermediacion_hombres'] = mod2_2['n_personal_servicios_intermediacion_hombres'].astype(pd.Int32Dtype())
        mod2_2['n_personal_servicios_intermediacion_mujeres'] = mod2_2['n_personal_servicios_intermediacion_mujeres'].astype(pd.Int32Dtype())
        mod2_2['n_trabajadores_postgrado'] = mod2_2['n_trabajadores_postgrado'].astype(pd.Int32Dtype()) 
        mod2_2['n_trabajadores_universitaria_completa'] = mod2_2['n_trabajadores_universitaria_completa'].astype(pd.Int32Dtype())
        mod2_2['n_trabajadores_universitaria_incompleta'] = mod2_2['n_trabajadores_universitaria_incompleta'].astype(pd.Int32Dtype())
        mod2_2['n_trabajadores_tecnico_completa'] = mod2_2['n_trabajadores_tecnico_completa'].astype(pd.Int32Dtype()) 
        mod2_2['n_trabajadores_tecnico_incompleta'] = mod2_2['n_trabajadores_tecnico_incompleta'].astype(pd.Int32Dtype())
        mod2_2['n_trabajadores_secundaria_primaria'] = mod2_2['n_trabajadores_secundaria_primaria'].astype(pd.Int32Dtype())
        mod2_2['n_trabajadores_inicial_sin_nivel'] = mod2_2['n_trabajadores_inicial_sin_nivel'].astype(pd.Int32Dtype()) 
        mod2_2['n_nivel_estudios_total'] = mod2_2['n_nivel_estudios_total'].astype(pd.Int32Dtype())
        mod2_2['conoce_modalidad_teletrabajo'] = mod2_2['conoce_modalidad_teletrabajo'].astype(pd.Int16Dtype()) 
        mod2_2['teletrabajo_adecuado_empresa'] = mod2_2['teletrabajo_adecuado_empresa'].astype(pd.Int16Dtype())
        mod2_2['entrega_incentivos_trabajadores_buenos_resultados'] = mod2_2['entrega_incentivos_trabajadores_buenos_resultados'].astype(pd.Int16Dtype())
        mod2_2['entrega_incentivos_economicos'] = mod2_2['entrega_incentivos_economicos'].astype(pd.Int16Dtype()) 
        mod2_2['otorga_ascensos'] = mod2_2['otorga_ascensos'].astype(pd.Int16Dtype())
        mod2_2['otorga_capacitaciones'] = mod2_2['otorga_capacitaciones'].astype(pd.Int16Dtype()) 
        mod2_2['otorga_viajes'] = mod2_2['otorga_viajes'].astype(pd.Int16Dtype()) 
        mod2_2['otorga_otro_incentivo'] = mod2_2['otorga_otro_incentivo'].astype(pd.Int16Dtype())
        mod2_2['requiere_contratar_personal_2014'] = mod2_2['requiere_contratar_personal_2014'].astype(pd.Int16Dtype())
        mod2_2['contratar_personal_innovacion_teconologica'] = mod2_2['contratar_personal_innovacion_teconologica'].astype(pd.Int16Dtype())
        mod2_2['contratar_personal_estacionalidad'] = mod2_2['contratar_personal_estacionalidad'].astype(pd.Int16Dtype())
        mod2_2['contratar_personal_renuncia_personal'] = mod2_2['contratar_personal_renuncia_personal'].astype(pd.Int16Dtype())
        mod2_2['contratar_personal_nueva_linea_negocio'] = mod2_2['contratar_personal_nueva_linea_negocio'].astype(pd.Int16Dtype())
        mod2_2['contratar_personal_expansion_empresa'] = mod2_2['contratar_personal_expansion_empresa'].astype(pd.Int16Dtype()) 
        mod2_2['contratar_personal_jubilacion'] = mod2_2['contratar_personal_jubilacion'].astype(pd.Int16Dtype())
        mod2_2['contratar_personal_otro'] = mod2_2['contratar_personal_otro'].astype(pd.Int16Dtype()) 
        mod2_2['dificultad_contratar_trabajadores_2014'] = mod2_2['dificultad_contratar_trabajadores_2014'].astype(pd.Int16Dtype())
        mod2_2['dificultad_contratar_escasez_postulante'] = mod2_2['dificultad_contratar_escasez_postulante'].astype(pd.Int16Dtype())
        mod2_2['dificultad_contratar_formacion_academica'] = mod2_2['dificultad_contratar_formacion_academica'].astype(pd.Int16Dtype())
        mod2_2['dificultad_contratar_experiencia'] = mod2_2['dificultad_contratar_experiencia'].astype(pd.Int16Dtype())
        mod2_2['dificultad_contratar_habilidades_personales_deseadas'] = mod2_2['dificultad_contratar_habilidades_personales_deseadas'].astype(pd.Int16Dtype())
        mod2_2['dificultad_contratar_informacion_confiable_postulante'] = mod2_2['dificultad_contratar_informacion_confiable_postulante'].astype(pd.Int16Dtype())
        mod2_2['dificultad_contratar_periodo_contrato'] = mod2_2['dificultad_contratar_periodo_contrato'].astype(pd.Int16Dtype())
        mod2_2['dificultad_contratar_remuneraciones'] = mod2_2['dificultad_contratar_remuneraciones'].astype(pd.Int16Dtype())
        mod2_2['dificultad_contratar_destino_geografico'] = mod2_2['dificultad_contratar_destino_geografico'].astype(pd.Int16Dtype())
        mod2_2['dificultad_contratar_jornada_laboral'] = mod2_2['dificultad_contratar_jornada_laboral'].astype(pd.Int16Dtype())
        mod2_2['dificultad_contratar_busqueda_postulantes'] = mod2_2['dificultad_contratar_busqueda_postulantes'].astype(pd.Int16Dtype())
        mod2_2['dificultad_contratar_otro'] = mod2_2['dificultad_contratar_otro'].astype(pd.Int16Dtype()) 
        mod2_2['dificil_conseguir_directivo_general'] = mod2_2['dificil_conseguir_directivo_general'].astype(pd.Int16Dtype())
        mod2_2['dificil_conseguir_profesionales'] = mod2_2['dificil_conseguir_profesionales'].astype(pd.Int16Dtype()) 
        mod2_2['dificil_conseguir_tecnicos'] = mod2_2['dificil_conseguir_tecnicos'].astype(pd.Int16Dtype())
        mod2_2['dificil_conseguir_operarios_calificados'] = mod2_2['dificil_conseguir_operarios_calificados'].astype(pd.Int16Dtype())
        mod2_2['dificil_conseguir_operarios_no_calificados'] = mod2_2['dificil_conseguir_operarios_no_calificados'].astype(pd.Int16Dtype())
        mod2_2['trabajadores_capacitacion_2014'] = mod2_2['trabajadores_capacitacion_2014'].astype(pd.Int16Dtype())
        mod2_2['brinda_capacitacion_centros_formacion_sectoriales'] = mod2_2['brinda_capacitacion_centros_formacion_sectoriales'].astype(pd.Int16Dtype())
        mod2_2['brinda_capacitacion_universidad_instituto_publico'] = mod2_2['brinda_capacitacion_universidad_instituto_publico'].astype(pd.Int16Dtype())
        mod2_2['brinda_capacitacion_universidad_instituto_privado'] = mod2_2['brinda_capacitacion_universidad_instituto_privado'].astype(pd.Int16Dtype())
        mod2_2['brinda_capacitacion_institucion_publica'] = mod2_2['brinda_capacitacion_institucion_publica'].astype(pd.Int16Dtype())
        mod2_2['brinda_capacitacion_centro_innovacion_tecnologica'] = mod2_2['brinda_capacitacion_centro_innovacion_tecnologica'].astype(pd.Int16Dtype())
        mod2_2['brinda_capacitacion_camara_comercio'] = mod2_2['brinda_capacitacion_camara_comercio'].astype(pd.Int16Dtype())
        mod2_2['brinda_capacitacion_proveedores_empresa'] = mod2_2['brinda_capacitacion_proveedores_empresa'].astype(pd.Int16Dtype())
        mod2_2['brinda_capacitacion_propia_empresa_casa_matriz'] = mod2_2['brinda_capacitacion_propia_empresa_casa_matriz'].astype(pd.Int16Dtype())
        mod2_2['brinda_capacitacion_instructor_externo'] = mod2_2['brinda_capacitacion_instructor_externo'].astype(pd.Int16Dtype()) 
        mod2_2['brinda_capacitacion_otro'] = mod2_2['brinda_capacitacion_otro'].astype(pd.Int16Dtype())
        mod2_2['n_trabajadores_capacitaciones_idiomas'] = mod2_2['n_trabajadores_capacitaciones_idiomas'].astype(pd.Int32Dtype())
        mod2_2['n_trabajadores_capacitaciones_gestion_empresarial'] = mod2_2['n_trabajadores_capacitaciones_gestion_empresarial'].astype(pd.Int32Dtype())
        mod2_2['n_trabajadores_capacitaciones_seguridad_salud_ocupacional'] = mod2_2['n_trabajadores_capacitaciones_seguridad_salud_ocupacional'].astype(pd.Int32Dtype())
        mod2_2['n_trabajadores_capacitaciones_tic'] = mod2_2['n_trabajadores_capacitaciones_tic'].astype(pd.Int32Dtype())
        mod2_2['n_trabajadores_capacitaciones_habilidades_socio_emocionales'] = mod2_2['n_trabajadores_capacitaciones_habilidades_socio_emocionales'].astype(pd.Int32Dtype())
        mod2_2['n_trabajadores_capacitaciones_temas_tecnicos_productivos'] = mod2_2['n_trabajadores_capacitaciones_temas_tecnicos_productivos'].astype(pd.Int32Dtype())
        mod2_2['n_trabajadores_capacitaciones_marketing'] = mod2_2['n_trabajadores_capacitaciones_marketing'].astype(pd.Int32Dtype())
        mod2_2['n_trabajadores_capacitaciones_otro'] = mod2_2['n_trabajadores_capacitaciones_otro'].astype(pd.Int32Dtype()) 
        mod2_2['razon_no_realiza_capacitaciones'] = mod2_2['razon_no_realiza_capacitaciones'].astype(pd.Int16Dtype())
        mod2_2['conoce_credito_beneficio_tributario_gastos_capacitacion'] = mod2_2['conoce_credito_beneficio_tributario_gastos_capacitacion'].astype(pd.Int16Dtype())
        mod2_2['utiliza_utilizaria_dicho_credito'] = mod2_2['utiliza_utilizaria_dicho_credito'].astype(pd.Int16Dtype()) 

        return df, mod2_1, mod2_2
