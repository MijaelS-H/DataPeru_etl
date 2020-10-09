import numpy as np
import os
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.helpers import grab_parent_dir
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from static import DTYPES, NULLABLE_LIST, RENAME_COLUMNS, SELECTED_COLUMNS, VARIABLES_DICT

path = grab_parent_dir('../../') + "/datasets/20201001/02. Información Censos (01-10-2020)/01 RENAMU - MUNICIPALIDADES/"

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.DataFrame()

        for folder in os.listdir(path):
            if int(folder[-4:]) >= 2019: 
                _df = pd.DataFrame()
                for subfolder in os.listdir('{}{}'.format(path, folder)):
                    for filename in os.listdir('{}{}/{}'.format(path, folder, subfolder)):
                        if filename.endswith('.sav'):
                            temp = pd.read_spss('{}{}/{}/{}'.format(path, folder, subfolder, filename))
                            temp = temp.replace('', np.nan).dropna(how='all')
                            temp.rename(
                                columns = {
                                    'Ubigeo': 'idmunici',
                                    'idimunici': 'idmunici'
                                },
                                inplace = True
                            )
                    
                            temp = temp.loc[:, temp.columns.isin(SELECTED_COLUMNS)].copy()
                            temp.drop_duplicates(inplace = True)
                    
                            if _df.shape[0] != 0:
                                _df = pd.merge(_df, temp, on = 'idmunici', suffixes = ('', '_drop'))
                                _df = _df.iloc[:, ~_df.columns.str.contains('_drop')]
                            else:
                                _df = temp
        
                _df['year'] = int(folder[-4:])
                df = df.append(_df)

        for item in VARIABLES_DICT:
            df[item] = df[item].replace(VARIABLES_DICT[item])

        df.rename(columns = RENAME_COLUMNS, inplace = True)

        df['requiere_asistencia_tecnica_acondicionamiento_territorial'] = df['requiere_asistencia_tecnica_acondicionamiento_territorial'].astype(pd.Int16Dtype())
        df['requiere_asistencia_tecnica_acondicionamiento_territorial'] = df['requiere_asistencia_tecnica_acondicionamiento_territorial'].astype(pd.Int16Dtype())
        df['requiere_asistencia_tecnica_administacion_tributaria'] = df['requiere_asistencia_tecnica_administacion_tributaria'].astype(pd.Int16Dtype())
        df['requiere_asistencia_tecnica_catastro_urbano_rural'] = df['requiere_asistencia_tecnica_catastro_urbano_rural'].astype(pd.Int16Dtype())
        df['requiere_asistencia_tecnica_control_gubernamental'] = df['requiere_asistencia_tecnica_control_gubernamental'].astype(pd.Int16Dtype())
        df['requiere_asistencia_tecnica_gestion_financiera'] = df['requiere_asistencia_tecnica_gestion_financiera'].astype(pd.Int16Dtype())
        df['requiere_asistencia_tecnica_evaluacion_riesgo'] = df['requiere_asistencia_tecnica_evaluacion_riesgo'].astype(pd.Int16Dtype())
        df['requiere_asistencia_tecnica_formulacion_proyectos'] = df['requiere_asistencia_tecnica_formulacion_proyectos'].astype(pd.Int16Dtype())
        df['requiere_asistencia_tecnica_gestion_estandares_atencion'] = df['requiere_asistencia_tecnica_gestion_estandares_atencion'].astype(pd.Int16Dtype())
        df['requiere_asistencia_tecnica_gestion_desarrollo_urbano'] = df['requiere_asistencia_tecnica_gestion_desarrollo_urbano'].astype(pd.Int16Dtype())
        df['requiere_asistencia_tecnica_gobierno_electronico'] = df['requiere_asistencia_tecnica_gobierno_electronico'].astype(pd.Int16Dtype())
        df['requiere_asistencia_tecnica_indicadores_gestion'] = df['requiere_asistencia_tecnica_indicadores_gestion'].astype(pd.Int16Dtype())
        df['requiere_asistencia_tecnica_metodologia_portal_transparencia'] = df['requiere_asistencia_tecnica_metodologia_portal_transparencia'].astype(pd.Int16Dtype())
        df['requiere_asistencia_tecnica_metodologia_simplificacion'] = df['requiere_asistencia_tecnica_metodologia_simplificacion'].astype(pd.Int16Dtype())
        df['requiere_asistencia_tecnica_plan_prevencion_riesgo'] = df['requiere_asistencia_tecnica_plan_prevencion_riesgo'].astype(pd.Int16Dtype())
        df['requiere_asistencia_tecnica_plan_reasentamiento'] = df['requiere_asistencia_tecnica_plan_reasentamiento'].astype(pd.Int16Dtype())
        df['requiere_asistencia_tecnica_plan_reconstruccion'] = df['requiere_asistencia_tecnica_plan_reconstruccion'].astype(pd.Int16Dtype())
        df['requiere_asistencia_tecnica_planificacion_estrategica'] = df['requiere_asistencia_tecnica_planificacion_estrategica'].astype(pd.Int16Dtype())
        df['requiere_asistencia_tecnica_poblacion_desarrollo'] = df['requiere_asistencia_tecnica_poblacion_desarrollo'].astype(pd.Int16Dtype())
        df['requiere_asistencia_tecnica_politica_desarrollo'] = df['requiere_asistencia_tecnica_politica_desarrollo'].astype(pd.Int16Dtype())
        df['requiere_asistencia_tecnica_presupuesto_resultados'] = df['requiere_asistencia_tecnica_presupuesto_resultados'].astype(pd.Int16Dtype())
        df['requiere_asistencia_tecnica_procedimientos_administrativos'] = df['requiere_asistencia_tecnica_procedimientos_administrativos'].astype(pd.Int16Dtype())
        df['requiere_asistencia_tecnica_programas_incentivos'] = df['requiere_asistencia_tecnica_programas_incentivos'].astype(pd.Int16Dtype())
        df['requiere_asistencia_tecnica_sistema_focalizacion'] = df['requiere_asistencia_tecnica_sistema_focalizacion'].astype(pd.Int16Dtype())
        df['requiere_asistencia_tecnica_otro'] = df['requiere_asistencia_tecnica_otro'].astype(pd.Int16Dtype())
        df['requiere_capacitacion_acondicionamiento_territorial'] = df['requiere_capacitacion_acondicionamiento_territorial'].astype(pd.Int16Dtype())
        df['requiere_capacitacion_administacion_tributaria'] = df['requiere_capacitacion_administacion_tributaria'].astype(pd.Int16Dtype())
        df['requiere_capacitacion_catastro_urbano_rural'] = df['requiere_capacitacion_catastro_urbano_rural'].astype(pd.Int16Dtype())
        df['requiere_capacitacion_control_gubernamental'] = df['requiere_capacitacion_control_gubernamental'].astype(pd.Int16Dtype())
        df['requiere_capacitacion_gestion_financiera'] = df['requiere_capacitacion_gestion_financiera'].astype(pd.Int16Dtype())
        df['requiere_capacitacion_evaluacion_riesgo'] = df['requiere_capacitacion_evaluacion_riesgo'].astype(pd.Int16Dtype())
        df['requiere_capacitacion_formulacion_proyectos'] = df['requiere_capacitacion_formulacion_proyectos'].astype(pd.Int16Dtype())
        df['requiere_capacitacion_gestion_estandares_atencion'] = df['requiere_capacitacion_gestion_estandares_atencion'].astype(pd.Int16Dtype())
        df['requiere_capacitacion_gestion_desarrollo_urbano'] = df['requiere_capacitacion_gestion_desarrollo_urbano'].astype(pd.Int16Dtype())
        df['requiere_capacitacion_gobierno_electronico'] = df['requiere_capacitacion_gobierno_electronico'].astype(pd.Int16Dtype())
        df['requiere_capacitacion_indicadores_gestion'] = df['requiere_capacitacion_indicadores_gestion'].astype(pd.Int16Dtype())
        df['requiere_capacitacion_metodologia_portal_transparencia'] = df['requiere_capacitacion_metodologia_portal_transparencia'].astype(pd.Int16Dtype())
        df['requiere_capacitacion_metodologia_simplificacion'] = df['requiere_capacitacion_metodologia_simplificacion'].astype(pd.Int16Dtype())
        df['requiere_capacitacion_plan_prevencion_riesgo'] = df['requiere_capacitacion_plan_prevencion_riesgo'].astype(pd.Int16Dtype())
        df['requiere_capacitacion_plan_reasentamiento'] = df['requiere_capacitacion_plan_reasentamiento'].astype(pd.Int16Dtype())
        df['requiere_capacitacion_plan_reconstruccion'] = df['requiere_capacitacion_plan_reconstruccion'].astype(pd.Int16Dtype())
        df['requiere_capacitacion_planificacion_estrategica'] = df['requiere_capacitacion_planificacion_estrategica'].astype(pd.Int16Dtype())
        df['requiere_capacitacion_poblacion_desarrollo'] = df['requiere_capacitacion_poblacion_desarrollo'].astype(pd.Int16Dtype())
        df['requiere_capacitacion_politica_desarrollo'] = df['requiere_capacitacion_politica_desarrollo'].astype(pd.Int16Dtype())
        df['requiere_capacitacion_presupuesto_resultados'] = df['requiere_capacitacion_presupuesto_resultados'].astype(pd.Int16Dtype())
        df['requiere_capacitacion_procedimientos_administrativos'] = df['requiere_capacitacion_procedimientos_administrativos'].astype(pd.Int16Dtype())
        df['requiere_capacitacion_programas_incentivos'] = df['requiere_capacitacion_programas_incentivos'].astype(pd.Int16Dtype())
        df['requiere_capacitacion_sistema_focalizacion'] = df['requiere_capacitacion_sistema_focalizacion'].astype(pd.Int16Dtype())
        df['requiere_capacitacion_otro'] = df['requiere_capacitacion_otro'].astype(pd.Int16Dtype())
        # df['instrumentos_gestion_plan_desarrollo_concertado_anio'] = df['instrumentos_gestion_plan_desarrollo_concertado_anio'].astype(pd.Int16Dtype())
        df['instrumentos_gestion_plan_desarrollo_concertado_razon_no_tiene'] = df['instrumentos_gestion_plan_desarrollo_concertado_razon_no_tiene'].astype(pd.Int16Dtype())
        # df['instrumentos_gestion_plan_estrategico_institucional_anio'] = df['instrumentos_gestion_plan_estrategico_institucional_anio'].astype(pd.Int16Dtype())
        df['instrumentos_gestion_plan_estrategico_institucional_razon_no_tiene'] = df['instrumentos_gestion_plan_estrategico_institucional_razon_no_tiene'].astype(pd.Int16Dtype())
        # df['instrumentos_gestion_plan_desarrollo_economico_anio'] = df['instrumentos_gestion_plan_desarrollo_economico_anio'].astype(pd.Int16Dtype())
        df['instrumentos_gestion_plan_desarrollo_economico_razon_no_tiene'] = df['instrumentos_gestion_plan_desarrollo_economico_razon_no_tiene'].astype(pd.Int16Dtype())
        # df['instrumentos_gestion_plan_acondicionamiento_territorial_anio'] = df['instrumentos_gestion_plan_acondicionamiento_territorial_anio'].astype(pd.Int16Dtype())
        df['instrumentos_gestion_plan_acondicionamiento_territorial_razon_no_tiene'] = df['instrumentos_gestion_plan_acondicionamiento_territorial_razon_no_tiene'].astype(pd.Int16Dtype())
        # df['instrumentos_gestion_plan_desarrollo_urbano_anio'] = df['instrumentos_gestion_plan_desarrollo_urbano_anio'].astype(pd.Int16Dtype())
        df['instrumentos_gestion_plan_desarrollo_urbano_razon_no_tiene'] = df['instrumentos_gestion_plan_desarrollo_urbano_razon_no_tiene'].astype(pd.Int16Dtype())
        # df['instrumentos_gestion_esquema_ordenamiento_urbano_anio'] = df['instrumentos_gestion_esquema_ordenamiento_urbano_anio'].astype(pd.Int16Dtype())
        df['instrumentos_gestion_esquema_ordenamiento_urbano_razon_no_tiene'] = df['instrumentos_gestion_esquema_ordenamiento_urbano_razon_no_tiene'].astype(pd.Int16Dtype())
        # df['instrumentos_gestion_plan_desarrollo_rural_anio'] = df['instrumentos_gestion_plan_desarrollo_rural_anio'].astype(pd.Int16Dtype())
        df['instrumentos_gestion_plan_desarrollo_rural_razon_no_tiene'] = df['instrumentos_gestion_plan_desarrollo_rural_razon_no_tiene'].astype(pd.Int16Dtype())
        # df['instrumentos_gestion_plan_desarrollo_capacidades_anio'] = df['instrumentos_gestion_plan_desarrollo_capacidades_anio'].astype(pd.Int16Dtype())
        df['instrumentos_gestion_plan_desarrollo_capacidades_razon_no_tiene'] = df['instrumentos_gestion_plan_desarrollo_capacidades_razon_no_tiene'].astype(pd.Int16Dtype())
        # df['instrumentos_gestion_reglamento_funciones_anio'] = df['instrumentos_gestion_reglamento_funciones_anio'].astype(pd.Int16Dtype())
        df['instrumentos_gestion_reglamento_funciones_razon_no_tiene'] = df['instrumentos_gestion_reglamento_funciones_razon_no_tiene'].astype(pd.Int16Dtype())
        # df['instrumentos_gestion_manual_funciones_anio'] = df['instrumentos_gestion_manual_funciones_anio'].astype(pd.Int16Dtype())
        df['instrumentos_gestion_manual_funciones_razon_no_tiene'] = df['instrumentos_gestion_manual_funciones_razon_no_tiene'].astype(pd.Int16Dtype())
        # df['instrumentos_gestion_cuadro_asignacion_personal_anio'] = df['instrumentos_gestion_cuadro_asignacion_personal_anio'].astype(pd.Int16Dtype())
        df['instrumentos_gestion_cuadro_asignacion_personal_razon_no_tiene'] = df['instrumentos_gestion_cuadro_asignacion_personal_razon_no_tiene'].astype(pd.Int16Dtype())
        # df['instrumentos_gestion_manual_procedimientos_anio'] = df['instrumentos_gestion_manual_procedimientos_anio'].astype(pd.Int16Dtype())
        df['instrumentos_gestion_manual_procedimientos_razon_no_tiene'] = df['instrumentos_gestion_manual_procedimientos_razon_no_tiene'].astype(pd.Int16Dtype())
        # df['instrumentos_gestion_plan_igualdad_genero_anio'] = df['instrumentos_gestion_plan_igualdad_genero_anio'].astype(pd.Int16Dtype())
        df['instrumentos_gestion_plan_igualdad_genero_razon_no_tiene'] = df['instrumentos_gestion_plan_igualdad_genero_razon_no_tiene'].astype(pd.Int16Dtype())
        df['instrumentos_gestion_residuos_plan_integral_gestion_ambiental'] = df['instrumentos_gestion_residuos_plan_integral_gestion_ambiental'].astype(pd.Int16Dtype())
        df['instrumentos_gestion_residuos_plan_manejo_residuos'] = df['instrumentos_gestion_residuos_plan_manejo_residuos'].astype(pd.Int16Dtype())
        df['instrumentos_gestion_residuos_sistema_recojo_residuos'] = df['instrumentos_gestion_residuos_sistema_recojo_residuos'].astype(pd.Int16Dtype())
        df['instrumentos_gestion_residuos_programa_transformacion_residuos'] = df['instrumentos_gestion_residuos_programa_transformacion_residuos'].astype(pd.Int16Dtype())
        df['instrumentos_gestion_residuos_programa_recoleccion_selectiva'] = df['instrumentos_gestion_residuos_programa_recoleccion_selectiva'].astype(pd.Int16Dtype())
        df['instrumentos_gestion_residuos_otro'] = df['instrumentos_gestion_residuos_otro'].astype(pd.Int16Dtype())
        df['instrumentos_gestion_residuos_sin_instrumento'] = df['instrumentos_gestion_residuos_sin_instrumento'].astype(pd.Int16Dtype())
        df['plan_seguridad_ciudadana_incluye_mapa_delito'] = df['plan_seguridad_ciudadana_incluye_mapa_delito'].astype(pd.Int16Dtype())
        df['plan_seguridad_ciudadana_incluye_mapa_riesgo'] = df['plan_seguridad_ciudadana_incluye_mapa_riesgo'].astype(pd.Int16Dtype())
        df['plan_seguridad_ciudadana_incluye_plan_integrado_pnp'] = df['plan_seguridad_ciudadana_incluye_plan_integrado_pnp'].astype(pd.Int16Dtype())
        df['plan_seguridad_ciudadana_incluye_otro'] = df['plan_seguridad_ciudadana_incluye_otro'].astype(pd.Int16Dtype())
        df['gestion_riesgo_desastres_plan_prevencion_riesgo'] = df['gestion_riesgo_desastres_plan_prevencion_riesgo'].astype(pd.Int16Dtype())
        df['gestion_riesgo_desastres_plan_preparacion'] = df['gestion_riesgo_desastres_plan_preparacion'].astype(pd.Int16Dtype())
        df['gestion_riesgo_desastres_plan_operaciones_emergencia'] = df['gestion_riesgo_desastres_plan_operaciones_emergencia'].astype(pd.Int16Dtype())
        df['gestion_riesgo_desastres_plan_educacion_comunitaria'] = df['gestion_riesgo_desastres_plan_educacion_comunitaria'].astype(pd.Int16Dtype())
        df['gestion_riesgo_desastres_plan_rehabilitacion'] = df['gestion_riesgo_desastres_plan_rehabilitacion'].astype(pd.Int16Dtype())
        df['gestion_riesgo_desastres_plan_contingencia'] = df['gestion_riesgo_desastres_plan_contingencia'].astype(pd.Int16Dtype())
        df['gestion_riesgo_desastres_sistema_arleta_temprana_comunitaria'] = df['gestion_riesgo_desastres_sistema_arleta_temprana_comunitaria'].astype(pd.Int16Dtype())
        df['gestion_riesgo_desastres_mapa_comunitario_riesgos'] = df['gestion_riesgo_desastres_mapa_comunitario_riesgos'].astype(pd.Int16Dtype())
        df['gestion_riesgo_desastres_zonificacion_ecologica'] = df['gestion_riesgo_desastres_zonificacion_ecologica'].astype(pd.Int16Dtype())
        df['gestion_riesgo_desastres_informe_evaluacion_riesgo'] = df['gestion_riesgo_desastres_informe_evaluacion_riesgo'].astype(pd.Int16Dtype())
        df['gestion_riesgo_desastres_otro'] = df['gestion_riesgo_desastres_otro'].astype(pd.Int16Dtype())
        df['bienes_ayuda_humanitaria_techo'] = df['bienes_ayuda_humanitaria_techo'].astype(pd.Int16Dtype())
        df['bienes_ayuda_humanitaria_alimentos'] = df['bienes_ayuda_humanitaria_alimentos'].astype(pd.Int16Dtype())
        df['bienes_ayuda_humanitaria_agua'] = df['bienes_ayuda_humanitaria_agua'].astype(pd.Int16Dtype())
        df['bienes_ayuda_humanitaria_botiquines'] = df['bienes_ayuda_humanitaria_botiquines'].astype(pd.Int16Dtype())
        df['bienes_ayuda_humanitaria_abrigo'] = df['bienes_ayuda_humanitaria_abrigo'].astype(pd.Int16Dtype())
        df['bienes_ayuda_humanitaria_enseres'] = df['bienes_ayuda_humanitaria_enseres'].astype(pd.Int16Dtype())
        df['bienes_ayuda_humanitaria_herramientas'] = df['bienes_ayuda_humanitaria_herramientas'].astype(pd.Int16Dtype())
        df['bienes_ayuda_humanitaria_otro'] = df['bienes_ayuda_humanitaria_otro'].astype(pd.Int16Dtype())
        df['incentivo_micro_pequena_empresas_promocion'] = df['incentivo_micro_pequena_empresas_promocion'].astype(pd.Int16Dtype())
        df['incentivo_micro_pequena_empresas_ferias'] = df['incentivo_micro_pequena_empresas_ferias'].astype(pd.Int16Dtype())
        df['incentivo_micro_pequena_empresas_convenios'] = df['incentivo_micro_pequena_empresas_convenios'].astype(pd.Int16Dtype())
        df['incentivo_micro_pequena_empresas_capacitaciones'] = df['incentivo_micro_pequena_empresas_capacitaciones'].astype(pd.Int16Dtype())
        df['incentivo_micro_pequena_empresas_simplificacion_licencia'] = df['incentivo_micro_pequena_empresas_simplificacion_licencia'].astype(pd.Int16Dtype())
        df['incentivo_micro_pequena_empresas_educacion_financiera'] = df['incentivo_micro_pequena_empresas_educacion_financiera'].astype(pd.Int16Dtype())
        df['incentivo_micro_pequena_empresas_otra'] = df['incentivo_micro_pequena_empresas_otra'].astype(pd.Int16Dtype())

        return df

class RenamuPipeline(EasyPipeline):

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        transform_step = TransformStep()
        load_step = LoadStep(
            'inei_renamu_dist', 
            connector = db_connector, 
            if_exists = 'drop', 
            pk = ['ubigeo', 'anio'],
            dtype = DTYPES, 
            nullable_list = NULLABLE_LIST
        )

        return [transform_step, load_step]

if __name__ == '__main__':
   pp = RenamuPipeline()
   pp.run({})
   