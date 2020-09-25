import pandas as pd
from simpledbf import Dbf5
from bamboo_lib.models import PipelineStep


class Module1Step(PipelineStep):
    def run_step(self, prev, params):

      file_data = '../../data/ene/01_EMPRESA_IDENTIFICA.dbf'

      dbf = Dbf5(file_data, codec='latin-1')
      df = dbf.to_dataframe()
      df.columns = [x.lower() for x in df.columns]

      # 01_EMPRESA_IDENTIFICA
      df['district_id'] = df['ccdd'].astype(
        str) + df['ccpp'].astype(str) + df['ccdi'].astype(str)

      df = df[[
          'iruc', 
          'district_id', 
          # 'c1', 
          # 'c2', 
           #'c3', 
          'c4', 
          'c14_cod', 
          'c15a_cod',
          'c15b_cod', 
          # 'c15c_cod', 
          # 'c15d_cod', 
          'c16', 
          # 'c16_o',
          'c20', 
          'factor_exp'
      ]].copy()

      columns = {
          'iruc': 'company_id',
          'district_id': 'district_id',
          # 'c1': 'ruc',
          # 'c2': 'razon_social',
          # 'c3': 'nombre_comercial',
          'c4': 'anio_inicio_actividades',
          'c14_cod': 'ciiu_rev4',
          'c15a_cod': 'ciiu_rev4_a',
          'c15b_cod': 'ciiu_rev4_b',
          # 'c15c_cod': 'ciiu_rev4_c',
          # 'c15d_cod': 'ciiu_rev4_d',
          'c16': 'tipo_sociedad',
          # 'c16_o': 'tipo_sociedad_text',
          'c20': 'rango_ventas_2014',
          'factor_exp': 'factor_expansion'
      }

      df.rename(columns=columns, inplace=True)
      df = df[list(columns.values())].copy()
      df['company_id'] = df['company_id'].astype(int)
      df['anio_inicio_actividades'] = df['anio_inicio_actividades'].astype(pd.Int32Dtype())
      df['tipo_sociedad'] = df['tipo_sociedad'].astype(pd.Int16Dtype())
      df['rango_ventas_2014'] = df['rango_ventas_2014'].astype(pd.Int16Dtype())

      df['ciiu_rev4'].fillna('0000', inplace=True)
      df['ciiu_rev4_a'].fillna('0000', inplace=True)
      df['ciiu_rev4_b'].fillna('0000', inplace=True)
      # df['ciiu_rev4_c'].fillna('0000', inplace=True)
      # df['ciiu_rev4_d'].fillna('0000', inplace=True)

      return df
