import pandas as pd
from simpledbf import Dbf5
from bamboo_lib.models import PipelineStep


class Module31Step(PipelineStep):
    def run_step(self, prev, params):

      df, mod2_1, mod2_2 = prev

      file_data = '../../data/ene/03_MÓDULO_IV_1.dbf'

      dbf = Dbf5(file_data, codec='latin-1')
      mod4_1 = dbf.to_dataframe()
      mod4_1.columns = [x.lower() for x in mod4_1.columns]

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
      mod4_1['modalidad_realizar_compras_2014'] = mod4_1['modalidad_realizar_compras_2014'].astype(pd.Int16Dtype())
      mod4_1['publicidad_conoce_proveedores'] = mod4_1['publicidad_conoce_proveedores'].astype(pd.Int16Dtype()) 
      mod4_1['pedido_proveedor_internacional_2014'] = mod4_1['pedido_proveedor_internacional_2014'].astype(pd.Int16Dtype())
      mod4_1['razon_pedido_insumos_proveedor_internacional_2014'] = mod4_1['razon_pedido_insumos_proveedor_internacional_2014'].astype(pd.Int16Dtype())
      mod4_1['razon_no_realiza_compras_proveedor_internacional'] = mod4_1['razon_no_realiza_compras_proveedor_internacional'].astype(pd.Int16Dtype())
      mod4_1['realiza_compras_internet_empresa_2014'] = mod4_1['realiza_compras_internet_empresa_2014'].astype(pd.Int16Dtype())
      mod4_1['frecuencia_compras_internet_2014'] = mod4_1['frecuencia_compras_internet_2014'].astype(pd.Int16Dtype())
      mod4_1['medio_pago_compras_internet_tarjeta_credito'] = mod4_1['medio_pago_compras_internet_tarjeta_credito'].astype(pd.Int16Dtype())
      mod4_1['medio_pago_compras_internet_transferencia'] = mod4_1['medio_pago_compras_internet_transferencia'].astype(pd.Int16Dtype())
      mod4_1['medio_pago_compras_internet_billetera_electronica'] = mod4_1['medio_pago_compras_internet_billetera_electronica'].astype(pd.Int16Dtype())
      mod4_1['medio_pago_compras_internet_contra_entrega_terminal_de_pago'] = mod4_1['medio_pago_compras_internet_contra_entrega_terminal_de_pago'].astype(pd.Int16Dtype())
      mod4_1['medio_pago_compras_internet_otro'] = mod4_1['medio_pago_compras_internet_otro'].astype(pd.Int16Dtype())
      mod4_1['tecnologia_predominante_proceso_produccion'] = mod4_1['tecnologia_predominante_proceso_produccion'].astype(pd.Int16Dtype())
      mod4_1['fuente_energia_utiliza_electricidad'] = mod4_1['fuente_energia_utiliza_electricidad'].astype(pd.Int16Dtype()) 
      mod4_1['fuente_energia_utiliza_diesel'] = mod4_1['fuente_energia_utiliza_diesel'].astype(pd.Int16Dtype())
      mod4_1['fuente_energia_utiliza_carbon'] = mod4_1['fuente_energia_utiliza_carbon'].astype(pd.Int16Dtype()) 
      mod4_1['fuente_energia_utiliza_gas_natural'] = mod4_1['fuente_energia_utiliza_gas_natural'].astype(pd.Int16Dtype())
      mod4_1['fuente_energia_utiliza_gas_licuado'] = mod4_1['fuente_energia_utiliza_gas_licuado'].astype(pd.Int16Dtype()) 
      mod4_1['fuente_energia_utiliza_otro'] = mod4_1['fuente_energia_utiliza_otro'].astype(pd.Int16Dtype())
      mod4_1['modalidad_venta_2014'] = mod4_1['modalidad_venta_2014'].astype(pd.Int16Dtype())
      mod4_1['realiza_estudios_mercado_para_promocion_producto_servicio_2014'] = mod4_1['realiza_estudios_mercado_para_promocion_producto_servicio_2014'].astype(pd.Int16Dtype())
      mod4_1['realiza_ventas_internet_2014'] = mod4_1['realiza_ventas_internet_2014'].astype(pd.Int16Dtype())

      return df, mod2_1, mod2_2, mod4_1
