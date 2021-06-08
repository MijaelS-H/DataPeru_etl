from numpy.core.records import array
from os import path
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import query_to_df

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        # Lectura de documento en ruta definida
        df = pd.read_excel(path.join(params["datasets"], "01_Informacion_ITP_red_CITE", "02_CLIENTES_ATENDIDOS", "20210605", "TABLA_02_N02.xlsx"))

        # Eliminación de columna fuente
        df = df.drop(columns=['fuente'])

        # Pivoteo de tabla, transformando cada columna en una fila asociada a cada cite, anio, tipo y fecha definida
        df = pd.melt(df, 
            id_vars=[
                'anio','tipo','fecha'
            ], 
            value_vars=[
                'mes_01', 'mes_02', 'mes_03', 'mes_04',
                'mes_05', 'mes_06', 'mes_07', 'mes_08', 
                'mes_09', 'mes_10', 'mes_11','mes_12'])

        # Renombre de columnas
        df = df.rename(columns={'variable':'month_id', 'anio':'year', 'value':'empresas', 'tipo':'tipo_cliente_name'})

        # Transformación de columna month_id (mes_01 --> 01)
        df['month_id'] = df['month_id'].str[-2:]

        # Creación de columna de tiempo
        df['time'] = df['year'].astype(str) + df['month_id']
        df['time'] = df['time'].astype(int)

        # Modificación de formato de fecha de actualización
        df['fecha_actualizacion'] = df['fecha'].astype(str).str[0:4] + df['fecha'].astype(str).str[5:7]
        df['fecha_actualizacion'] = df['fecha_actualizacion'].astype(int)

        # Consulta y agregación de ID según tipo de cliente
        dim_tipo_query = 'SELECT tipo_cliente_name, tipo_cliente_id FROM dim_shared_cite_tipo_cliente'
        dim_tipo = query_to_df(self.connector, raw_query=dim_tipo_query)
        df = df.merge(dim_tipo, on="tipo_cliente_name")

        df = df[['tipo_cliente_id', 'time', 'empresas', 'fecha_actualizacion']]

        return df

class CiteEmpresasPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(name='level', dtype=str),
            Parameter(name='table_name', dtype=str),
            Parameter(name='pk', dtype=array),
            Parameter(name='dtypes', dtype=object),
            Parameter(name='nullable_list', dtype=array)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))

        dtypes = params['dtypes']

        transform_step = TransformStep(connector=db_connector)
        load_step = LoadStep(params['table_name'], connector=db_connector, if_exists='drop', pk=params['pk'], dtype=dtypes, nullable_list=params['nullable_list'])

        return [transform_step, load_step]

def run_pipeline(params: dict):
    PARAMS = {
        'level': 'fact',
        'table_name': 'itp_cite_empresas_tipo_agg',
        'pk': ['tipo_cliente_id', 'time'],
        'dtypes': {
            'tipo_cliente_id':       'UInt8',
            'time':                  'UInt32',
            'empresas':              'Float32',
            'fecha_actualizacion':   'UInt32'
        },
        'nullable_list': []
    }
    
    pp = CiteEmpresasPipeline()

    pp_params = {'level': PARAMS['level'], 'table_name': PARAMS['table_name'], 'pk': PARAMS['pk'], 'dtypes': PARAMS['dtypes'], 'nullable_list': PARAMS['nullable_list']}
    pp_params.update(params)
    pp.run(pp_params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
