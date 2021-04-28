# Importa librerías externas a utilizar
from os import path
import pandas as pd

# Importar funcionalidades de Bamboo a utlizar
from bamboo_lib.helpers import query_to_df
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep

# Definición de pasos o steps a desarrollar en cada pipeline, su nombre debe hacer referencia al código a ejecutar en cada clase (ejemplos: TransformStep, LoadStep, ReplaceStep, etc.).
# Como buena práctica, se recomienda generar una clase por cada tipo de procedimiento a realizar en el código correspondiente.
# En caso de no ser necesario, es posible realizar todo el ejercicio de transformación en solo un step.

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        
        # Lectura de archivo.
        # A diferencia del ejercicio anterior, el presente pipeline ingesta una fact table o tabla de hecho la cual responde a un archivo específico.
        # A través del parámetro datasets, es necesario construir la ruta exacta al archivo a ingestar a través del uso de funcionalidades de concatenación de direcciones.
        # Su resultado corresponde al archivo excel original como se aprecia a continuación:
        # | Unnamed: 0 | Unnamed: 1 | Unnamed: 2 | Unnamed: 3 | Unnamed: 4 | Unnamed: 5 | Unnamed: 6 | Unnamed: 7 | Unnamed: 8 | Unnamed: 9 | Unnamed: 10 | Unnamed: 11 | Unnamed: 12 |	Unnamed: 13 | Unnamed: 14 | Unnamed: 15 | Unnamed: 16 | Unnamed: 17 | Unnamed: 18 | Unnamed: 19 |
        # Dada la estructura del archivo, no existe una columna única que permita establecer un header válido para el presente archivo, por lo que todas las columnas poseen un nombre undefinido.
            
        df = pd.read_excel(path.join(params["datasets"], "A_Economia", "A.72.xlsx"), header=8)

        # Definición de diccionario.
        # Dada la estructura del archivo base presentado, se establece un diccionario que permitirá renombrar las columnas presentes en el DataFrame con el objetivo de facilitar su comprensión.

        column_dict = {
            "Unnamed: 0": "year",
            "Unnamed: 1": "drop_1",
            "Unnamed: 2": "drop_2",
            "Unnamed: 3": "drop_3",
            "Unnamed: 4": "drop_4",
            "Unnamed: 5": "drop_5",
            "Unnamed: 6": "Pesca Marítima, Consumo Humano Directo, Enlatado",
            "Unnamed: 7": "Pesca Marítima, Consumo Humano Directo, Congelado",
            "Unnamed: 8": "Pesca Marítima, Consumo Humano Directo, Curado",
            "Unnamed: 9": "Pesca Marítima, Consumo Humano Directo, Fresco",
            "Unnamed: 10": "drop_6",
            "Unnamed: 11": "drop_7",
            "Unnamed: 12": "Pesca Marítima, Consumo Humano Indirecto, Anchoveta",
            "Unnamed: 13": "Pesca Marítima, Consumo Humano Indirecto, Otras Especies",
            "Unnamed: 14": "drop_8",
            "Unnamed: 15": "drop_9",
            "Unnamed: 16": "Pesca Continental, Consumo Humano Directo, Curado",
            "Unnamed: 17": "Pesca Continental, Consumo Humano Directo, Fresco",
            "Unnamed: 18": "Pesca Continental, Consumo Humano Directo, Congelado",
            "Unnamed: 19": "drop_10"
        }

        # Renombre de columnas.
        # A través del diccionario anteriormente definido, se realiza un reemplazo de las columnas pos su nombre a utilizar.
        # De esta forma el DataFrame toma la siguiente estructura:
        # | year | drop_1 |	drop_2 | drop_3 | drop_4 | drop_5 | Pesca Marítima, Consumo Humano Directo, Enlatado | Pesca Marítima, Consumo Humano Directo, Congelado | Pesca Marítima, Consumo Humano Directo, Curado | Pesca Marítima, Consumo Humano Directo, Fresco | drop_6 | drop_7 | Pesca Marítima, Consumo Humano Indirecto, Anchoveta | Pesca Marítima, Consumo Humano Indirecto, Otras Especies | drop_8 | drop_9 | Pesca Continental, Consumo Humano Directo, Curado | Pesca Continental, Consumo Humano Directo, Fresco	Pesca Continental, Consumo Humano Directo, Congelado | drop_10 |

        df.rename(columns=column_dict, inplace=True)

        # Dropeo de columnas no utilizadas.
        # Como se aprecia en el paso anterior, se establecen un set de columnas con un nombre "drop_" las cuales no serán utilizadas en el resto del pipeline, por lo cual son dropeadas del DataFrame original.

        df.drop(list(df.filter(regex="drop_")), axis=1, inplace=True)

        # Reemplazo de caracteres especiales.
        # Con el objetivo de evitar conflictos según el tipo de datos, son eliminados ciertos caracteres existentes en los datos para transformar las columnas a su tipo de dato correspondiente.

        df.replace("-", 0, inplace=True)
        df.replace("2018 P/", 2018, inplace=True)

        # Dropeo de filas nulas.
        # Dada la existencia de nulos en las filas correspondientes, se procede a eliminar dichas filas que no poseen información relevante
        
        df.dropna(inplace=True)

        # Pivoteo de tabla
        # Actualmente la tabla posee una columna para cada tipo de producto relacionado como se presenta en la línea 56 del presente código.
        # El objetivo de pivotear una tabla es transformar las columnas en filas, permitiendo llevar los datos existentes a un formato de datos ordenados o tidy data.
        # El resultado del pivoteo de tabla corresponde a, donde desembarque corresponde a la medida representada en la tabla original:
        # | year | utilizacion                                      | desembarque |
        # | 1995 | Pesca Marítima, Consumo Humano Directo, Enlatado | 196.788     |
        # | 1996 | Pesca Marítima, Consumo Humano Directo, Enlatado | 213.905     |

        df = pd.melt(df, id_vars=["year"], var_name="utilizacion", value_name="desembarque")

        # Transformación de columna año.
        # Dado que originalmente el año 2018 estaba definido como 2018 P/ haciendo referencia a datos preeliminares, la columna year asumió un formato de datos de texto, por lo cual es necesario tranformarlo a un tipo de dato entero.

        df["year"] = df.year.astype("int64")

        # Consulta de datos de tabla de dimensión.
        # Dado que actualmente la columna utilizacion posee el nombre completo de la categoría relacionada, pero no el código establecido en la tabla de dimensiones, es necesario realizar una comparación con dicha tabla.
        # El código definido a continuación permite realizar una consulta SQL a la base de datos correspondiente, retornando un DataFrame utilizable en el presente pipeline.
        # Su resultado es:
        # | utilizacion                                       | tipo_utilizacion_id |
        # | Pesca Marítima, Consumo Humano Directo, Enlatado  | 010101              |
        # | Pesca Marítima, Consumo Humano Directo, Congelado │ 010102              |

        dim_training_query = 'SELECT utilizacion, tipo_utilizacion_id FROM dim_training'
        db_connector = Connector.fetch('clickhouse-database', open(params["connector"]))
        dim_training = query_to_df(db_connector, raw_query=dim_training_query)

        # Unión de tablas.
        # Con el objetivo de establecer el código para cada utilizacion definida, se concatenan ambos DataFrame generados mediante el siguiente merge de datos.
        # Su resultado es:
        # | year | utilizacion                                      | desembarque | tipo_utilizacion_id |
        # | 1995 | Pesca Marítima, Consumo Humano Directo, Enlatado | 196.788     | 010101              |
        # | 1996 | Pesca Marítima, Consumo Humano Directo, Enlatado | 213.905     | 010101              |

        df = pd.merge(df, dim_training, on='utilizacion')

        # Selección de columnas.
        # Finalmente se selecccionan las columnas a ingestar y el orden en el que ellas son presentadas finalmente.
        # El resutlado responde a la siguiente estructura:
        # | tipo_utilizacion_id | year | desembarque |
        # | 010101              | 1995 | 196.788     |
        # | 010101              | 1996 | 213.905     |

        df = df[["tipo_utilizacion_id", "year", "desembarque"]].copy()

        # Retorna el DataFrame construído para los siguientes pasos a ejecutar.

        return df

# En caso de no existir más pasos o steps a definir es necesario establecer la clase de ejecución, la cual recopila el trabajo anteriormente desarrollado y lo ejecuta según lo indicado.
# Su nombre debe ser ÚNICO y se recomienda que responda al contenido a ejecutar para asegurar su funcionalidad.

class TrainingPipeline(EasyPipeline):
    @staticmethod
    def steps(params):

        # Definición de conector a base de datos.
        # En caso de que el repositorio no haya presentado cambios en su estructura, no modificar.

        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))

        # Definición de tipos de datos.
        # Es necesario definir para cada columna a ingestar su tipo de dato correspondiente.
        # Es importante que este tipo de dato coincida con el código a utilizar a futuro, dado que un String "101" no es igual a un entero 101.

        dtype = {
            "tipo_utilizacion_id":  "String",
            "year":                 "UInt16",
            "desembarque":          "Float32"
        }

        # Importación de steps.
        # Cada step antes definido debe ser importado en las siguientes líneas, siguiendo la estructura presentada.

        transform_step = TransformStep()

        # Definición de LoadStep
        # Adicionalmente, se define el step de carga de datos, el cual permitira ingestar el dataset retornado en la base de datos correspondiente.
        # Su estructura es constante y responde a los siguientes parámetros:
        # LoadStep(table_name, connector, if_exist, pk, dtype, nullable_list)
        # table_name: Corresponde al nombre de la tabla sobre el cual la data será ingestada en la base de datos
        # connector: Corresponde a la conexión establecida con la base de datos, definida anteriormente
        # if_exist: Definición de tratamiento con la tabla si esta ya se encuentra disponibe en la base de datos. Puede tomar los valores "drop" o "append"
        # pk: Definición de la clave primaria de la tabla a ingestar
        # dtype: Listado de tipos de datos previamente definido
        # nullable_list: Listado de columnas que pueden contener valores nulos

        load_step = LoadStep("fact_training", db_connector, if_exists="drop", pk=["tipo_utilizacion_id"], dtype=dtype, nullable_list=[])

        # Return de los pasos o steps previamente importados y definidos.
        # IMPORTANTE: Los pasos se ejecutarán según su posición en el array definido.

        return [transform_step, load_step]

# Ejecución de pipeline.
# Las siguientes líneas ejecutan el pipeline previamente generado.
# Se recomienda no modificar estas líneas excepto por lo señalado.

def run_pipeline(params: dict):

    # Asignación de pipeline.
    # Notar que el pipeline asignado al parámetro pp corresponde al nombre de pipeline definido en la línea 104.

    pp = TrainingPipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    # Ejecución de pipeline
    # Notar que el conector corresponde a la ruta relativa hacia el archivo conns.yaml, por lo que es necesario definir correctamente dicha ruta al momento de ejecutar un pipeline.

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
