# Importa librerías externas a utilizar
from os import path
import pandas as pd

# Importar funcionalidades de Bamboo a utlizar
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep

# Definición de pasos o steps a desarrollar en cada pipeline, su nombre debe hacer referencia al código a ejecutar en cada clase (ejemplos: TransformStep, LoadStep, ReplaceStep, etc.).
# Como buena práctica, se recomienda generar una clase por cada tipo de procedimiento a realizar en el código correspondiente.
# En caso de no ser necesario, es posible realizar todo el ejercicio de transformación en solo un step.

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        # Array de combinaciones de datos existentes en archivo de Variables Framework "A. Economía/A.72.xlsx".
        # Cada elemento corresponde a la combinación entre el tipo de pesca, tipo de consumo y tipo de utilización reportada.
        # Dada la estructura del libro de datos, la forma más factible de construir una tabla de dimensiones es a través de un array de datos inicial.
        # En caso de necesitar agregar más columnas, insertar en array siguiente una fila de datos con la estructura siguiente:
        # ["Jerarquía de datos separada por comas", "ID único"]

        data = [
            ["Pesca Marítima, Consumo Humano Directo, Enlatado", "010101"],
            ["Pesca Marítima, Consumo Humano Directo, Congelado", "010102"],
            ["Pesca Marítima, Consumo Humano Directo, Curado", "010103"],
            ["Pesca Marítima, Consumo Humano Directo, Fresco", "010104"],
            ["Pesca Marítima, Consumo Humano Indirecto, Anchoveta", "010201"],
            ["Pesca Marítima, Consumo Humano Indirecto, Otras Especies", "010202"],
            ["Pesca Continental, Consumo Humano Directo, Curado", "020101"],
            ["Pesca Continental, Consumo Humano Directo, Fresco", "020102"],
            ["Pesca Continental, Consumo Humano Directo, Curado", "020103"]
        ]

        # Generación de dataframe a partir de los datos construídos anteriormente.
        # Su resultado es un Dataframe con dos columnas (utilizacion y utilizacion_id) como se aprecia a continuación:
        # | utilizacion                                       | utilizacion_id |
        # | Pesca Marítima, Consumo Humano Directo, Enlatado  | 010101         |
        # | Pesca Marítima, Consumo Humano Directo, Congelado | 010102         |

        df = pd.DataFrame(data, columns=["utilizacion", "utilizacion_id"])

        # Generación de columnas referentes a tipo de pesca, tipo de consumo y tipo de utilización.
        # La función split permite generar separaciones de string de textos con base en un caracter especial, en este caso una coma ",",
        # el cual combinado con el parámetro expand permite generar una columna dentro del DataFrame correspondiente según cada elemento resultante de dicho split.
        # Su resultado corresponde al DataFrame original con la agregación de las columnas "tipo_pesca", "tipo_consumo", "tipo_utilizacion" como se aprecia a continuación:
        # | utilizacion                                       | utilizacion_id | tipo_pesca     | tipo_consumo           | tipo_utilizacion |
        # | Pesca Marítima, Consumo Humano Directo, Enlatado  | 010101         | Pesca Marítima | Consumo Humano Directo | Enlatado         |       
        # | Pesca Marítima, Consumo Humano Directo, Congelado | 010102         | Pesca Marítima | Consumo Humano Directo | Congelado        |

        df[["tipo_pesca", "tipo_consumo", "tipo_utilizacion"]] = df.utilizacion.str.split(",", expand=True)

        # Definción de largo de identificadores según nivel.
        # Como se aprecia en las secciones anteriores, se ha definido un id numérico de 6 dígitos que responde al tipo de pesca, tipo de consumo y tipo de utilización.
        # Cada par de dígitos hace referencia a un nivel de los tres anteriormente mencionados, por lo cual el código 020103, es expresado de la siguiente forma:
        # 02: Pesca Continental
        # 01: Consumo Humano Directo
        # 03: Curado
        # Dado que Tesseract necesita de identificadores únicos para cada jerarquía, se define como identificador para cada elemento la concatenación de los identificadores propios
        # con los identificadores superiores en jerarquía, por lo que el identificador de Pesca Continental de Consumo Humano Directo Curado corresponde al identificador 020103.
        # El siguiente arreglo define el largo del identificador de cada jerarquía, desde el nivel jerarquico mayor al nivel inferior

        ids_len = [2, 4, 6]

        # Construcción de identificadores para cada nivel.
        # Como se menciona anteriormente, cada nivel posee un identificador que responde a su nivel jerárquico.
        # El siguiente código itera para cada uno de dichos largos y genera una columna adicional en el DataFrame con el identificador correspondiente.
        # El resultado corresponde a la siguiente estructura:
        # | utilizacion                                       | utilizacion_id | tipo_pesca     | tipo_consumo           | tipo_utilizacion | code_0 | code_1 | code_2 |
        # | Pesca Marítima, Consumo Humano Directo, Enlatado  | 010101         | Pesca Marítima | Consumo Humano Directo | Enlatado         | 01     | 0101   | 010101 |
        # | Pesca Marítima, Consumo Humano Directo, Congelado | 010102         | Pesca Marítima | Consumo Humano Directo | Congelado        | 01     | 0101   | 010102 |

        for level in range(len(ids_len)):
            df['code_{}'.format(level)] = df['utilizacion_id'].astype(str).str[:ids_len[level]]

        # Renombre de columnas.
        # Las columnas anteriormente generadas son renombradas para permitir una identificación más rápida.
        # El resultado responde a la siguiente estructura:
        # | utilizacion                                       | utilizacion_id | tipo_pesca     | tipo_consumo           | tipo_utilizacion | tipo_pesca_id | tipo_consumo_id | tipo_utilizacion_id |
        # | Pesca Marítima, Consumo Humano Directo, Enlatado  | 010101         | Pesca Marítima | Consumo Humano Directo | Enlatado         | 01            | 0101            | 010101              |
        # | Pesca Marítima, Consumo Humano Directo, Congelado | 010102         | Pesca Marítima | Consumo Humano Directo | Congelado        | 01            | 0101            | 010102              |

        df.rename(columns={
            "code_0": "tipo_pesca_id",
            "code_1": "tipo_consumo_id",
            "code_2": "tipo_utilizacion_id"
        }, inplace=True)

        # Selección de columnas.
        # Finalmente se selecccionan las columnas a ingestar y el orden en el que ellas son presentadas finalmente.
        # El resutlado responde a la siguiente estructura:
        # | tipo_pesca_id | tipo_pesca     | tipo_consumo_id | tipo_consumo           | tipo_utilizacion_id | tipo_utilizacion | utilizacion                                       |
        # | 01            | Pesca Marítima | 0101            | Consumo Humano Directo | 010101              | Enlatado         | Pesca Marítima, Consumo Humano Directo, Enlatado  |
        # | 01            | Pesca Marítima | 0101            | Consumo Humano Directo | 010102              | Congelado        | Pesca Marítima, Consumo Humano Directo, Congelado |

        df = df[["tipo_pesca_id", "tipo_pesca", "tipo_consumo_id", "tipo_consumo", "tipo_utilizacion_id", "tipo_utilizacion", "utilizacion"]].copy()

        # Retorna el DataFrame construído para los siguientes pasos a ejecutar.
        return df

# En caso de no existir más pasos o steps a definir es necesario establecer la clase de ejecución, la cual recopila el trabajo anteriormente desarrollado y lo ejecuta según lo indicado.
# Su nombre debe ser ÚNICO y se recomienda que responda al contenido a ejecutar para asegurar su funcionalidad.

class DimensionTrainingPipeline(EasyPipeline):
    @staticmethod
    def steps(params):

        # Definición de conector a base de datos.
        # En caso de que el repositorio no haya presentado cambios en su estructura, no modificar.

        db_connector = Connector.fetch("clickhouse-database", open(params["connector"]))

        # Definición de tipos de datos.
        # Es necesario definir para cada columna a ingestar su tipo de dato correspondiente.
        # Es importante que este tipo de dato coincida con el código a utilizar a futuro, dado que un String "101" no es igual a un entero 101.

        dtype = {
            "tipo_pesca_id":            "String",
            "tipo_pesca":               "String",
            "tipo_consumo_id":          "String",
            "tipo_consumo":             "String",
            "tipo_utilizacion_id":      "String",
            "tipo_utilizacion":         "String",
            "utilizacion":              "String"
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

        load_step = LoadStep("dim_training", db_connector, if_exists="drop", pk=["tipo_utilizacion_id"], dtype=dtype, nullable_list=[])

        # Return de los pasos o steps previamente importados y definidos.
        # IMPORTANTE: Los pasos se ejecutarán según su posición en el array definido.

        return [transform_step, load_step]

# Ejecución de pipeline.
# Las siguientes líneas ejecutan el pipeline previamente generado.
# Se recomienda no modificar estas líneas excepto por lo señalado.

def run_pipeline(params: dict):

    # Asignación de pipeline.
    # Notar que el pipeline asignado al parámetro pp corresponde al nombre de pipeline definido en la línea 104.

    pp = DimensionTrainingPipeline()
    pp.run(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    # Ejecución de pipeline
    # Notar que el conector corresponde a la ruta relativa hacia el archivo conns.yaml, por lo que es necesario definir correctamente dicha ruta al momento de ejecutar un pipeline.

    run_pipeline({
        "connector": path.join(__dirname, "..", "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
