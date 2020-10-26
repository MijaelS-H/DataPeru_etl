# Plataforma de datos ITP: ETL

## Descripción

El presente repositorio tiene como objetivo recopilar el proceso de extracción, transformación y carga de datos (ETL) desarrollado durante las etapas de construcción de API.
Los componentes de el presente repositorio corresponden, en su mayoría, a pipelines de ingestión, los cuales corresponden a estructuras de código que permiten la replicación de la ingesta de datos en múltiples entornos de desarrollo y producción.

## Estructura del repositorio

El repositorio actual se encuentra estructurado de la siguiente forma:

### etl/

Recopilación de pipelines de ingestión de múltiples bases de datos (ej: MINAGRI, MEF, ITP, entre otros). Su estructura responde a carpetas de archivos organizados según fuentes de datos o temáticas similares (ej: Indicadores de Censos o Encuestas). Cada archivo `.py` existente en el directorio `etl/` corresponde a un pipeline de ingestión, un set de variables necesarias para la ejecución de un pipeline de ingestión, o bien, un paso necesario para la ingestión de datos. Su estructura es variable y responde a la estructura de cada set de datos procesado por ellos.
De forma adicional, es posible encontrar el archivo `conns.yaml`, el cual determina la dirección de la base de datos asociada hacia la cual se realizará el proceso de ingestión de datos.

### init.sh

Archivo de iniciación de repositorio. Su objetivo es facilitar la creación de un ambiente de trabajo con todas las dependencias necesarias para la correcta ejecución de cada pipeline generado.

### requirements.txt

Archivo de requisitos necesarios para la ejecución de múltiples comandos intermedios. Se recomienda, para su utilización, la utilización del archivo `init.sh`, o bien, la creación de un entorno virtual para evitar el conflicto de versiones preexistentes de los requisitos especificados. Un ejemplo de creación de entorno vitual es realizado mediante los siguientes comandos:

```
sudo apt-get install python3-pip
sudo pip3 install virtualenv

virtualenv -p python3.7 venv
source venv/bin/activate
```

Una vez generado el entorno virtual, es necesaria la instalacción de las dependencias requeridas:

```
pip install -r requirements.txt
```

## Configuración

Para realizar el proceso de ingestión de datos es necesario seguir los siguientes pasos:

### 1. Clonar el repositorio

Realizar un clon del repositorio en un directorio local que permita el desarrollo, pruebas e ingestión de datos en un servidor de desarrollo o producción. Lo anterior es posible mediante la ejecución de:

```
bash
git clone https://github.com/dataperu/dataperu-etl.git
cd dataperu-etl
```

En caso de ser un setup local, es necesaria la instalación de [git](https://git-scm.com/).

### 2. Configurar un entorno virtual

Para evitar conflictos entre versiones de dependencias instaladas en la máquina o entorno en el cual se llevará a cabo el proceso de ingestión, se recomienda la creación de un entorno virtual que permita una instalación de dependencias requeridas sin afectar las ya existentes en el entorno global. Lo anterior es posible de realizar mediante la ejecución del archivo `init.sh` detallado en la directorio raíz del presente repositorio, o bien mediante la ejecución de:

```bash
sudo apt-get install python3-pip
sudo pip3 install virtualenv

virtualenv -p python3.7 venv
source venv/bin/activate
```

Una vez generado el entorno virtual, es necesaria la instalacción de las dependencias requeridas:

```
pip install -r requirements.txt
```

### 3. Agregar las variables de entorno

Un set de variables de entorno son requeridas para el desarrollo del proyecto. Por la seguridad de los datos, estas variables no se encuentran disponibles en el presente repositorio.
Favor contactar al administrador del sitio para obtener acceso a las variables requeridas.

### 4. Descargar dump de datos

Un set de datos son requeridos para el desarrollo del proyecto. Por su seguridad, estos no se encuentran disponibles en el presente repositorio.
Favor contactar al administrador del sitio para obtener acceso a los datos requeridos.

### 5. Ejecutar un pipeline de ingestión de prueba para probar la configuración

Con el objetivo de probar el setup del entorno virtual necesario, se recomienda ejecutar el comando `python etl/shared/actividad_economica_54.py` en el directorio raíz del repositorio clonado, el cual realizará la ingestión de una tabla de dimensiones liviana, el cual permitirá reconocer errores en la configuración del repositorio, o bien, asegurar que la configuración fue un éxito.
