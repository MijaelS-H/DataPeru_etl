# Plataforma de datos ITP: ETL

## Descripción

El presente repositorio tiene como objetivo recopilar la estructura del proceso de ETL realizado para el proyecto desarrollado en conjunto entre ITP y Datawheel, con el objetivo de establecer una base común de desarrollo que permita mantener un estándar actualizado del proceso de ingestión de datos durante el completo proyecto.

## Estructura del Repositorio

El repositorio actual se encuentra estructurado de la siguiente forma:

### etl/

Recopilación de pipelines de ingestión, funciones y archivos estáticos que permiten el desarrollo del completo proceso de desarrollo. Su estructura responde a carpetas de archivos organizados según tópicos o fuentes de datos (ej: Indicadores de Censos o Encuestas), en los cuales es posible encontrar archivos `.py` los cuales corresponden a pipelines de ingestión de archivos específicos.
Adicionalmente, cada carpeta cuenta con un archivo `__init__.py` el cual automatiza la ingestión de los pipelines incluídos en dicho directorio.

### .gitignore

Archivo git que permite establecer rutas de archivos o extensiones de archivos no deseadas, las cuales son excluídas del proceso de gestión de cambios dentro del repositorio.

### .pylintrc

Archivo de configuración de Python. Es utilizado para establecer estándares de programación comunes dentro de un repositorio.

### requirements.txt

Archivo de requisitos necesarios para la ejecución de múltiples comandos intermedios. Se recomienda, para su utilización, la creación de un entorno virtual para evitar el conflicto de versiones preexistentes de los requisitos especificados. Un ejemplo de creación de entorno vitual es realizado mediante los siguientes comandos:

```
sudo apt-get install python3-pip
sudo pip3 install virtualenv

virtualenv -p python3.7 venv
source venv/bin/activate
```

Una vez generado el entorno virtual, es necesaria la instalacción de las dependencias requeridas:

`pip install -r requirements.txt`

### run.py

Archivo de ejecución de la totalidad de pipelines de ingestión disponibles en el existente repositorio.

## Setup

Para la configuración y funcionamiento de los scripts de ETL existentes para el presente proyecto, se deben seguir las siguientes instrucciones con el objetivo de completar una correcta configuración del entorno local y asegurar la ejecución exitosa de cada script desarrollado por la entidad consultora.

### Clonar el repositorio

El primer paso de clonación de repositorio consiste en la obtención del código fuente del proceso de ingestión de datos desarrollado por la entidad consultora, actualmente almacenado en la plataforma de control de versiones de archivos Github, proceso que permite obtener una versión en el servidor local o de desarrollo del set de códigos generado por la entidad consultora. Este paso es fundamental para obtener la versión más reciente de los códigos desarrollados, lo cual permite replicar el proceso de ingestión considerando las últimas modificaciones al código fuente. Lo anterior es posible mediante la ejecución del siguiente código en una terminal o consola de comandos, previamente configurada con las herramientas de git, el cual será almacenado en una carpeta relativa al directorio de ejecución del código anterior con el nombre actual del repositorio.

```
git clone https://github.com/dataperu/dataperu-etl.git
```

El comando previamente mencionado, realizará una descarga del código actualmente disponible en el repositorio desarrollado para el proceso de ETL del presente proyecto, el cual permitirá su replicación en múltiples instancias

Para acceder al contenido mediante la terminal previamente utilizada durante el proceso de clonación del repositorio es necesario ejecutar el siguiente comando, el cual modificará la ruta hacia la cual está apuntando actualmente la terminal en proceso.

`cd dataperu-etl/`

### Creación de entorno virtual de Python

Comúnmente durante el proceso de desarrollo de aplicaciones o exploración de datos con base en lenguajes de programación como Python, se tienden a utilizar una serie de librerías en diferentes versiones según los requerimientos de cada proyecto, lo cual puede provocar problemas de compatibilidad con códigos desarrollados por terceros. Lo anterior, hace necesaria la generación de entornos virtuales de Python que permitan establecer una configuración para cada proyecto, evitando así, problemas de compatibilidad futuros.

Para la generación del entorno virtual se recomienda la ejecución de los siguientes comandos, posterior a la instalación de Python 3.7.9 en la terminal correspondiente, preferentemente de Ubuntu 18.04 LTS.

```bash
sudo apt-get install python3-pip
sudo pip3 install virtualenv

virtualenv -p python3.7 venv
source venv/bin/activate
```

La primera línea de comandos asegurará la actualización de repositorios disponibles en las fuentes de descargas de Ubuntu, lo cual asegurará que las versiones a instalar de cada repositorio sean las más recientes y actualizadas. La segunda línea de comandos realizará la instalación de python3-venv, herramienta que permitirá la generación de entornos virtuales con base en Python3 de forma dinámica y directa, tal como se aprecia en la tercera línea de comandos, la cual generará un entorno virtual en el directorio actual, dentro de la carpeta venv, la cual contendrá los requisitos funcionales mínimos para ejecutar scripts de python. Finalmente la cuarta línea de comandos activará el entorno virtual anteriormente generado permitiendo continuar con la configuración necesaria para la ejecución de los pipelines de ingestión.

Cabe señalar que la activación de entornos virtuales es necesaria cada vez que la terminal de trabajo es cerrada. Para lo anterior, es necesaria la ejecución de la última línea de comandos anteriormente mencionada en el directorio en donde fue creado el entorno virtual.

### Instalar las dependencias del proyecto

Para la instalación de las dependencias del proyecto, es posible ejecutar la siguiente línea de comando, la cual ejecutará la instalación de todas las dependencias requeridas y definidas anteriormente.

```
pip install -r requirements.txt
```

Este proceso garantizará la disponibilidad de las dependencias requeridas para la ejecución de los pipelines de ingestión desarrollados.

### Agregar variables de entorno

Con el objetivo de estandarizar el trabajo de ingestión de datos y permitir la conexión de los datos ingestados con Tesseract, una serie de variables de entorno son requeridas para el proceso de desarrollo y producción del presente proyecto, tales como, la ubicación del esquema general de Tesseract, las credenciales de acceso a Clickhouse, entre otros. Dada la sensibilidad de dichos valores, a continuación se definirán las variables de entorno necesarias para el correcto funcionamiento durante el completo proceso de desarrollo, cuyo contenido será informado oportunamente al equipo técnico de ITP.

- `CLICKHOUSE_URL`: corresponde al URL o ruta de Clickhouse en el servidor de desarrollo. Ejemplo: `127.0.0.1`.
- `CLICKHOUSE_USERNAME`: corresponde al usuario de Clickhouse definido para acceder a la base de datos correspondiente. Ejemplo: `dataperu`.
- `CLICKHOUSE_DATABASE`: corresponde a la base de datos de Clickhouse definida para el almacenamiento de los datos ingestados. Ejemplo: `dataperu`.
- `CLICKHOUSE_PASSWORD`: corresponde a la contraseña del usuario de Clickhouse definida para acceder a la base de datos correspondiente. Ejemplo: `dat4p3ru`.
