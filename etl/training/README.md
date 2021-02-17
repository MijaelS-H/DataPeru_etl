# Entrenamiento ETL

## Descripción

El presente directorio tiene como objetivo desarrollar un ejemplo de ingestión de datos que permita establecer una conexión con un esquema básico desarrollado para el presente proyecto a modo de apoyo al desarrollo futuro.

Su objetivo es establecer los pasos clave a desarrollar en todo pipeline de ingestión, sin profundizar extremadamente en el procesamiento o transformación de los datos correspondientes.
Cada pipeline desarrollado posee comentarios internos que permitirán comprender de mejor forma el contenido relacionado.

## Comandos a ejecutar

### Ingestión de tabla de dimensiones
python -m etl.training.dimensions.training_dimensions_pipeline ../../datasets

### Ingestión de tabla de hechos
python -m etl.training.training_pipeline ../datasets

## Revisión de tablas en Clickhouse
clickhouse-client -d dataperuv2
select * from dim_training
select * from fact_training