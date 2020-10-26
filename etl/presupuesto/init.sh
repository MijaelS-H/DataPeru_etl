#!bin/bash

python download_pipeline.py
python temp_dims.py
python dim_gastos_pipeline.py
python dim_gastos_aggregate_pipeline.py
python dim_ingresos_pipeline.py
python presupuesto_gastos_pipeline.py
python presupuesto_ingresos_pipeline.py