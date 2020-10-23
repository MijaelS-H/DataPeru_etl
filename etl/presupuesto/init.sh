#!bin/bash

python download_pipeline.py
python create_dims.py
python dims_pipeline.py
python presupuesto_pipeline.py