from .agentes_libro.cultura_agentes import run_pipeline as cultura_pipeline_agentes
from .agentes_libro.dimensions_pipeline import run_pipeline as dim_agentes_pipeline
from .asociaciones.cultura_asociaciones import run_pipeline as cultura_asociaciones_pipeline
from .asociaciones.dimensions_pipeline import run_pipeline as dim_asociaciones_pipeline
from .cine.cultura_cine import run_pipeline as cultura_pipeline_cine
from .cine.dimensions_pipeline import run_pipeline as dim_cine_pipeline
from .estimulos_economicos.cultura_eec import run_pipeline as cultura_pipeline_estimulos_eco
from .estimulos_economicos.dimensions_pipeline import run_pipeline as dim_estimulos_eco_pipeline
from .infocultura.dimensions_infocultura_month import run_pipeline as dim_infocultura_month
from .infocultura.dimensions_infocultura_year import run_pipeline as dim_infocultura_year
from .infocultura.cultura_infocultura_month import run_pipeline as infocultura_month
from .infocultura.cultura_infocultura_year import run_pipeline as infocultura_year

def run_pipeline(params: dict):
    dim_agentes_pipeline(params)
    cultura_pipeline_agentes(params)
    dim_asociaciones_pipeline(params)
    cultura_asociaciones_pipeline(params)
    dim_cine_pipeline(params)
    cultura_pipeline_cine(params)
    dim_estimulos_eco_pipeline
    cultura_pipeline_estimulos_eco
    dim_infocultura_month(params)
    dim_infocultura_year(params)
    infocultura_month(params)
    infocultura_year(params)

if __name__ == "__main__":
    import sys
    from os import path
    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline({
        "connector": path.join(__dirname, "..", "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
