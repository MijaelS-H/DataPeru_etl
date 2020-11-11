from .dimensions_infocultura_month import run_pipeline as run_pipeline_dim_infocultura_month
from .dimensions_infocultura_year import run_pipeline as run_pipeline_dim_infocultura_year
from .cultura_infocultura_month import run_pipeline as run_pipeline_infocultura_month
from .cultura_infocultura_year import run_pipeline as run_pipeline_infocultura_year


def run_pipeline(params: dict):
    run_pipeline_dim_infocultura_month(params)
    run_pipeline_dim_infocultura_year(params)
    run_pipeline_infocultura_month(params)
    run_pipeline_infocultura_year(params)


if __name__ == "__main__":
    import sys
    from os import path
    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline({
        "connector": path.join(__dirname, "..", "..", "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
