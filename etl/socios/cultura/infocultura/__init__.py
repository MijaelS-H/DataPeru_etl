from .dimensions_infocultura_month import run_pipeline as dim_infocultura_month
from .dimensions_infocultura_year import run_pipeline as dim_infocultura_year
from .cultura_infocultura_month import run_pipeline as infocultura_month
from .cultura_infocultura_year import run_pipeline as infocultura_year


def run_pipeline(params: dict):
    dim_infocultura_month(params)
    dim_infocultura_year(params)
    infocultura_month(params)
    infocultura_year(params)


if __name__ == "__main__":
    import sys
    from os import path
    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline({
        "connector": path.join(__dirname, "..", "..", "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
