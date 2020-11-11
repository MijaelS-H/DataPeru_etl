from .cultura_asociaciones import run_pipeline as cultura_asociaciones_pipeline
from .dimensions_pipeline import run_pipeline as cultura_dimensiones_pipeline


def run_pipeline(params: dict):
    dim_asociaciones_pipeline(params)
    cultura_asociaciones_pipeline(params)


if __name__ == "__main__":
    import sys

    run_pipeline({
        "connector": params["connector"],
        "datasets": sys.argv[1]
    })
