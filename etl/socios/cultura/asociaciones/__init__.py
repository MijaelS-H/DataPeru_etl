from .cultura_asociaciones import run_pipeline as cultura_asociaciones_pipeline
from .dimensions_pipeline import run_pipeline as dim_asociaciones_pipeline


def run_pipeline(params: dict):
    dim_asociaciones_pipeline(params)
    cultura_asociaciones_pipeline(params)


if __name__ == "__main__":
    import sys
    from os import path
    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline({
        "connector": path.join(__dirname, "..", "..", "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
