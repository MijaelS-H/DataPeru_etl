from .cultura_cine import run_pipeline as cultura_pipeline_cine
from .dimensions_pipeline import run_pipeline as dim_cine_pipeline


def run_pipeline(params: dict):
    dim_cine_pipeline(params)
    cultura_pipeline_cine(params)


if __name__ == "__main__":
    import sys
    from os import path
    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline({
        "connector": path.join(__dirname, "..", "..", "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
