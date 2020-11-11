from .cultura_cine import run_pipeline as cultura_pipeline_cine
from .dimensions_pipeline import run_pipeline as dim_cine_pipeline


def run_pipeline(params: dict):
    dim_cine_pipeline(params)
    cultura_pipeline_cine(params)


if __name__ == "__main__":
    import sys

    run_pipeline({
        "connector": params["connector"],
        "datasets": sys.argv[1]
    })
