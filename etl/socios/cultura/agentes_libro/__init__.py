from .cultura_agentes import run_pipeline as cultura_pipeline_agentes
from .dimensions_pipeline import run_pipeline as dimensiones_cultura_pipeline


def run_pipeline(params: dict):
    dim_agentes_pipeline(params)
    cultura_pipeline_agentes(params)


if __name__ == "__main__":
    import sys

    run_pipeline({
        "connector": params["connector"],
        "datasets": sys.argv[1]
    })
