from .dinamica_agricola_pipeline import run_pipeline as run_pipeline_minagri_agricola
from .dinamica_pecuaria_pipeline import run_pipeline as run_pipeline_minagri_pecuaria


def run_pipeline(params: dict):
    run_pipeline_minagri_agricola(params)
    run_pipeline_minagri_pecuaria(params)


if __name__ == "__main__":
    import sys

    run_pipeline({
        "connector": "../conns.yaml",
        "datasets": sys.argv[1]
    })
