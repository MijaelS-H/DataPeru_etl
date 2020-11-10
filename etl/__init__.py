from .census_indicators import run_pipeline as run_pipeline_census_indicators
from .minagri import run_pipeline as run_pipeline_minagri
from .presupuesto import run_pipeline as run_pipeline_presupuesto


def run_pipeline(params: dict):
    run_pipeline_census_indicators(params)
    run_pipeline_minagri(params)
    run_pipeline_presupuesto(params)


if __name__ == "__main__":
    import sys

    run_pipeline({
      "connector": "./conns.yaml",
      "datasets": sys.argv[1]
    })
