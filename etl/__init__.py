from .census_indicators import run_pipeline as run_pipeline_census_indicators
from .minagri import run_pipeline as run_pipeline_minagri


def run_pipeline(params: dict):
    run_pipeline_census_indicators(params)
    run_pipeline_minagri(params)


if __name__ == "__main__":
    import sys

    run_pipeline({
      "connector": "./conns.yaml",
      "datasets": sys.argv[1]
    })
