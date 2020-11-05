from .census_indicators import run_pipeline as run_pipeline_census_indicators


def run_pipeline(params: dict):
    run_pipeline_census_indicators(params)


if __name__ == "__main__":
    import sys

    run_pipeline({
      "connector": "./conns.yaml",
      "datasets": sys.argv[1]
    })
