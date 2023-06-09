from .cenama_pipeline import run_pipeline as run_pipeline_inei_cenama
from .dimension_pipeline import run_pipeline as run_pipeline_dim_market


def run_pipeline(params: dict):
    run_pipeline_dim_market(params)
    run_pipeline_inei_cenama(params)


if __name__ == "__main__":
    import sys
    from os import path
    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline({
        "connector": path.join(__dirname, "..", "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
