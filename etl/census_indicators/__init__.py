from .cenama import run_pipeline as run_pipeline_cenama
from .concytec import run_pipeline as run_pipeline_concytec
from .renamu_municipalities import \
    run_pipeline as run_pipeline_renamu_municipalities
from .renamu_populated_centers import \
    run_pipeline as run_pipeline_renamu_populated_centers


def run_pipeline(params: dict):
    run_pipeline_cenama(params)
    run_pipeline_concytec(params)
    # run_pipeline_renamu_municipalities(params)
    run_pipeline_renamu_populated_centers(params)


if __name__ == "__main__":
    import sys

    run_pipeline({
        "connector": "../conns.yaml", 
        "datasets": sys.argv[1]
    })
