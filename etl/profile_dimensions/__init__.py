from .dimension_ubigeo import run_pipeline as run_dimension_ubigeo_pipeline
from .dimension_ciiu import run_pipeline as run_dimension_ciiu_pipeline


def run_pipeline(params: dict):
    run_dimension_ubigeo_pipeline(params)
    run_dimension_ciiu_pipeline(params)


if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
