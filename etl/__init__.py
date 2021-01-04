from .census_indicators import run_pipeline as run_pipeline_census_indicators
from .cite import run_pipeline as run_pipeline_cite
from .datasets import run_pipeline as run_pipeline_datasets
from .minagri import run_pipeline as run_pipeline_minagri
from .presupuesto import run_pipeline as run_pipeline_presupuesto
from .profile_dimensions import run_pipeline as run_pipeline_profile_dimensions
from .shared import run_pipeline as run_pipeline_shared
from .socios import run_pipeline as run_pipeline_socios
from .sunat import run_pipeline as run_pipeline_sunat
from .survey_indicators import run_pipeline as run_pipeline_survey_indicators


def run_pipeline(params: dict):
    run_pipeline_shared(params)
    run_pipeline_census_indicators(params)
    run_pipeline_cite(params)
    run_pipeline_datasets(params)
    run_pipeline_minagri(params)
    run_pipeline_presupuesto(params)
    run_pipeline_profile_dimensions(params)
    run_pipeline_socios(params)
    run_pipeline_sunat(params)
    run_pipeline_survey_indicators(params)


if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline({
        "connector": path.join(__dirname, ".", "conns.yaml"),
        "datasets": sys.argv[1],
    })
