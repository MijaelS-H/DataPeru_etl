from .eea.eea_pipeline import run_pipeline as run_pipeline_eea
from .eea.dimensions_pipeline import run_pipeline as run_pipeline_dim_eea
from .enaho.enaho_pipeline import run_pipeline as run_pipeline_enaho
from .enaho.dimensions_pipeline import run_pipeline as run_pipeline_dim_enaho
from .enapres.enapres_pipeline import run_pipeline as run_pipeline_enapres
from .enapres.dimensions_pipeline import run_pipeline as run_pipeline_dim_enapres
from .enave.enave_pipeline import run_pipeline as run_pipeline_enave
from .enave.dimensions_pipeline import run_pipeline as run_pipeline_dim_enave
from .ene.ene_pipeline import run_pipeline as run_pipeline_ene
from .ene.dimensions_pipeline import run_pipeline as run_pipeline_dim_ene
from .enhat.enhat_pipeline import run_pipeline as run_pipeline_enhat
from .enhat.dimensions_pipeline import run_pipeline as run_pipeline_dim_enhat
from .enima.enima_pipeline import run_pipeline as run_pipeline_enima
from .enima.dimensions_pipeline import run_pipeline as run_pipeline_dim_enima

def run_pipeline(params: dict):
    run_pipeline_dim_eea(params)
    run_pipeline_eea(params)
    run_pipeline_dim_enaho(params)
    run_pipeline_enaho(params)
    run_pipeline_dim_enapres(params)
    run_pipeline_enapres(params)
    run_pipeline_dim_enave(params)
    run_pipeline_enave(params)
    run_pipeline_dim_ene(params)
    run_pipeline_ene(params)
    run_pipeline_dim_enhat(params)
    run_pipeline_enhat(params)
    run_pipeline_dim_enima(params)
    run_pipeline_enima(params)


if __name__ == "__main__":
    import sys
    from os import path
    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })

