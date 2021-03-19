from .download_mef_gastos import run_pipeline as run_pipeline_download_gastos
from .download_mef_ingresos import run_pipeline as run_pipeline_download_ingresos
from .dim_mef_gastos import run_pipeline as run_pipeline_dim_gastos
from .dim_mef_ingresos import run_pipeline as run_pipeline_dim_ingresos
from .mef_gastos_pipeline import run_pipeline as run_pipeline_presupuesto_gastos
from .mef_ingresos_pipeline import run_pipeline as run_pipeline_presupuesto_ingresos


def run_pipeline(params: dict):
    run_pipeline_download_gastos(params)
    run_pipeline_download_ingresos(params)
    run_pipeline_dim_gastos(params)
    run_pipeline_dim_ingresos(params)
    run_pipeline_presupuesto_gastos(params)
    run_pipeline_presupuesto_ingresos(params)


if __name__ == "__main__":
    import sys
    from os import path
    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml")
    })
