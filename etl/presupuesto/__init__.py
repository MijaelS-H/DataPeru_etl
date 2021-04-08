from .download_pipeline import run_pipeline as run_pipeline_download
from .temp_dims import run_pipeline as run_pipeline_temp_dims
from .dim_gastos_pipeline import run_pipeline as run_pipeline_dim_gastos
from .dim_ingresos_pipeline import run_pipeline as run_pipeline_dim_ingresos
from .presupuesto_gastos_pipeline import run_pipeline as run_pipeline_presupuesto_gastos
from .presupuesto_ingresos_pipeline import run_pipeline as run_pipeline_presupuesto_ingresos


def run_pipeline(params: dict):
    # run_pipeline_download(params)
    run_pipeline_temp_dims(params)
    run_pipeline_dim_gastos(params)
    run_pipeline_dim_ingresos(params)
    run_pipeline_presupuesto_gastos(params)
    run_pipeline_presupuesto_ingresos(params)


if __name__ == "__main__":
    import sys
    from os import path
    __dirname = path.dirname(path.realpath(__file__))
    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
