from .dim_gastos_pipeline import run_pipeline as run_pipeline_dim_gastos
from .dim_ingresos_pipeline import run_pipeline as run_pipeline_dim_ingresos
from .presupuesto_gastos_pipeline import run_pipeline as run_pipeline_presupuesto_gastos
from .presupuesto_ingresos_pipeline import run_pipeline as run_pipeline_presupuesto_ingresos


def run_pipeline(params: dict):
    run_pipeline_dim_gastos(params)
    run_pipeline_dim_ingresos(params)
    run_pipeline_presupuesto_gastos(params)
    run_pipeline_presupuesto_ingresos(params)


if __name__ == "__main__":
    import sys

    run_pipeline({
        "connector": "../conns.yaml",
        "datasets": sys.argv[1]
    })
