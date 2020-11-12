from .itp_cite_clientes_aspecto import run_pipeline as itp_pipeline_clientes_aspecto
from .itp_cite_ejecucion_presupuestal import run_pipeline as itp_pipeline_ejecucion_presupuestal
from .itp_cite_empresas_ciiu_agg import run_pipeline as itp_pipeline_empresas_ciiu_agg
from .itp_cite_empresas_ciiu import run_pipeline as itp_pipeline_empresas_ciiu
from .itp_cite_empresas_contribuyente_agg import run_pipeline as itp_pipeline_emp_contribuyentes_agg
from .itp_cite_empresas_contribuyente import run_pipeline as itp_pipeline_emp_contribuyentes

from .itp_cite_empresas_tipo_agg import run_pipeline as itp_pipeline_emp_tipo_agg
from .itp_cite_empresas_tipo import run_pipeline as itp_pipeline_emp_tipo
from .itp_cite_inversion import run_pipeline as itp_pipeline_inversion
from .itp_cite_partidas_atendidas import run_pipeline as itp_pipeline_partidas_atendidas
from .itp_cite_partidas import run_pipeline as itp_pipeline_partidas

from .itp_cite_pim import run_pipeline as itp_pipeline_pim
from .itp_cite_servicios_subcategorias_agg import run_pipeline as itp_pipeline_subcategorias_agg
from .itp_cite_servicios_subcategorias import run_pipeline as itp_pipeline_subcategorias
from .itp_cite_trabajadores import run_pipeline as itp_pipeline_trabajadores


def run_pipeline(params: dict):
    itp_pipeline_clientes_aspecto(params)
    itp_pipeline_ejecucion_presupuestal(params)
    itp_pipeline_empresas_ciiu_agg(params)
    itp_pipeline_empresas_ciiu(params)
    itp_pipeline_emp_contribuyentes_agg(params)
    itp_pipeline_emp_contribuyentes(params)

    itp_pipeline_emp_tipo_agg(params)
    itp_pipeline_emp_tipo(params)
    itp_pipeline_inversion(params)
    itp_pipeline_partidas_atendidas(params)
    itp_pipeline_partidas(params)

    itp_pipeline_pim(params)
    itp_pipeline_subcategorias_agg(params)
    itp_pipeline_subcategorias(params)
    itp_pipeline_trabajadores(params)


if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
