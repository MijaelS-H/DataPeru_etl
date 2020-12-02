from .actividad_economica_54 import run_pipeline as run_pipeline_acti_eco_54
from .cad_prod_cite import run_pipeline as run_pipeline_prod_cite
from .cadenas_productivas_atendidas import run_pipeline as run_pipeline_cadenas_productivas
from .ciiu_division import run_pipeline as run_pipeline_ciiu_division
from .ciiu_group import run_pipeline as run_pipeline_ciiu_grupo

from .ciiu_production_ids import run_pipeline as run_pipeline_ciiu_prod_ids
from .ciiu_rev3 import run_pipeline as run_pipeline_ciiu_rev
from .ciiu_section import run_pipeline as run_pipeline_ciiu_section
from .ciiu import run_pipeline as run_pipeline_ciiu
from .cite import run_pipeline as run_pipeline_cite

from .contribuyente_cite import run_pipeline as run_pipeline_contribuyente_cite
from .countries import run_pipeline as run_pipeline_countries
from .customs_measure_unity import run_pipeline as run_pipeline_customs_measure
from .customs import run_pipeline as run_pipeline_customs
from .estado_cite import run_pipeline as run_pipeline_estado_cite

from .hs import run_pipeline as run_pipeline_hs
from .hs10 import run_pipeline as run_pipeline_hs10
from .itp_ports import run_pipeline as run_pipeline_itp_ports
from .time_quarter import run_pipeline as run_pipeline_time_quarter
from .time_month import run_pipeline as run_pipeline_time_month
from .time import run_pipeline as run_pipeline_time

from .trade_flow import run_pipeline as run_pipeline_trade_flow
from .ubigeo_department import run_pipeline as run_pipeline_ubigeo_department
from .ubigeo_district import run_pipeline as run_pipeline_ubigeo_district
from .ubigeo_nation import run_pipeline as run_pipeline_ubigeo_nation
from .ubigeo_province import run_pipeline as run_pipeline_ubigeo_province


def run_pipeline(params: dict):
    run_pipeline_acti_eco_54(params)
    run_pipeline_prod_cite(params)
    run_pipeline_cadenas_productivas(params)
    run_pipeline_ciiu_division(params)
    run_pipeline_ciiu_grupo(params)

    run_pipeline_ciiu_prod_ids(params)
    run_pipeline_ciiu_rev(params)
    run_pipeline_ciiu_section(params)
    run_pipeline_ciiu(params)
    run_pipeline_cite(params)

    run_pipeline_contribuyente_cite(params)
    run_pipeline_countries(params)
    run_pipeline_customs_measure(params)
    run_pipeline_customs(params)
    run_pipeline_estado_cite(params)

    run_pipeline_hs(params)
    run_pipeline_hs10(params)
    run_pipeline_itp_ports(params)
    run_pipeline_time_quarter(params)
    run_pipeline_time_month(params)
    run_pipeline_time(params)

    run_pipeline_trade_flow(params)
    run_pipeline_ubigeo_department(params)
    run_pipeline_ubigeo_district(params)
    run_pipeline_ubigeo_nation(params)
    run_pipeline_ubigeo_province(params)


if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
