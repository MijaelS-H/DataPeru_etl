from .inei_population_y_age_nat_travel_pipeline import run_pipeline as run_pipeline_age_travel
from .inei_population_y_gender_dep_pipeline import run_pipeline as run_pipeline_gender_department
from .inei_population_y_gender_nat_travel_pipeline import run_pipeline as run_pipeline_gender_travel
from .inei_population_y_n_dep_pipeline import run_pipeline as run_pipeline_year_deparment
from .inei_population_y_n_dep_urb_rur_pipeline import run_pipeline as run_pipeline_urban_rural_dep_department

from .itp_indicators_m_n_nat_pipeline import run_pipeline as run_pipeline_monthly_national
from .itp_indicators_q_n_nat_pipeline import run_pipeline as run_pipeline_quarter_national
from .itp_indicators_y_d_ports_pipeline import run_pipeline as run_pipeline_yearly_ports
from .itp_indicators_y_n_manu_mill_soles_2007_pipeline import run_pipeline as run_pipeline_manufacture_stats
from .itp_indicators_y_n_nat_54_pipeline import run_pipeline as run_pipeline_activities_national

from .itp_indicators_y_n_nat_pipeline import run_pipeline as run_pipeline_year_national
from .itp_indicators_y_n_prod_ciiu_group_pipeline import run_pipeline as run_pipeline_yearly_ciiu_group
from .itp_indicators_y_n_tourism_capacity_pipeline import run_pipeline as run_pipeline_yearly_tourims_c
from .itp_indicators_y_n_tourism_pipeline import run_pipeline as run_pipeline_yearly_tourims
from .proinversion_fdi_y_origin_nat_pipeline import run_pipeline as run_pipeline_fdi_national

from .itp_indicators_y_act_dept_pipeline import run_pipeline as run_pipeline_activity_department
from .inei_population_y_n_gender_age_urb_rur_pipeline import run_pipeline as run_pipeline_gender_age_urb_rur_nat
from .proinversion_fdi_y_origin_sector_pipeline import run_pipeline as run_pipeline_fdi_sector
from .inei_population_y_nat_nbi_pipeline import run_pipeline as run_pipeline_national_nbi
from .inei_population_y_n_illiteracy_pipeline import run_pipeline as run_pipeline_illiteracy
from .inei_population_y_gender_geo_eap_pipeline import run_pipeline as run_pipeline_geography_eap

def run_pipeline(params: dict):
    run_pipeline_age_travel(params)
    run_pipeline_gender_department(params)
    run_pipeline_gender_travel(params)
    run_pipeline_year_deparment(params)
    run_pipeline_urban_rural_dep_department(params)

    run_pipeline_monthly_national(params)
    run_pipeline_quarter_national(params)
    run_pipeline_yearly_ports(params)
    run_pipeline_manufacture_stats(params)
    run_pipeline_activities_national(params)

    run_pipeline_year_national(params)
    run_pipeline_yearly_ciiu_group(params)
    run_pipeline_yearly_tourims_c(params)
    run_pipeline_yearly_tourims(params)
    run_pipeline_fdi_national(params)

    run_pipeline_activity_department(params)
    run_pipeline_gender_age_urb_rur_nat(params)
    run_pipeline_fdi_sector(params)
    run_pipeline_national_nbi(params)
    run_pipeline_illiteracy(params)
    run_pipeline_geography_eap(params)

if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
