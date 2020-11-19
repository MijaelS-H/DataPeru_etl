from .dimension_ubigeo import run_pipeline as run_dimension_ubigeo_pipeline


#from .inei_population_y_n_dep_urb_rur_pipeline import run_pipeline as run_pipeline_urban_rural_dep_department



def run_pipeline(params: dict):
    run_dimension_ubigeo_pipeline(params)


   # run_pipeline_urban_rural_dep_department(params)


if __name__ == "__main__":
    import sys
    from os import path

    __dirname = path.dirname(path.realpath(__file__))

    run_pipeline({
        "connector": path.join(__dirname, "..", "conns.yaml"),
        "datasets": sys.argv[1]
    })
