from .concytec_pipeline import run_pipeline as run_pipeline_concytec


def run_pipeline(params: dict):
    run_pipeline_concytec(params)


if __name__ == "__main__":
    import sys
    
    run_pipeline({
        "connector": "../../conns.yaml", 
        "datasets": sys.argv[1]
    })
