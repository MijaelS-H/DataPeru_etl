from etl.datasets import run_pipeline
run_pipeline({"connector":"etl/conns.yaml", "datasets": "/mnt/d/Datawheel/Peru/datasets"})