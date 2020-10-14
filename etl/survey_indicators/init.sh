# ene
bamboo-cli --folder ene/ --entry ene_pipeline
bamboo-cli --folder ene/ --entry dimensions_pipeline --pk="category_id" --table_name="dim_category_ene" 
bamboo-cli --folder ene/ --entry dimensions_pipeline --pk="indicator_id" --table_name="dim_indicator_ene"

# enave
bamboo-cli --folder enave/ --entry enave_pipeline
bamboo-cli --folder enave/ --entry dimensions_pipeline --pk="category_id" --table_name="dim_category_enave" 
bamboo-cli --folder enave/ --entry dimensions_pipeline --pk="indicator_id" --table_name="dim_indicator_enave" 

# enaho
bamboo-cli --folder enaho/ --entry enaho_pipeline
bamboo-cli --folder enaho/ --entry dimensions_pipeline --pk="category_id" --table_name="dim_category_enaho" 
bamboo-cli --folder enaho/ --entry dimensions_pipeline --pk="indicator_id" --table_name="dim_indicator_enaho" 
bamboo-cli --folder enaho/ --entry dimensions_pipeline --pk="region_id" --table_name="dim_region_enaho" 
bamboo-cli --folder enaho/ --entry dimensions_pipeline --pk="geo_id" --table_name="dim_geo_enaho" 