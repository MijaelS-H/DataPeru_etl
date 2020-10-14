bamboo-cli --folder . --entry ene_pipeline
bamboo-cli --folder . --entry dimensions_pipeline --pk="category_id" --table_name="dim_category_ene" 
bamboo-cli --folder . --entry dimensions_pipeline --pk="indicator_id" --table_name="dim_indicator_ene" 
bamboo-cli --folder enaho/ --entry enaho_pipeline

bamboo-cli --folder enaho/ --entry dimensions_pipeline --pk="category_id" --table_name="dim_category_enaho" 
bamboo-cli --folder enaho/ --entry dimensions_pipeline --pk="indicator_id" --table_name="dim_indicator_enaho" 
bamboo-cli --folder enaho/ --entry dimensions_pipeline --pk="region_id" --table_name="dim_region_enaho" 
bamboo-cli --folder enaho/ --entry dimensions_pipeline --pk="geo_id" --table_name="dim_geo_enaho" 