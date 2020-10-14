bamboo-cli --folder ene/ --entry ene_pipeline
bamboo-cli --folder ene/ --entry dimensions_pipeline --pk="category_id" --table_name="dim_category_ene" 
bamboo-cli --folder ene/ --entry dimensions_pipeline --pk="indicator_id" --table_name="dim_indicator_ene"
bamboo-cli --folder enave/ --entry enave_pipeline
bamboo-cli --folder enave/ --entry dimensions_pipeline --pk="category_id" --table_name="dim_category_enave" 
bamboo-cli --folder enave/ --entry dimensions_pipeline --pk="indicator_id" --table_name="dim_indicator_enave" 