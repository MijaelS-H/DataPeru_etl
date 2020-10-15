import pandas as pd
from bamboo_lib.helpers import grab_parent_dir, query_to_df
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
path1 = grab_parent_dir('../../') + "/datasets/20200318"
path2 = grab_parent_dir('../../') + "/datasets/anexos"

country_list = ['España', 'Reino Unido', 'Chile', 'Estados Unidos de América', 'Países Bajos', 'Brasil', 'Colombia', 'Canadá', 'Panamá', 'México', 'Luxemburgo', 'Suiza', 'Singapur', 'Islas Bermudas ', 'Japón', 'China', 'Francia', 'Alemania', 'Islas Bahamas ', 'Bélgica', 'Italia', 'Ecuador', 'Uruguay', 'Islas Caimán', 'Suecia', 'Corea', 'Argentina', 'Portugal', 'Gran Bretaña', 'Liechtenstein', 'Dinamarca', 'Austria', 'Australia', 'Nueva Zelandia', 'Malta', 'U.E.A. (United Arab Emirates)', 'Venezuela', 'Bolivia', 'Honduras', 'Rusia', 'Otros 1/']


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        # Loading data
        df1 = pd.read_excel(io = '{}/{}/{}'.format(path1, 'A. Economía', "A.188.xlsx"), skiprows = (0,1,2,4))[0:41]

        dim_country_query = 'SELECT * FROM dim_shared_country'
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
        countries = query_to_df(db_connector, raw_query=dim_country_query)
        #countries = pd.read_csv(grab_parent_dir('../../') + "/datasets/anexos/countries.csv")

        # Transpose dataframe, adding new header and year column from index
        df1 = df1.T
        new_header = df1.iloc[0]
        df1 = df1[1:]
        df1.columns = new_header
        df1['year'] = df1.index

        # Melt step for countries
        df_1 = pd.melt(df1, id_vars = "year", value_vars = country_list, var_name = 'country_name_es', value_name = "ied_millones_USD")

        # Correcting minor typos
        df_1['year'].replace({"2018 P/": 2018}, inplace = True)
        df_1['country_name_es'].replace({"Otros 1/": "Otros"}, inplace = True)

        # Adding countries columns to dataframe
        df = pd.merge(df_1, countries[["iso3", "continent_id", "continent_es", "country_name_es"]], on = 'country_name_es', how = 'left')

        # Changing types to certain columns
        df["year"] = df["year"].astype(int)
        df["ied_millones_USD"] = df["ied_millones_USD"].astype(float)

        return df

class proinversion_fdi_y_origin_nat_pipeline(EasyPipeline):
  
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "year":                               "UInt16",
            "ied_millones_USD":                   "Float32",
            "continent_es":                       "String",
            "continent_id":                       "String",
            "country_name_es":                    "String",
            "iso3":                               "String"
            }

        transform_step = TransformStep()
        load_step = LoadStep(
            "proinversion_fdi_y_origin_nat", db_connector, if_exists="drop", pk=["year"], dtype=dtype, 
            nullable_list=["iso3", "continent_id", "continent_es"]
        )

        return [transform_step, load_step]

if __name__ == "__main__":
    pp = proinversion_fdi_y_origin_nat_pipeline()
    pp.run({})