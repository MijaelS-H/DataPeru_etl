import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from etl.helpers import format_text

puerto_dict = [
    {0: "Atico", 1: 1},
    {0: "Bayovar", 1: 2},
    {0: "Callao", 1: 3},
    {0: "Casma", 1: 4},
    {0: "Chicama", 1: 5},
    {0: "Chimbote", 1: 6},
    {0: "Culebras", 1: 7},
    {0: "Huacho", 1: 8},
    {0: "Huarmey", 1: 9},
    {0: "Ilo", 1: 10},
    {0: "Lomas", 1: 11},
    {0: "Mancora", 1: 12},
    {0: "Matarani", 1: 13},
    {0: "Mollendo", 1: 14},
    {0: "Otros", 1: 15},
    {0: "Paita", 1: 16},
    {0: "Pimentel", 1: 17},
    {0: "Pisco", 1: 18},
    {0: "Puerto de Salaverry", 1: 19},
    {0: "Samanco", 1: 20},
    {0: "San José", 1: 21},
    {0: "Supe", 1: 22},
    {0: "Tambo de Mora", 1: 23},
    {0: "Vegeta", 1: 24},
    {0: "Chancay", 1: 25},
    {0: "Quilca", 1: 26},
    {0: "Zorritos", 1: 27}
]

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        # Creates dataframe from dictionary
        df = pd.DataFrame(puerto_dict)
        df.columns = ['port_name', 'port_id']

        return df

class ITPPuertosDesembarquePipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            "port_name":                 "String",
            "port_id":                   "UInt8",
        }

        transform_step = TransformStep()
        load_step = LoadStep('dim_shared_ports', db_connector, if_exists='drop', pk=['port_id'], 
            dtype=dtype
        )

        return [transform_step, load_step]

if __name__ == '__main__':
    pp = ITPPuertosDesembarquePipeline()
    pp.run({})






