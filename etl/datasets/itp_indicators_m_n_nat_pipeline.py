import pandas as pd
from bamboo_lib.helpers import grab_parent_dir
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
path = grab_parent_dir('../../') + "/datasets/20200318"


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        month_dict = {'Enero': '01', 'Febrero': '02', 'Marzo': '03', 'Abril': '04', 'Mayo': '05', 'Junio': '06', 'Julio': '07', 'Agosto': '08', 'Septiembre': '09', 'Setiembre': '09', 'Octubre': '10', 'Noviembre': '11', 'Diciembre': '12'}

        # Loading data
        df1 = pd.read_excel(io = '{}/{}/{}'.format(path, 'A. Economía', "A.158.xlsx"), skiprows = (0,1,2))
        df2 = pd.read_excel(io = '{}/{}/{}'.format(path, 'A. Economía', "A.159.xlsx"), skiprows = (0,1,2))

        # Common steps
        for item in [df1,df2]:
            item["Año"].fillna(method = "ffill", inplace = True)
            item["Año"] = item["Año"].astype(int)
            item["Mes"].replace(month_dict, inplace = True)
            item["date_id"] = item["Año"].astype(str) + item["Mes"].astype(str)

        # Renaming columns to understandable names
        df1.rename(columns = {"Índice": "ipc_base_2011_observado", "Mensual": "ipc_base_2011_var_mes_anterior", "Acumulada": "ipc_base_2011_var_acumulado", "Anual": "ipc_base_2011_var_anio_anterior"}, inplace = True)
        df2.rename(columns = {"Índice": "ipm_base_2013_observado", "Mensual": "ipm_base_2013_var_mes_anterior", "Acumulada": "ipm_base_2013_var_acumulado"}, inplace = True)

        # Formatting data
        df1["ipc_base_2011_var_anio_anterior"].replace("-", pd.np.nan, inplace = True)
        df1 = df1[["date_id", "ipc_base_2011_observado", "ipc_base_2011_var_mes_anterior", "ipc_base_2011_var_acumulado", "ipc_base_2011_var_anio_anterior"]]

        df = pd.merge(df1,  df2[['date_id', 'ipm_base_2013_observado', 'ipm_base_2013_var_mes_anterior', 'ipm_base_2013_var_acumulado']], on = 'date_id', how = 'left')

        for i in df.columns:
            df[i] = df[i].astype(float)

        return df

class itp_indicators_m_n_nat_pipeline(EasyPipeline):
  
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))
        dtype = {
            "date_id":                                          "UInt16",
            "ipc_base_2011_observado":                          "Float",
            "ipc_base_2011_var_mes_anterior":                   "Float",
            "ipc_base_2011_var_acumulado":                      "Float",
            "ipc_base_2011_var_anio_anterior":                  "Float",
            "ipm_base_2013_observado":                          "Float",
            "ipm_base_2013_var_mes_anterior":                   "Float",
            "ipm_base_2013_var_acumulado":                      "Float"
            }

        transform_step = TransformStep()
        load_step = LoadStep(
            "itp_indicators_m_n_nat", db_connector, if_exists="drop", pk=["date_id"], dtype=dtype, 
            nullable_list=["ipc_base_2011_observado", "ipc_base_2011_var_mes_anterior", "ipc_base_2011_var_acumulado", "ipc_base_2011_var_anio_anterior", 
            "ipm_base_2013_observado", "ipm_base_2013_var_mes_anterior", "ipm_base_2013_var_acumulado"]
        )

        return [transform_step, load_step]

if __name__ == "__main__":
    pp = itp_indicators_m_n_nat_pipeline()
    pp.run({})