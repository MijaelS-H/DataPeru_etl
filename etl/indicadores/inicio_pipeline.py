import pandas as pd
import os
import csv

from bamboo_lib.logger import logger
from bamboo_lib.helpers import grab_connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep

VARIABLE_DICT = { 
    1: "Tasa de desempleo",
    2: "Población ocupada",
    3: "Tasa de pobreza",
    4: "Ingreso promedio mensual",
    5: "Contribución al VAB",
    6: "Variación del VAB",
    7: "Porcentaje de MYPE",
    8: "Oficinas financieras",
    9: "Monto de depósitos",
    10: "Créditos directos",
    11: "MYPE accede a crédito inicial",
    12: "MYPE accede a crédito",
    13: "Investigadores cada 10 mil habitantes",
    14: "Inversión en I+D",
    15: "Exportaciones en valor FOB",
    16: "Ejecución presupuestal",
    17: "Ejecución presupuestal del ITP",
}


class OpenStep(PipelineStep):
    def run_step(self, prev, params):
        logger.info("Opening files from source folder...")
        
        ## 01. Inicio
        df = {i: pd.read_excel("../../data/indicadores_itp/DATASETS_01/Data sets DSE/01. Inicio/chart{}.xlsx".format(i)) for i in range(1,5)}


        ## 02. Produccion
        df[5] = pd.read_excel("../../data/indicadores_itp/DATASETS_01/Data sets DSE/02. Produccion/chart1.xlsx")
        df[6] = pd.read_excel("../../data/indicadores_itp/DATASETS_01/Data sets DSE/02. Produccion/chart4.xlsx")

        ## 03. Estructura empresarial
        df[7] = pd.read_excel("../../data/indicadores_itp/DATASETS_01/Data sets DSE/03. Estructura empresarial/chart3.xlsx")

        ## 04. Educación
        ## No hay datasets con dos dimensiones

        ## 05. Empleo
        ## No hay dataset con dos dimensiones

        ## 06. Financiamiento
        df[8] = pd.read_excel("../../data/indicadores_itp/DATASETS_01/Data sets DSE/06. Financiamiento/chart1.xlsx")
        df[9] = pd.read_excel("../../data/indicadores_itp/DATASETS_01/Data sets DSE/06. Financiamiento/chart3.xlsx")
        df[10] = pd.read_excel("../../data/indicadores_itp/DATASETS_01/Data sets DSE/06. Financiamiento/chart5.xlsx")
        df[11] = pd.read_excel("../../data/indicadores_itp/DATASETS_01/Data sets DSE/06. Financiamiento/chart7.xlsx")
        df[12] = pd.read_excel("../../data/indicadores_itp/DATASETS_01/Data sets DSE/06. Financiamiento/chart8.xlsx")

        ## 07. I+D+i
        df[13] = pd.read_excel("../../data/indicadores_itp/DATASETS_01/Data sets DSE/07. I+D+i/chart2.xlsx")
        df[14] = pd.read_excel("../../data/indicadores_itp/DATASETS_01/Data sets DSE/07. I+D+i/chart4.xlsx")

        ## 08. TIC
        ## no hay dataset con dos dimensiones

        ## 09. Exportaciones
        df[15] = pd.read_excel("../../data/indicadores_itp/DATASETS_01/Data sets DSE/09. Exportaciones/chart1.xlsx")

        ## 10. Presupuesto e inversion
        df[16] = pd.read_excel("../../data/indicadores_itp/DATASETS_01/Data sets DSE/10. Presupuesto e inversion/chart4.xlsx")
        df[17] = pd.read_excel("../../data/indicadores_itp/DATASETS_01/Data sets DSE/10. Presupuesto e inversion/chart8.xlsx")
        
        
        return df

class TidyStep(PipelineStep):
    def run_step(self, prev, params):
        logger.info("Tidying up DataFrame...")

        df = prev
        
        df[11]['year'] = 2017
        df[12]['year'] = 2017
        df[13]['year'] = 2015
        df[15]['year'] = 2018
        
        for i in range(1,18):
            df[i]["region"] = df[i]["region"].str.title()
            df[i]["data_origin"] = "ENAHO - INEI" if i in [1,2,3,4] else "INEI" if i in [5,6,8,9,10] else "SUNAT" if i in [7,15] else  "ENE" if i in [11,12]  else "SIAF" if i in [16,17] else "CONCYTEC" if i in [13,14] else "N/A"
            df[i] = df[i].rename(columns={"año":"year",
            "valor_porcentaje": "value",
            "valor_numerico": "value",
            "valor_numerico (millones)": "value",
            "valor_numerico (millones dolares)": "value",
            "valor_numerico (por cada 10 mil)" : "value"})
            df[i]["variable"] = VARIABLE_DICT[i]
            df[i]["year"] = df[i]["year"].astype(int)
        
        
        df[14]['value'] =  df[14]['value']*1000000
        df[15]['value'] =  df[15]['value']*1000000
        
        df_list = [df[i] for i in range(1,18)]
        df = pd.concat(df_list, ignore_index=True)
        df = df[['region','year','data_origin','variable','value']]
        
        return df

class RegionDimensionStep(PipelineStep):
    def run_step(self, prev, params):
        logger.info("Creating Region Dimension...")

        df = prev

        region_list = list(df["region"].unique())
        df_region = pd.DataFrame({"region_id": list(range(len(region_list))), "region_name": sorted(region_list)})
        

        return df, df_region

class VariableDimensionStep(PipelineStep):
    def run_step(self, prev, params):
        logger.info("Creating Variable Dimension...")

        df, df_region = prev
        

        df_var = df["variable"].copy()
        df_var = df_var.drop_duplicates().reset_index(drop=True)
        df_var = pd.DataFrame(df_var)
        df_var["variable_id"] = df_var.index
        

        return  df, df_region, df_var

class OriginDimensionStep(PipelineStep):
    def run_step(self, prev, params):
        logger.info("Creating Origin Dimension...")

        df, df_region, df_var = prev

        df_origin = df["data_origin"].copy()
        df_origin = df_origin.drop_duplicates().reset_index(drop=True)
        df_origin = pd.DataFrame(df_origin)
        df_origin["data_origin_id"] = df_origin.index
        
        return df, df_region, df_var, df_origin


class FactTableStep(PipelineStep):
    def run_step(self, prev, params):
        logger.info("Creating Fact Table...")

        df, df_region, df_var, df_origin = prev
        
        
        region_map = {k:v for (k,v) in zip(df_region["region_name"], df_region["region_id"])}
        df["region_id"] = df["region"].map(region_map)
        
        
        
        origin_map = {k:v for (k,v) in zip(df_origin["data_origin"], df_origin["data_origin_id"])}
        df["data_origin_id"] = df["data_origin"].map(origin_map)
        
        
        variable_map = {k:v for (k,v) in zip(df_var["variable"], df_var["variable_id"])}
                
        df["variable_id"] = df["variable"].map(variable_map)
        
        df = df[["region_id", "year", "data_origin_id", "variable_id",  "value"]]
        
        
        return df


class InicioPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter("output-db", dtype=str),
            Parameter("ingest", dtype=bool)
        ]

    @staticmethod
    def steps(params):
        db_connector = grab_connector(__file__, params.get("output-db"))

        open_step = OpenStep()
        tidy_step = TidyStep()

        region_step = RegionDimensionStep()
        load_region = LoadStep(
            table_name="inicio_dim_region", 
            connector=db_connector, 
            if_exists="drop", 
            pk=["region_id"],
            dtype={"region_id":"UInt8","region_name":"String"},
            nullable_list=[]
        )

        variable_step = VariableDimensionStep()
        load_variable = LoadStep(
            table_name="inicio_dim_variable", 
            connector=db_connector, 
            if_exists="drop", 
            pk=["variable_id"],
            dtype={"variable_id":"UInt8","variable_name":"String"},
            nullable_list=[]
        )

        origin_step = OriginDimensionStep()
        load_origin = LoadStep(
            table_name="inicio_dim_origin", 
            connector=db_connector, 
            if_exists="drop", 
            pk=["data_origin_id"],
            dtype={"data_origin_id":"UInt8","data_origin":"String"},
            nullable_list=[]
        )

        fact_step = FactTableStep()
        load_fact = LoadStep(
            table_name="inicio_fact", 
            connector=db_connector, 
            if_exists="drop", 
            pk=["region_id"],
            dtype={"region_id":"UInt8","year":"UInt8","data_origin_id":"UInt8","variable_id":"UInt8","percentage":"Float64"},
            nullable_list=[]
        )

        if params.get("ingest")==True:
            steps = [open_step, tidy_step, region_step, load_region, variable_step, load_variable, origin_step, load_origin, fact_step, load_fact]
        else:
            steps = [open_step, tidy_step, region_step, variable_step, origin_step, fact_step]

        return steps


if __name__ == "__main__":
    inicio_pipeline = InicioPipeline()
    inicio_pipeline.run(
        {
            "output-db": "clickhouse-local",
            "ingest": False
        }
    )
