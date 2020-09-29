import pandas as pd
import os
import csv
from functools import reduce
from bamboo_lib.logger import logger
from bamboo_lib.helpers import grab_connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep

VARIABLE_DICT = { 
    1: "tasa_desempleo",
    2: "poblacion_ocupada",
    3: "tasa_pobreza",
    4: "ingreso_promedio_mensual",
    5: "contribucion_al_vab",
    6: "variacion_del_vab",
    7: "porcentaje_de_mype",
    8: "oficinas_financieras",
    9: "monto_de_depositos",
    10: "creditos_directos",
    11: "mype_accede_a_credito_inicial",
    12: "mype_accede_a_credito",
    13: "investigadores_cada_10_mil_habitantes",
    14: "inversion_en_id",
    15: "exportaciones_en_valor_fob",
    16: "ejecucion_presupuestal",
    17: "ejecucion_presupuestal_del_itp",
}

ACCENTS_MAP = {
    "Ancash": "Áncash",
    "Apurimac": "Apurímac",
    "Huanuco": "Huánuco",
    "Junin": "Junín",
    "Madre De Dios": "Madre de Dios",
    "San Martin": "San Martín"
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
            df[i] = df[i].rename(columns={"año":"year",
            "region" : 'department_name',
            "valor_porcentaje": "value",
            "valor_numerico": "value",
            "valor_numerico (millones)": "value",
            "valor_numerico (millones dolares)": "value",
            "valor_numerico (por cada 10 mil)" : "value"})
            df[i]["year"] = df[i]["year"].astype(int)
            df[i] = df[i][['department_name','year','value']]    

        df[14]['value'] =  df[14]['value']*1000000
        df[15]['value'] =  df[15]['value']*1000000

        for i in range(1,18):
             df[i] = df[i].rename(columns={"value": VARIABLE_DICT[i]})

        df_list = [df[i] for i in range(1,18)]

        final_df = reduce(lambda df1,df2: pd.merge(df1,df2,on=['department_name', 'year'],how='outer'), df_list)
        final_df['department_name'] = final_df['department_name'].replace(ACCENTS_MAP)

        return final_df     



class FactTableStep(PipelineStep):
    def run_step(self, prev, params):
        logger.info("Creating Fact Table...")

        final_df = prev
        
        regions = pd.DataFrame(final_df['department_name'].unique()).reset_index()
        regions['index'] = regions['index'] + 1
        regions_map = {k:v for (k,v) in zip(final_df['department_name'].unique(), regions['index'])}
        final_df["department_id"] = final_df["department_name"].map(regions_map)
        

        final_df["department_id"] = final_df["department_id"].astype(str)
        final_df["year"] = final_df["year"].astype(int)

        final_df = final_df[['department_id','year','tasa_desempleo', 'poblacion_ocupada', 'tasa_pobreza',
       'ingreso_promedio_mensual', 'contribucion_al_vab', 'variacion_del_vab',
       'porcentaje_de_mype', 'oficinas_financieras', 'monto_de_depositos',
       'creditos_directos', 'mype_accede_a_credito_inicial',
       'mype_accede_a_credito', 'investigadores_cada_10_mil_habitantes',
       'inversion_en_id', 'exportaciones_en_valor_fob',
       'ejecucion_presupuestal', 'ejecucion_presupuestal_del_itp']]
    
        


        return final_df


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


        fact_step = FactTableStep()
        load_fact = LoadStep(
            table_name="itp_fact", 
            connector=db_connector, 
            if_exists="drop", 
            pk=["department_id"],
            dtype={"department_id":"String",
                        "year":"UInt32",
                        "tasa_desempleo":"Float32",
                        "poblacion_ocupada":"Float32",
                        "tasa_pobreza":"Float32",
                        "ingreso_promedio_mensual":"Float32",
                        "contribucion_al_vab":"Float32",
                        "variacion_del_vab":"Float32",
                        "porcentaje_de_mype":"Float32",
                        "oficinas_financieras":"UInt16 ",
                        "monto_de_depositos":"Float32 ",
                        "creditos_directos":"Float32",
                        "mype_accede_a_credito_inicial":"Float32",  
                        "mype_accede_a_credito":"Float32",  
                        "investigadores_cada_10_mil_habitantes":"Float64",  
                        "inversion_en_id":"Float32",  
                        "exportaciones_en_valor_fob":"Float32",
                        "ejecucion_presupuestal":"Float32",  
                        "ejecucion_presupuestal_del_itp":"Float32",      
                    },

            
            nullable_list = [
                    "tasa_desempleo",
                    "poblacion_ocupada",
                    "tasa_pobreza",
                    "ingreso_promedio_mensual",
                    "contribucion_al_vab",
                    "variacion_del_vab",
                    "porcentaje_de_mype",
                    "oficinas_financieras",
                    "monto_de_depositos",
                    "creditos_directos",
                    "mype_accede_a_credito_inicial",  
                    "mype_accede_a_credito",  
                    "investigadores_cada_10_mil_habitantes",  
                    "inversion_en_id",  
                    "exportaciones_en_valor_fob",
                    "ejecucion_presupuestal",  
                    "ejecucion_presupuestal_del_itp",    
                        ],
        )

        if params.get("ingest")==True:
            steps = [open_step, tidy_step, fact_step, load_fact]
        else:
            steps = [open_step, tidy_step, fact_step]

        return steps


if __name__ == "__main__":
    inicio_pipeline = InicioPipeline()
    inicio_pipeline.run(
        {
            "output-db": "clickhouse-local",
            "ingest": False
        }
    )
