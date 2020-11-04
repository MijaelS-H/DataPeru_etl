import numpy as np
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_csv('../../../datasets/anexos/ISIC_Rev_4_spanish_structure.txt', encoding='latin-1')
        
        sections = [
            ['A', 1, 3],
            ['B', 5, 9],
            ['C', 10, 33],
            ['D', 35, 35],
            ['E', 36, 39],
            ['F', 41, 43],
            ['G', 45, 47],
            ['H', 49, 53],
            ['I', 55, 56],
            ['J', 58, 63],
            ['K', 64, 66],
            ['L', 68, 68],
            ['M', 69, 75],
            ['N', 77, 82],
            ['O', 84, 84],
            ['P', 85, 85],
            ['Q', 86, 88],
            ['R', 90, 93],
            ['S', 94, 96],
            ['T', 97, 98],
            ['U', 99, 99]
        ]

        sections = pd.DataFrame(sections, columns=['section_id', 'interval_lower', 'interval_upper'])

        
        df['division_id'] = df.apply(lambda x: x['Code'][0:2] if len(x['Code']) >= 3 else np.nan, axis=1)
        df['section_id'] = df.apply(lambda x: float(x['division_id']), axis=1)

        for section in df.section_id.unique():
            for level in range(sections.shape[0]):
                if (section >= sections.interval_lower[level]) & (section <= sections.interval_upper[level]):
                    df.section_id.replace(section, str(sections.section_id[level]), inplace=True)
                    break

        sections_df = df[df['Code'].str.len() == 1][['Code', 'Title']].copy()
        df = df[df['Code'].str.len() == 4]

        sections_df.rename(columns={
            'Code': 'section_id',
            'Title': 'section_name'
        }, inplace=True)


        df.rename(columns={
            'Code': 'class_id',
            'Title': 'class_name'
        }, inplace=True)

        
        df = pd.merge(df, sections_df, on='section_id', how='left')

        df = df[['section_name', 'section_id']]

        additional_df = pd.DataFrame(data=[['No determinado', 'Z' ]], columns=df.columns)

        df = df.append(additional_df)
        df.drop_duplicates(subset=['section_id'], inplace = True)

    
        return df

class CIIUPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'section_id': 'String',
            'section_name': 'String',
        }

        transform_step = TransformStep()

        load_step = LoadStep(
            "dim_shared_ciiu_section", db_connector, if_exists="drop", pk=["section_id"], dtype=dtype)

        return [transform_step, load_step]

if __name__ == "__main__":

    pp = CIIUPipeline()
    pp.run({})