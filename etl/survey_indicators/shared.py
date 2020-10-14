
from bamboo_lib.models import PipelineStep
from static import INDUSTRY_REPLACE

class ReplaceStep(PipelineStep):
    def run_step(self, prev, params):

        df = prev

        # replace dims
        category_dim = dict(zip(df['categoría'].unique(), range(1, len(df['categoría'].unique()) + 1 )))
        df['categoría'].replace(category_dim, inplace=True)

        indicator_dim = dict(zip(df['indicador'].unique(), range(1, len(df['indicador'].unique()) + 1 )))
        df['indicador'].replace(indicator_dim, inplace=True)

        df['industry_id'].replace(INDUSTRY_REPLACE, inplace=True)

        return df, category_dim, indicator_dim