
from bamboo_lib.models import PipelineStep
from static import INDUSTRY_REPLACE

class ReplaceStep(PipelineStep):
    def run_step(self, prev, params):

        df = prev

        # replace dims
        df['industry_id'].replace(INDUSTRY_REPLACE, inplace=True)

        # replace
        indicator_dim = dict(zip(df['indicador'].unique(), range(1, len(df['indicador'].unique()) + 1 )))
        df['indicador'].replace(indicator_dim, inplace=True)

        return df, indicator_dim