from bamboo_lib.models import PipelineStep

class ReplaceStep(PipelineStep):
    def run_step(self, prev, params):

        df = prev

        # replace
        indicator_dim = dict(zip(df['indicator_id'].unique(), range(1, len(df['indicator_id'].unique()) + 1 )))
        df['indicator_id'].replace(indicator_dim, inplace=True)

        response_dim = dict(zip(df['response_id'].unique(), range(1, len(df['response_id'].unique()) + 1 )))
        df['response_id'].replace(response_dim, inplace=True)

        
        return df, indicator_dim, response_dim