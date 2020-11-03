from bamboo_lib.models import PipelineStep

class ReplaceStep(PipelineStep):
    def run_step(self, prev, params):

        df = prev

        # replace
        indicator_dim = dict(zip(df['indicator_id'].unique(), range(1, len(df['indicator_id'].unique()) + 1 )))
        df['indicator_id'].replace(indicator_dim, inplace=True)

        category_dim = dict(zip(df['category_id'].unique(), range(1, len(df['category_id'].unique()) + 1 )))
        df['category_id'].replace(category_dim, inplace=True)

        subcategory_dim = dict(zip(df['subcategory_id'].dropna().unique(), range(1, len(df['subcategory_id'].unique()) + 1 )))
        df['subcategory_id'].replace(subcategory_dim, inplace=True)


        return df, indicator_dim, category_dim, subcategory_dim