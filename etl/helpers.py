
import os
import time
from bamboo_lib.connectors.models import Connector
from bamboo_lib.helpers import query_to_df

def format_text(df, cols_names=None, stopwords=None):
    for ele in cols_names:
        df[ele] = df[ele].str.title().str.strip()
        for ene in stopwords:
            df[ele] = df[ele].str.replace(' ' + ene.title() + ' ', ' ' + ene + ' ')

    return df

def clean_tables(table):
    conns_path = os.getcwd().split('/etl/')[0] + '/etl/conns.yaml'
    db_connector = Connector.fetch('clickhouse-database', open(conns_path))
    query = 'DROP TABLE {}'.format(table)
    query_to_df(db_connector, raw_query=query)
    print('Success! {}'.format(table))

    return 0
