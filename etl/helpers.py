from bamboo_lib.connectors.models import Connector
from bamboo_lib.helpers import query_to_df
from bamboo_lib.logger import logger


def format_text(df, cols_names=None, stopwords=None):
    for ele in cols_names:
        df[ele] = df[ele].str.title().str.strip()
        for ene in stopwords:
            df[ele] = df[ele].str.replace(" " + ene.title() + " ", " " + ene + " ")

    return df


def clean_tables(table_name: str, connector_path: str):
    db_connector = Connector.fetch("clickhouse-database", open(connector_path))

    try:
        query_to_df(db_connector, raw_query=f"DROP TABLE {table_name}")
        logger.info(f"Table {table_name} dropped!")
    except:
        pass
