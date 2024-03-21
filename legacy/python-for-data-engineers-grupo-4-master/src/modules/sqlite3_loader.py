import sqlite3
import pandas as pd
import typing as types
from typing import Literal

class Sqlite3Handler:

    @staticmethod
    def get_connection(db_filepath: str):
        return sqlite3.connect(db_filepath)

    @staticmethod
    def load_dataframe_to_db(df: pd.DataFrame, connection: sqlite3.Connection, table_name: str, if_exists: Literal['fail', 'replace', 'append']):
        df.to_sql(name = table_name, con = connection, if_exists = if_exists)
        return df

    @staticmethod
    def read_df_from_sql(connection: sqlite3.Connection, sql: str):
        return pd.read_sql(sql = sql, con = connection)

    @staticmethod
    def create_index(connection: sqlite3.Connection, index_name: str, table_name: str, cols_name: types.List[str]):
        cols = ",".join(cols_name)
        connection.execute(f"CREATE INDEX {index_name} ON {table_name} ({cols});")

    def __init__(self) -> None:
        raise NotImplementedError()