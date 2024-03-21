import sqlite3


class Database:

    @staticmethod
    def get_connection():
        con = sqlite3.connect("D:/Projetos/milho-musics-discord-bot/python-for-data-engineers-grupo-4/src/output/db.sqlite3")
        con.row_factory = sqlite3.Row
        return con

    @staticmethod
    def execute(connection: sqlite3.Connection, query: str, params: list):
        return connection.execute(query, params).fetchall()

    @staticmethod
    def execute_many(connection: sqlite3.Connection, query: str, params: list):
        return connection.executemany(query, params).fetchall()

    @staticmethod
    def verify_if_row_exists(connection: sqlite3.Connection, query: str, params: list):
        row_count = len(connection.execute(query, params).fetchall())

        return False if row_count == 0 else True