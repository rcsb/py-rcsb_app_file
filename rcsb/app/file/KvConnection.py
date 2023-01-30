import sqlite3
import logging


class KvConnection:
    def __init__(self, filepath, sessionTable, logTable):
        self.filePath = filepath
        self.sessionTable = sessionTable
        self.logTable = logTable
        try:
            with self.getConnection() as connection:
                connection.cursor().execute(
                    f"CREATE TABLE IF NOT EXISTS {self.sessionTable} (key, val)"
                )
                connection.cursor().execute(
                    f"CREATE TABLE IF NOT EXISTS {self.logTable} (key,val)"
                )
        except Exception as exc:
            raise Exception(f"exception in KvConnection, {type(exc)} {exc}")

    def getConnection(self):
        connection = sqlite3.connect(self.filePath)
        return connection

    def get(self, key, table):
        res = None
        try:
            with self.getConnection() as connection:
                res = (
                    connection.cursor()
                    .execute(f"SELECT val FROM {table} WHERE key = '{key}'")
                    .fetchone()[0]
                )
        except Exception:
            pass
        return res

    def set(self, key, val, table):
        try:
            with self.getConnection() as connection:
                res = (
                    connection.cursor()
                    .execute(f"SELECT val FROM {table} WHERE key = '{key}'")
                    .fetchone()
                )
                if res is None:
                    res = connection.cursor().execute(
                        f"INSERT INTO {table} VALUES ('{key}', \"{val}\")"
                    )
                    connection.commit()
                else:
                    res = connection.cursor().execute(
                        f"UPDATE {table} SET val = \"{val}\" WHERE key = '{key}'"
                    )
                    connection.commit()
        except Exception as exc:
            logging.warning(
                "possible error in Kv set for table %s, %s = %s, %s %s", table, key, val, type(exc), exc
            )

    def clear(self, key, table):
        try:
            with self.getConnection() as connection:
                connection.cursor().execute(f"DELETE FROM {table} WHERE key = '{key}'")
                connection.commit()
        except Exception as exc:
            logging.warning("possible error in Kv clear table {table}, %s %s", type(exc), exc)

    def deleteRowWithKey(self, key, table):
        try:
            with self.getConnection() as connection:
                connection.cursor().execute(f"DELETE FROM {table} WHERE key = '{key}'")
                connection.commit()
        except Exception as exc:
            logging.warning("error in Kv delete from %s, %s %s", table, type(exc), exc)

    def deleteRowWithVal(self, val, table):
        try:
            with self.getConnection() as connection:
                connection.cursor().execute(f"DELETE FROM {table} WHERE val = '{val}'")
                connection.commit()
        except Exception as exc:
            logging.warning("error in Kv delete from %s, %s %s", table, type(exc), exc)

    def clearTable(self, table):
        try:
            with self.getConnection() as connection:
                connection.cursor().execute(f"DELETE FROM {table}")
                connection.commit()
        except Exception as exc:
            logging.warning("error in Kv delete from %s, %s %s", table, type(exc), exc)
