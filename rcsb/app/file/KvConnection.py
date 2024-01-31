# file - KvConnection.py
# author - James Smith 2023

import sqlite3
import logging
from fastapi.exceptions import HTTPException

# sqlite queries


class KvConnection(object):
    def __init__(self, filepath, sessionTable, mapTable, lockTable):
        self.filePath = filepath
        self.sessionTable = sessionTable
        self.mapTable = mapTable
        self.lockTable = lockTable
        try:
            # string interpolation for table names but not for data
            with self.getConnection() as connection:
                connection.cursor().execute(
                    f"CREATE TABLE IF NOT EXISTS {self.sessionTable} (key, val)"
                )
                connection.cursor().execute(
                    f"CREATE TABLE IF NOT EXISTS {self.mapTable} (key,val)"
                )
                connection.cursor().execute(
                    f"CREATE TABLE IF NOT EXISTS {self.lockTable} (key,val)"
                )
        except Exception as exc:
            raise HTTPException(
                status_code=400, detail=f"exception in KvConnection, {type(exc)} {exc}"
            )

    def getConnection(self):
        connection = sqlite3.connect(self.filePath)
        return connection

    def get(self, key, table):
        res = None
        try:
            with self.getConnection() as connection:
                params = (key,)
                res = (
                    connection.cursor()
                    .execute(f"SELECT val FROM {table} " + "WHERE key = ?", params)
                    .fetchone()[0]
                )
                # res = (
                #     connection.cursor()
                #     .execute(f"SELECT val FROM {table} WHERE key = '{key}'")
                #     .fetchone()[0]
                # )
        except Exception:
            pass
        return res

    def getAll(self, table):
        res = None
        try:
            with self.getConnection() as connection:
                res = (
                    connection.cursor()
                    .execute(f"SELECT * FROM {table}")
                ).fetchall()
        except Exception:
            pass
        return res

    def set(self, key, val, table):
        try:
            with self.getConnection() as connection:
                params = (key,)
                res = (
                    connection.cursor()
                    .execute(f"SELECT val FROM {table} " + "WHERE key = ?", params)
                    .fetchone()
                )
                # res = (
                #     connection.cursor()
                #     .execute(f"SELECT val FROM {table} WHERE key = '{key}'")
                #     .fetchone()
                # )
                if res is None:
                    params = (key, val,)
                    res = connection.cursor().execute(
                        f"INSERT INTO {table} " + "VALUES (?, ?)", params
                    )
                    # res = connection.cursor().execute(
                    #     f"INSERT INTO {table} VALUES ('{key}', \"{val}\")"
                    # )
                    connection.commit()
                else:
                    params = (val, key,)
                    res = connection.cursor().execute(
                        f"UPDATE {table} " + "SET val = ? WHERE key = ?", params
                    )
                    # res = connection.cursor().execute(
                    #     f"UPDATE {table} SET val = \"{val}\" WHERE key = '{key}'"
                    # )
                    connection.commit()
        except Exception as exc:
            logging.warning(
                "possible error in Kv set for table %s, %s = %s, %s %s",
                table,
                key,
                val,
                type(exc),
                exc,
            )

    def clear(self, key, table):
        try:
            with self.getConnection() as connection:
                params = (key,)
                connection.cursor().execute(f"DELETE FROM {table} " + "WHERE key = ?", params)
                # connection.cursor().execute(f"DELETE FROM {table} WHERE key = '{key}'")
                connection.commit()
        except Exception as exc:
            logging.warning(
                "possible error in Kv clear table {table}, %s %s", type(exc), exc
            )

    def deleteRowWithKey(self, key, table):
        try:
            with self.getConnection() as connection:
                params = (key,)
                connection.cursor().execute(f"DELETE FROM {table} " + "WHERE key = ?", params)
                # connection.cursor().execute(f"DELETE FROM {table} WHERE key = '{key}'")
                connection.commit()
        except Exception as exc:
            logging.warning("error in Kv delete from %s, %s %s", table, type(exc), exc)

    def deleteRowWithVal(self, val, table):
        try:
            with self.getConnection() as connection:
                params = (val,)
                connection.cursor().execute(f"DELETE FROM {table} " + "WHERE val = ?", params)
                # connection.cursor().execute(f"DELETE FROM {table} WHERE val = '{val}'")
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
