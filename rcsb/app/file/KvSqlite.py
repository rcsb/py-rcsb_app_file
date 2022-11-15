import sqlite3
import typing
import logging
from rcsb.app.file.ConfigProvider import ConfigProvider


class Kv:
    def __init__(self, filepath, tL):
        self.filePath = filepath
        self.table = tL
        try:
            with self.getConnection() as connection:
                connection.cursor().execute(
                    "CREATE TABLE IF NOT EXISTS sessions (key, val)"
                )
        except Exception as exc:
            raise Exception(f"exception in Kv, {type(exc)} {exc}")

    def getConnection(self):
        connection = sqlite3.connect(self.filePath)
        return connection

    def get(self, key):
        res = None
        try:
            with self.getConnection() as connection:
                res = (
                    connection.cursor()
                    .execute(f"SELECT val FROM sessions WHERE key = '{key}'")
                    .fetchone()[0]
                )
        except Exception:
            pass
            # logging.warning(f'warning in Kv get, {type(exc)} {exc}')
        return res

    def set(self, key, val):
        try:
            with self.getConnection() as connection:
                res = (
                    connection.cursor()
                    .execute(f"SELECT val FROM sessions WHERE key = '{key}'")
                    .fetchone()
                )
                if res is None:
                    res = connection.cursor().execute(
                        f"INSERT INTO sessions VALUES ('{key}', \"{val}\")"
                    )
                    connection.commit()
                else:
                    res = connection.cursor().execute(
                        f"UPDATE sessions SET val = \"{val}\" WHERE key = '{key}'"
                    )
                    connection.commit()
        except Exception as exc:
            logging.warning(
                "possible error in Kv set for %s = %s, %s %s", key, val, type(exc), exc
            )

    def clear(self, key):
        try:
            with self.getConnection() as connection:
                connection.cursor().execute(f"DELETE FROM sessions WHERE key = '{key}'")
                connection.commit()
        except Exception as exc:
            logging.warning("possible error in Kv clear, %s %s", type(exc), exc)


class KvSqlite:
    def __init__(self, cP: typing.Type[ConfigProvider]):
        self.kV = None
        self.__cP = cP
        # fix mount point
        self.filePath = self.__cP.get("KV_FILE_PATH")
        self.table = self.__cP.get("KV_TABLE_NAME")
        # create database if not exists
        # create table if not exists
        try:
            self.kV = Kv(self.filePath, self.table)
        except Exception:
            # table already exists
            pass
            # logging.warning(f'exception in KvSqlite: {type(exc)} {exc}')
        if self.kV is None:
            raise Exception("error in KvSqlite - no database")

    def convert(self, _d):
        return str(_d)

    def deconvert(self, _s):
        return eval(_s)

    def get(self, key, val):
        _s = self.kV.get(key)
        if _s is None:
            self.kV.set(key, self.convert({}))
            _s = self.kV.get(key)
        _d = self.deconvert(_s)
        if val not in _d:
            _d[val] = 0
            self.kV.set(key, self.convert(_d))
            _s = self.kV.get(key)
            _d = self.deconvert(_s)
        try:
            return _d[val]
        except Exception:
            raise Exception(f"error in KV get, {_d}")

    def set(self, key, val, vval):
        _s = self.kV.get(key)
        if _s is None:
            self.kV.set(key, self.convert({}))
            _s = self.kV.get(key)
        _d = self.deconvert(_s)
        if val not in _d:
            _d[val] = 0
            self.kV.set(key, self.convert(_d))
            _s = self.kV.get(key)
            _d = self.deconvert(_s)
        _d[val] = vval
        self.kV.set(key, self.convert(_d))

    def inc(self, key, val):
        _s = self.kV.get(key)
        if _s is None:
            self.kV.set(key, self.convert({}))
            _s = self.kV.get(key)
        _d = self.deconvert(_s)
        if val not in _d:
            _d[val] = 0
            self.kV.set(key, self.convert(_d))
            _s = self.kV.get(key)
            _d = self.deconvert(_s)
        _d[val] += 1
        self.kV.set(key, self.convert(_d))

    def clearVal(self, key, val):
        _s = self.kV.get(key)
        if _s is not None:
            _d = self.deconvert(_s)
            if val in _d:
                del _d[val]
                self.kV.set(key, self.convert(_d))

    def clearKey(self, key):
        _s = self.kV.get(key)
        if _s is not None:
            self.kV.clear(key)
