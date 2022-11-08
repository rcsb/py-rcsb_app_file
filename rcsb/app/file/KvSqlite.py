import sqlite3
import typing
import logging
from rcsb.app.file.ConfigProvider import ConfigProvider

class Kv(object):
    def __init__(self, filepath, table):
        self.FILEPATH = filepath
        self.TABLE = table
        try:
            with self.getConnection() as connection:
                connection.cursor().execute("CREATE TABLE IF NOT EXISTS sessions (key, val)")
        except Exception as exc:
            raise Exception(f'exception in Kv, {type(exc)} {exc}')

    def getConnection(self):
        connection = sqlite3.connect(self.FILEPATH)
        return connection

    def get(self, key):
        res = None
        try:
            with self.getConnection() as connection:
                res = connection.cursor().execute(f"SELECT val FROM sessions WHERE key = '{key}'").fetchone()[0]
        except Exception as exc:
            pass
            # logging.warning(f'warning in Kv get, {type(exc)} {exc}')
        return res

    def set(self, key, val):
        try:
            with self.getConnection() as connection:
                res = connection.cursor().execute(f"SELECT val FROM sessions WHERE key = '{key}'").fetchone()
                if res is None:
                    res = connection.cursor().execute(f"INSERT INTO sessions VALUES ('{key}', \"{val}\")")
                    connection.commit()
                else:
                    res = connection.cursor().execute(f"UPDATE sessions SET val = \"{val}\" WHERE key = '{key}'")
                    connection.commit()
        except Exception as exc:
            logging.warning(f'possible error in Kv set for {key} = {val}, {type(exc)} {exc}')

    def clear(self, key):
        try:
            with self.getConnection() as connection:
                res = connection.cursor().execute(f"DELETE FROM sessions WHERE key = '{key}'")
                connection.commit()
        except Exception as exc:
            logging.warning(f'possible error in Kv clear, {type(exc)} {exc}')


class KvSqlite(object):
    def __init__(self,  cP: typing.Type[ConfigProvider]):
        self.KV = None
        self.__cP = cP
        # fix mount point
        self.FILEPATH = self.__cP.get('KV_FILE_PATH')
        self.TABLE = self.__cP.get('KV_TABLE_NAME')
        # create database if not exists
        # create table if not exists
        try:
            self.KV = Kv(self.FILEPATH, self.TABLE)
        except Exception as exc:
            # table already exists
            pass
            # logging.warning(f'exception in KvSqlite: {type(exc)} {exc}')
        if self.KV is None:
            raise Exception(f'error in KvSqlite - no database')

    def convert(self, d):
        return str(d)

    def deconvert(self, s):
        return eval(s)

    def get(self, key, val):
        s = self.KV.get(key)
        if s is None:
            self.KV.set(key, self.convert({}))
            s = self.KV.get(key)
        d = self.deconvert(s)
        if val not in d:
            d[val] = 0
            self.KV.set(key, self.convert(d))
            s = self.KV.get(key)
            d = self.deconvert(s)
        try:
            return d[val]
        except:
            raise Exception(f'error in KV get, {d}')

    def set(self, key, val, vval):
        s = self.KV.get(key)
        if s is None:
            self.KV.set(key, self.convert({}))
            s = self.KV.get(key)
        d = self.deconvert(s)
        if val not in d:
            d[val] = 0
            self.KV.set(key, self.convert(d))
            s = self.KV.get(key)
            d = self.deconvert(s)
        d[val] = vval
        self.KV.set(key, self.convert(d))

    def inc(self, key, val):
        s = self.KV.get(key)
        if s is None:
            self.KV.set(key, self.convert({}))
            s = self.KV.get(key)
        d = self.deconvert(s)
        if val not in d:
            d[val] = 0
            self.KV.set(key, self.convert(d))
            s = self.KV.get(key)
            d = self.deconvert(s)
        d[val] += 1
        self.KV.set(key, self.convert(d))

    def clear_val(self, key, val):
        s = self.KV.get(key)
        if s is not None:
            d = self.deconvert(s)
            if val in d:
                del d[val]
                self.KV.set(key, self.convert(d))

    def clear_key(self, key):
        s = self.KV.get(key)
        if s is not None:
            self.KV.clear(key)


