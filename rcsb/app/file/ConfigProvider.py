##
# File:    ConfigProvider.py
# Author:  jdw
# Date:    16-Aug-2021
# Version: 0.001
#
# Updates: James Smith 2023
##
"""
Accessors for configuration details.
"""

__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "john.westbrook@rcsb.org"
__license__ = "Apache 2.0"

import datetime
import logging
import typing
import re
import validators
from rcsb.app.config.setConfig import getConfig
from rcsb.utils.config.ConfigUtil import ConfigUtil

logger = logging.getLogger(__name__)


class ConfigProvider(object):
    """Accessors for configuration details."""

    def __init__(self, configFilePath: typing.Optional[str] = None):
        # ---
        self.__configFilePath = configFilePath if configFilePath else getConfig()
        self.__configD = None
        # ---

    def get(self, ky: str) -> typing.Optional[str]:
        try:
            if not self.__configD:
                if self.__configFilePath:
                    self.__readConFigFromConFigYmlFile()
            return self.__configD["data"][ky]
        except Exception:
            pass
        return None

    def getConfig(self) -> typing.Dict:
        try:
            if not self.__configD:
                if self.__configFilePath:
                    self.__readConFigFromConFigYmlFile()
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return self.__configD["data"]

    def getConfigFilePath(self) -> str:
        return self.__configFilePath

    def getVersion(self) -> typing.Optional[str]:
        try:
            return self.__configD["version"]
        except Exception:
            pass
        return None

    def __readConFigFromConFigYmlFile(self) -> bool:
        """Read Yml configuration file and set internal configuration dictionary"""
        ok = False
        try:
            cfgOb = ConfigUtil(
                configPath=self.__configFilePath,
                defaultSectionName="configuration",
                mockTopPath=None,
            )
            self.__configD = {
                "version": 0.30,
                "created": datetime.datetime.now().isoformat(),
                "data": cfgOb.exportConfig(sectionName="configuration"),
            }
            ok = True
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            ok = False
        return ok

    def set(self, key: str, val: typing.Union[str, int, float, bool]):
        """
        facilitate validation tests
        should only be used by validate
        """
        if not self.__configD:
            if self.__configFilePath:
                self.__readConFigFromConFigYmlFile()
        self.__configD["data"][key] = val

    def validate(self) -> bool:
        """
        exit at server start if settings do not validate
        """
        # validate host and port
        host = self.get("SERVER_HOST_AND_PORT")
        if not host or not validators.url(str(host)):
            return False
        # validate surplus processors
        surplus = self.get("SURPLUS_PROCESSORS")
        if (
            surplus is None
            or surplus == ""
            or re.match(r"\d+", str(surplus)) is None
            or int(surplus) < 0
        ):
            return False
        # validate path strings
        paths = [
            self.get("REPOSITORY_DIR_PATH"),
            self.get("SESSION_DIR_PATH"),
            self.get("SHARED_LOCK_PATH"),
            self.get("KV_FILE_PATH"),
        ]
        if not all(paths) or not all(
            [re.match(r"\.{0,2}(/?\w+)+/?", str(path)) for path in paths]
        ):
            return False
        # validate lock transactions
        lock = self.get("LOCK_TRANSACTIONS")
        if not lock or not isinstance(lock, bool):
            return False
        # validate lock type
        lock_types = ["soft", "ternary", "redis"]
        lock_type = self.get("LOCK_TYPE")
        if not lock_type or str(lock_type) not in lock_types:
            return False
        # validate lock timeout
        lock_timeout = self.get("LOCK_TIMEOUT")
        if not lock_timeout or not re.match(r"\d+", str(lock_timeout)):
            return False
        # validate kv mode
        kv_modes = ["sqlite", "redis"]
        kv_mode = self.get("KV_MODE")
        if not kv_mode or str(kv_mode) not in kv_modes:
            return False
        # require kv mode redis for lock type redis and vice versa
        kv_mode_redis = self.get("KV_MODE") == "redis"
        lock_type_redis = self.get("LOCK_TYPE") == "redis"
        if any([kv_mode_redis, lock_type_redis]) and not all(
            [kv_mode_redis, lock_type_redis]
        ):
            return False
        # validate redis host
        redis_hosts = ["localhost", "redis"]
        redis_host = self.get("REDIS_HOST")
        if not redis_host or str(redis_host) not in redis_hosts:
            if not validators.url(str(redis_host)):
                return False
        # validate table names
        table_names = [
            self.get("KV_MAP_TABLE_NAME"),
            self.get("KV_SESSION_TABLE_NAME"),
            self.get("KV_LOCK_TABLE_NAME"),
        ]
        if not all(table_names) or not all(
            [re.match(r"\w+", str(name)) for name in table_names]
        ):
            return False
        # validate max seconds
        max_seconds = self.get("KV_MAX_SECONDS")
        if (
            not max_seconds
            or not re.match(r"\d+", str(max_seconds))
            or int(max_seconds) < 0
        ):
            return False
        # validate chunk size
        chunk_size = self.get("CHUNK_SIZE")
        if (
            not chunk_size
            or not re.match(r"\d+", str(chunk_size))
            or int(chunk_size) < 0
        ):
            return False
        # validate compression type
        compressions = ["gzip", "zip", "bzip2", "lzma"]
        compression = self.get("COMPRESSION_TYPE")
        if not compression or str(compression) not in compressions:
            return False
        # validate hash type
        hash_types = ["MD5", "SHA1", "SHA256"]
        hash_type = self.get("HASH_TYPE")
        if not hash_type or str(hash_type) not in hash_types:
            return False
        # reading dictionary returns integer value of octal so have no way to verify octal string
        permissions = self.__configD["data"]["DEFAULT_FILE_PERMISSIONS"]
        if not permissions or not re.match(r"\d+", str(permissions)):
            return False
        # validate jwt strings
        jwts = [self.get("JWT_SUBJECT"), self.get("JWT_SECRET")]
        if not all(jwts) or not all([re.match(r"\w+", str(jwt)) for jwt in jwts]):
            return False
        # validate jwt algorithm
        algorithms = [
            "HS256",
            "HS384",
            "HS512",
            "ES256",
            "ES256K",
            "ES384",
            "ES512",
            "RS256",
            "RS384",
            "RS512",
            "PS256",
            "PS384",
            "PS512",
            "EdDSA",
        ]
        algorithm = self.get("JWT_ALGORITHM")
        if not algorithm or str(algorithm) not in algorithms:
            return False
        # validate jwt timeout
        duration = self.get("JWT_DURATION")
        if not duration or not re.match(r"\d+", str(duration)) or int(duration) < 0:
            return False
        # validate bypass authorization
        bypass_authorization = self.get("BYPASS_AUTHORIZATION")
        if not bypass_authorization or not isinstance(bypass_authorization, bool):
            return False
        return True
