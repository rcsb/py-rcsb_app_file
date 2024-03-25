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

    def _set(self, key: str, val: typing.Union[str, int, float, bool]):
        """
        facilitate validation tests
        should only be used by testValidate
        """
        if not self.__configD:
            if self.__configFilePath:
                self.__readConFigFromConFigYmlFile()
        self.__configD["data"][key] = val

    def validate(self) -> bool:
        """
        exit at server start if settings do not validate
        """

        # define our own bools that differ from python
        def non_empty(value) -> bool:
            """do not match nulls, whitespace, or empty string"""
            return value is not None and str(value).strip() != ""

        def falsy(value: int) -> bool:
            """do not match zero or less than zero"""
            return value is None or str(value).strip() == "" or int(value) <= 0

        def nullish(value: int) -> bool:
            """match zero"""
            return value is None or str(value).strip() == "" or int(value) < 0

        settings = [
            "SERVER_HOST_AND_PORT",
            "SURPLUS_PROCESSORS",
            "REPOSITORY_DIR_PATH",
            "SESSION_DIR_PATH",
            "SHARED_LOCK_PATH",
            "LOCK_TRANSACTIONS",
            "LOCK_TYPE",
            "LOCK_TIMEOUT",
            "KV_MODE",
            "REDIS_HOST",
            "KV_SESSION_TABLE_NAME",
            "KV_MAP_TABLE_NAME",
            "KV_LOCK_TABLE_NAME",
            "KV_MAX_SECONDS",
            "KV_FILE_PATH",
            "CHUNK_SIZE",
            "COMPRESSION_TYPE",
            "HASH_TYPE",
            "DEFAULT_FILE_PERMISSIONS",
            "JWT_SUBJECT",
            "JWT_ALGORITHM",
            "JWT_SECRET",
            "JWT_DURATION",
            "BYPASS_AUTHORIZATION",
        ]
        assert_non_falsy = ["KV_MAX_SECONDS", "CHUNK_SIZE", "JWT_DURATION"]
        assert_non_nullish = ["SURPLUS_PROCESSORS", "LOCK_TIMEOUT"]

        if not all([non_empty(self.get(setting)) for setting in settings]):
            return False
        if any([falsy(self.get(setting)) for setting in assert_non_falsy]):
            return False
        if any([nullish(self.get(setting)) for setting in assert_non_nullish]):
            return False

        # validate host and port
        host = self.get("SERVER_HOST_AND_PORT")
        if not validators.url(str(host)):
            return False
        # validate surplus processors
        surplus = self.get("SURPLUS_PROCESSORS")
        if not re.fullmatch(r"\d+", str(surplus)):
            return False
        # validate path strings
        paths = [
            self.get("REPOSITORY_DIR_PATH"),
            self.get("SESSION_DIR_PATH"),
            self.get("SHARED_LOCK_PATH"),
            self.get("KV_FILE_PATH"),
        ]
        if not all(
            [
                re.fullmatch(r"^\.{0,2}(/?[\w\.\- _\~]+)+/?$", str(path))
                for path in paths
            ]
        ):
            return False
        # validate lock transactions
        lock = self.get("LOCK_TRANSACTIONS")
        if not isinstance(lock, bool):
            return False
        # validate lock type
        lock_types = ["soft", "ternary", "redis"]
        lock_type = self.get("LOCK_TYPE")
        if str(lock_type) not in lock_types:
            return False
        # validate lock timeout
        timeout = self.get("LOCK_TIMEOUT")
        if not re.fullmatch(r"\d+", str(timeout)):
            return False
        # validate kv mode
        kv_modes = ["sqlite", "redis"]
        kv_mode = self.get("KV_MODE")
        if str(kv_mode) not in kv_modes:
            return False
        # require kv mode redis for lock type redis but not vice versa
        kv_mode_redis = self.get("KV_MODE") == "redis"
        lock_type_redis = self.get("LOCK_TYPE") == "redis"
        if lock_type_redis and not kv_mode_redis:
            return False
        # validate redis host
        redis_hosts = ["localhost", "redis"]
        redis_host = self.get("REDIS_HOST")
        if str(redis_host) not in redis_hosts:
            if not validators.url(str(redis_host)):
                return False
        # validate table names
        table_names = [
            self.get("KV_MAP_TABLE_NAME"),
            self.get("KV_SESSION_TABLE_NAME"),
            self.get("KV_LOCK_TABLE_NAME"),
        ]
        if not all([re.fullmatch(r"\w+", str(name)) for name in table_names]):
            return False
        # validate max seconds
        max_seconds = self.get("KV_MAX_SECONDS")
        if not re.fullmatch(r"\d+", str(max_seconds)):
            return False
        # validate chunk size
        chunk_size = self.get("CHUNK_SIZE")
        if not re.fullmatch(r"\d+", str(chunk_size)):
            return False
        # validate compression type
        compressions = ["gzip", "zip", "bzip2", "lzma"]
        compression = self.get("COMPRESSION_TYPE")
        if str(compression) not in compressions:
            return False
        # validate hash type
        hash_types = ["MD5", "SHA1", "SHA256"]
        hash_type = self.get("HASH_TYPE")
        if str(hash_type) not in hash_types:
            return False
        # validate default file permissions
        # reading dictionary returns integer value of octal so have no way to verify octal string
        permissions = self.__configD["data"]["DEFAULT_FILE_PERMISSIONS"]
        if not re.fullmatch(r"\w+", str(permissions)):
            return False
        # validate jwt strings
        jwts = [self.get("JWT_SUBJECT"), self.get("JWT_SECRET")]
        if not all([re.fullmatch(r"\w+", str(jwt)) for jwt in jwts]):
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
        if str(algorithm) not in algorithms:
            return False
        # validate jwt timeout
        duration = self.get("JWT_DURATION")
        # will not match zero
        if not re.fullmatch(r"\d+", str(duration)) or int(duration) < 0:
            return False
        # validate bypass authorization
        bypass_authorization = self.get("BYPASS_AUTHORIZATION")
        if not isinstance(bypass_authorization, bool):
            return False
        return True
