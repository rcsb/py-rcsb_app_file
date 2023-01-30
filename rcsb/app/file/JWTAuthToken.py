##
# File: JWTAuthToken.py
# Date: 23-Aug-2021
#
##

import datetime
import logging
import time
from typing import Optional

import jwt

from rcsb.app.file.ConfigProvider import ConfigProvider

logger = logging.getLogger(__name__)


class JWTAuthToken:
    def __init__(self, configFilePath: str):
        cP = ConfigProvider(configFilePath)
        self.__jwtSecret = cP.get("JWT_SECRET")
        self.__jwtAlgorithm = cP.get("JWT_ALGORITHM")
        self.__jwtSubject = cP.get("JWT_SUBJECT")
        self.__jwtDuration = cP.get("JWT_DURATION")
        #

    def decodeToken(self, token: str) -> dict:
        try:
            decodedToken = jwt.decode(token, self.__jwtSecret, algorithms=[self.__jwtAlgorithm])
            logger.debug("Decoded (%r) %r", self.__jwtSubject, decodedToken)
            return decodedToken if (decodedToken["exp"] >= time.time()) and (decodedToken["sub"] == self.__jwtSubject) else None
        except Exception as e:
            logger.exception("Failing as %s", str(e))
            return None

    def createToken(self, data: dict, subject: str, expiresDelta: Optional[datetime.timedelta] = None):
        payload = data.copy()
        now = datetime.datetime.utcnow()
        if expiresDelta:
            expire = now + expiresDelta
        else:
            expire = now + datetime.timedelta(minutes=self.__jwtDuration)
        payload.update({"exp": expire, "iat": now, "sub": subject})
        jwtToken = jwt.encode(payload, self.__jwtSecret, algorithm=self.__jwtAlgorithm)

        return jwtToken
