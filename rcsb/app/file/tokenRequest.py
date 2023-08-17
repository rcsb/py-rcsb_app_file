##
# File: tokenRequest.py
# Date: 17-Aug-2023
##

__docformat__ = "google en"
__author__ = "James Smith"

import logging
from fastapi import APIRouter
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.ConfigProvider import ConfigProvider

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/token", tags=["token"])
def get_token():
    token = JWTAuthToken().createToken({}, ConfigProvider().get("JWT_SUBJECT"))
    logger.info("created token %r", token)
    return {"token": token}


@router.get("/validate-token/{token}", tags=["token"])
def validate_token(token):
    result = JWTAuthToken().decodeToken(token)
    valid = True
    if result is None:
        valid = False
    return {"valid": valid}
