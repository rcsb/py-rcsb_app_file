##
# File: main.py
# Date: 11-Aug-2020
#
# Template/skeleton web service application
#
##
__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "john.westbrook@rcsb.org"
__license__ = "Apache 2.0"

import logging
import os
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

# pylint: disable=wrong-import-position
# This environment must be set before JWTAuthBearer is imported
HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
os.environ["CONFIG_FILE"] = os.environ.get("CONFIG_FILE", os.path.join("rcsb", "app", "config", "config.yml"))

from . import ConfigProvider
from . import LogFilterUtils
from . import downloadRequest  # This triggers JWTAuthBearer
from . import serverStatus
from . import uploadRequest
from . import pathRequest

logger = logging.getLogger()
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()

# The following mimics the default Gunicorn logging format
formatter = logging.Formatter("%(asctime)s [%(process)d] [%(levelname)s] [%(module)s.%(funcName)s] %(message)s", "[%Y-%m-%d %H:%M:%S %z]")
# The following mimics the default Uvicorn logging format
# formatter = logging.Formatter("%(levelname)s:     %(asctime)s-%(module)s.%(funcName)s: %(message)s")

ch.setFormatter(formatter)
logger.addHandler(ch)
logger.propagate = True
# Apply logging filters -
lu = LogFilterUtils.LogFilterUtils()
lu.addFilters()
# ---

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startupEvent():
    # Note that this will run every time a test is performed via, "with TestClient(app) as...",
    # but in production will only run once at startup
    configFilePath = os.environ.get("CONFIG_FILE")
    logger.debug("Startup - running application startup placeholder method")
    logger.debug("Using configFilePath %r", configFilePath)
    cp = ConfigProvider.ConfigProvider(configFilePath)
    _ = cp.getConfig()


@app.on_event("shutdown")
def shutdownEvent():
    logger.debug("Shutdown - running application shutdown placeholder method")


app.include_router(
    downloadRequest.router,
    prefix="/file-v1",
)

app.include_router(
    pathRequest.router,
    prefix="/file-v1",
)

app.include_router(
    uploadRequest.router,
    prefix="/file-v2",
)

app.include_router(serverStatus.router)
