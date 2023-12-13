##
# File: main.py
# Date: 11-Aug-2020
# Updates - James Smith 2023
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
from . import ConfigProvider
from . import downloadRequest
from . import serverStatusRequest
from . import uploadRequest
from . import ioRequest
from . import pathRequest
from . import tokenRequest
from .Sessions import Sessions

provider = ConfigProvider.ConfigProvider()
locktype = provider.get("LOCK_TYPE")
kvmode = provider.get("KV_MODE")
if locktype == "redis":
    if kvmode == "redis":
        from rcsb.app.file.RedisLock import Locking
    else:
        from rcsb.app.file.RedisSqliteLock import Locking
elif locktype == "ternary":
    from rcsb.app.file.TernaryLock import Locking
else:
    from rcsb.app.file.SoftLock import Locking

logger = logging.getLogger()
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()

# The following mimics the default Gunicorn logging format
formatter = logging.Formatter(
    "%(asctime)s [%(process)d] [%(levelname)s] [%(module)s.%(funcName)s] %(message)s",
    "[%Y-%m-%d %H:%M:%S %z]",
)
# The following mimics the default Uvicorn logging format
# formatter = logging.Formatter("%(levelname)s:     %(asctime)s-%(module)s.%(funcName)s: %(message)s")

ch.setFormatter(formatter)
logger.addHandler(ch)
logger.propagate = True

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
    # Runs every time a test is performed via, "with TestClient(app) as...",
    # but in production will only run once at startup
    logger.debug("Startup - running application startup placeholder method")
    cp = ConfigProvider.ConfigProvider()
    repositoryDir = cp.get("REPOSITORY_DIR_PATH")
    sessionDir = cp.get("SESSION_DIR_PATH")
    sharedLockDir = cp.get("SHARED_LOCK_PATH")
    defaultFilePermissions = cp.get("DEFAULT_FILE_PERMISSIONS")
    if not os.path.exists(repositoryDir):
        os.makedirs(repositoryDir, mode=defaultFilePermissions, exist_ok=True)
    if not os.path.exists(sessionDir):
        os.makedirs(sessionDir, mode=defaultFilePermissions, exist_ok=True)
    if not os.path.exists(sharedLockDir):
        os.makedirs(sharedLockDir, mode=defaultFilePermissions, exist_ok=True)


@app.on_event("shutdown")
async def shutdownEvent():
    # Runs every time a test is performed via, "with TestClient(app) as...",
    # but in production will only run once at startup
    logger.debug("Shutdown - running application shutdown placeholder method")
    maxSeconds = (
        0  # set <= 0 to remove all sessions, set to None to keep unexpired sessions
    )
    await Sessions.cleanupSessions(maxSeconds)
    await Locking.cleanup()


app.include_router(
    uploadRequest.router,
)


app.include_router(
    downloadRequest.router,
)


app.include_router(
    ioRequest.router,
)

app.include_router(pathRequest.router)

app.include_router(serverStatusRequest.router)

app.include_router(tokenRequest.router)
