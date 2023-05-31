# file - PathRequest.py
# author - James Smith 2023

import logging
import os
from fastapi import APIRouter, Query, HTTPException, Depends
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.JWTAuthBearer import JWTAuthBearer
from rcsb.app.file.PathProvider import PathProvider

provider = ConfigProvider()
jwtDisable = bool(provider.get("JWT_DISABLE"))
if not jwtDisable:
    router = APIRouter(dependencies=[Depends(JWTAuthBearer())], tags=["path"])
else:
    router = APIRouter(tags=["path"])

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


@router.get("/file-path")
async def getFilePath(
    repositoryType: str = Query(...),
    depId: str = Query(...),
    contentType: str = Query(...),
    milestone: str = Query(default=""),
    partNumber: int = Query(default=1),
    contentFormat: str = Query(...),
    version: str = Query(default="next"),
):
    # join parameters into a valid 1-dep file path
    # return relative file path on server, assuming file exists
    # does not test file existence
    path = None
    try:
        path = PathProvider().getVersionedPath(
            repositoryType,
            depId,
            contentType,
            milestone,
            partNumber,
            contentFormat,
            version,
        )
    except HTTPException as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"filePath": path}


@router.get("/file-exists", status_code=200)
async def fileExists(
    repositoryType: str = Query(...),
    depId: str = Query(...),
    contentType: str = Query(...),
    milestone: str = Query(default=""),
    partNumber: int = Query(default=1),
    contentFormat: str = Query(...),
    version: str = Query(default="latest"),
):
    # existence of file based on 1-dep parameters
    # return status code 200 or status code 404
    if not PathProvider().fileExists(
        repositoryType,
        depId,
        contentType,
        milestone,
        partNumber,
        contentFormat,
        version,
    ):
        raise HTTPException(status_code=404, detail="file path not found")


@router.get("/dir-exists", status_code=200)
async def dirExists(repositoryType: str = Query(...), depId: str = Query(...)):
    # existence of directory from 1-dep parameters
    # return status code 200 or status code 404
    if not PathProvider().dirExists(repositoryType, depId):
        raise HTTPException(
            status_code=404, detail="Request directory path does not exist"
        )
    return {"result": True}


@router.get("/path-exists")
async def pathExists(path: str = Query(...)):
    # existence of absolute path on server
    # return status code 200 or status code 404
    if not os.path.exists(path):
        raise HTTPException(
            status_code=404, detail="Request path does not exist %s" % path
        )


@router.get("/list-dir")
async def listDir(repositoryType: str = Query(), depId: str = Query()):
    dirList = await PathProvider().listDir(repositoryType, depId)
    if not dirList:
        raise HTTPException(
            status_code=404, detail=f"Folder not found {repositoryType} {depId}"
        )
    return {"dirList": dirList}


@router.get("/next-version")
async def nextVersion(
    repositoryType: str = Query(...),
    depId: str = Query(...),
    contentType: str = Query(...),
    milestone: str = Query(default=""),
    partNumber: int = Query(default=1),
    contentFormat: str = Query(...),
    version: str = Query(default="latest"),
):
    version = PathProvider().getNextVersion(
        repositoryType,
        depId,
        contentType,
        milestone,
        partNumber,
        contentFormat,
        version,
    )
    if version:
        return {"version": version}
    raise HTTPException(status_code=404, detail="Error - file not found")


@router.get("/latest-version")
async def latestVersion(
    repositoryType: str = Query(...),
    depId: str = Query(...),
    contentType: str = Query(...),
    milestone: str = Query(default=""),
    partNumber: int = Query(default=1),
    contentFormat: str = Query(...),
    version: str = Query(default="latest"),
):
    version = PathProvider().getLatestVersion(
        repositoryType,
        depId,
        contentType,
        milestone,
        partNumber,
        contentFormat,
        version,
    )
    if version:
        return {"version": version}
    raise HTTPException(status_code=404, detail="Error - file not found")
