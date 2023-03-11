import asyncio
import tkinter as tk
from tkinter import ttk
from tkinter.filedialog import askopenfilename, askdirectory
import sys
import os
import io
import gzip
from copy import deepcopy
from PIL import ImageTk, Image
import math
import requests
import json
import time
import rcsb.app.config.setConfig  # noqa: F401 pylint: disable=W0611
from rcsb.utils.io.CryptUtils import CryptUtils
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.client.ClientUtils import ClientUtils
from rcsb.app.file.Definitions import Definitions

"""
author James Smith 2023
"""

# other global variables
contentTypeInfoD = None
fileFormatExtensionD = None
headerD = None
configFilePath = os.environ.get("CONFIG_FILE")
cP = ConfigProvider(configFilePath)
cP.getConfig()
""" modifiable global variables
"""
base_url = cP.get("SERVER_HOST_AND_PORT")
chunkSize = cP.get("CHUNK_SIZE")
hashType = cP.get("HASH_TYPE")
""" do not alter from here
"""
subject = cP.get("JWT_SUBJECT")
headerD = {
    "Authorization": "Bearer "
    + JWTAuthToken(configFilePath).createToken({}, subject)
}
HERE = os.path.abspath(os.path.dirname(__file__))

dF = Definitions()
repoTypeList = dF.getRepoTypeList()
milestoneList = dF.getMilestoneList()
milestoneList.append("none")
fileFormatExtensionD = dF.getFileFormatExtD()
contentTypeInfoD = dF.getContentTypeD()


class Gui(tk.Frame):
    def __init__(self, master):
        super().__init__(master)
        # master.geometry("500x500")
        master.title("FILE ACCESS AND DEPOSITION APPLICATION")

        self.__cU = ClientUtils()

        self.tabs = ttk.Notebook(master)
        self.splashTab = ttk.Frame(master)
        self.uploadTab = ttk.Frame(master)
        self.downloadTab = ttk.Frame(master)
        self.listTab = ttk.Frame(master)
        self.tabs.add(self.splashTab, text="HOME")
        self.tabs.add(self.uploadTab, text="UPLOAD")
        self.tabs.add(self.downloadTab, text="DOWNLOAD")
        self.tabs.add(self.listTab, text="LIST")
        self.tabs.pack(expand=1, fill="both")

        load = Image.open(os.path.join(HERE, "onedep_logo.png"))
        render = ImageTk.PhotoImage(load)
        img = ttk.Label(self.splashTab, image=render)
        img.image = render
        img.place(relx=0.5, rely=0.5, anchor=tk.CENTER)
        img.pack(fill=tk.BOTH, expand=1)

        # UPLOADS

        self.repo_type = tk.StringVar(master)
        self.dep_id = tk.StringVar(master)
        self.content_type = tk.StringVar(master)
        self.mile_stone = tk.StringVar(master)
        self.part_number = tk.StringVar(master)
        self.file_format = tk.StringVar(master)
        self.version_number = tk.StringVar(master)
        self.resumable = tk.IntVar(master)
        self.allow_overwrite = tk.IntVar(master)
        self.compress = tk.IntVar(master)
        self.decompress = tk.IntVar(master)
        self.upload_status = tk.StringVar(master)
        self.upload_status.set("0%")
        self.file_path = None

        self.fileButtonLabel = ttk.Label(self.uploadTab, text="UPLOAD FILE")
        self.fileButtonLabel.pack()
        self.fileButton = ttk.Button(self.uploadTab, text="select", command=self.selectFile)
        self.fileButton.pack()

        self.repoTypeLabel = ttk.Label(self.uploadTab, text="REPOSITORY TYPE")
        self.repoTypeLabel.pack()
        self.repoTypeListbox = ttk.Combobox(self.uploadTab, exportselection=0, textvariable=self.repo_type)
        self.repoTypeListbox.pack()
        self.repoTypeListbox["values"] = repoTypeList
        self.repoTypeListbox.current()

        self.depIdLabel = ttk.Label(self.uploadTab, text="DEPOSIT ID")
        self.depIdLabel.pack()
        self.depIdEntry = ttk.Entry(self.uploadTab, textvariable=self.dep_id)
        self.depIdEntry.pack()

        self.contentTypeLabel = ttk.Label(self.uploadTab, text="CONTENT TYPE")
        self.contentTypeLabel.pack()
        self.contentTypeListbox = ttk.Combobox(self.uploadTab, exportselection=0, textvariable=self.content_type)
        self.contentTypeListbox.pack()
        self.contentTypeListbox["values"] = [key for key in contentTypeInfoD.keys()]

        self.milestoneLabel = ttk.Label(self.uploadTab, text="MILESTONE")
        self.milestoneLabel.pack()
        self.milestoneListbox = ttk.Combobox(self.uploadTab, exportselection=0, textvariable=self.mile_stone)
        self.milestoneListbox.pack()
        self.milestoneListbox["values"] = milestoneList
        self.milestoneListbox.current()

        self.partLabel = ttk.Label(self.uploadTab, text="PART NUMBER")
        self.partLabel.pack()
        self.partNumberEntry = ttk.Entry(self.uploadTab, textvariable=self.part_number)
        self.partNumberEntry.insert(1, "1")
        self.partNumberEntry.pack()

        self.contentFormatLabel = ttk.Label(self.uploadTab, text="CONTENT FORMAT")
        self.contentFormatLabel.pack()
        self.contentFormatListbox = ttk.Combobox(self.uploadTab, exportselection=0, textvariable=self.file_format)
        self.contentFormatListbox.pack()
        self.contentFormatListbox["values"] = [key for key in fileFormatExtensionD.keys()]

        self.versionLabel = ttk.Label(self.uploadTab, text="VERSION")
        self.versionLabel.pack()
        self.versionEntry = ttk.Entry(self.uploadTab, textvariable=self.version_number)
        self.versionEntry.insert(1, "next")
        self.versionEntry.pack()

        self.upload_group = ttk.Frame(self.uploadTab)
        self.resumableButton = ttk.Checkbutton(self.upload_group, text="resumable", variable=self.resumable)
        self.resumableButton.pack(anchor=tk.W)
        self.allowOverwriteButton = ttk.Checkbutton(self.upload_group, text="allow overwrite", variable=self.allow_overwrite)
        self.allowOverwriteButton.pack(anchor=tk.W)
        self.compressCheckbox = ttk.Checkbutton(self.upload_group, text="compress", variable=self.compress)
        self.compressCheckbox.pack(anchor=tk.W)
        self.decompressCheckbox = ttk.Checkbutton(self.upload_group, text="decompress after upload", variable=self.decompress)
        self.decompressCheckbox.pack(anchor=tk.W)
        self.upload_group.pack()

        self.uploadButton = ttk.Button(self.uploadTab, text="submit", command=self.upload)
        self.uploadButton.pack()

        self.statusLabel = ttk.Label(self.uploadTab, textvariable=self.upload_status)
        self.statusLabel.pack()

        self.resetButton = ttk.Button(self.uploadTab, text="reset", command=self.reset)
        self.resetButton.pack()

        # DOWNLOADS

        self.download_repo_type = tk.StringVar(master)
        self.download_dep_id = tk.StringVar(master)
        self.download_content_type = tk.StringVar(master)
        self.download_mile_stone = tk.StringVar(master)
        self.download_part_number = tk.StringVar(master)
        self.download_file_format = tk.StringVar(master)
        self.download_version_number = tk.StringVar(master)
        self.download_allow_overwrite = tk.IntVar(master)
        self.download_status = tk.StringVar(master)
        self.download_status.set("0%")
        self.download_file_path = None

        self.download_fileButtonLabel = ttk.Label(self.downloadTab, text="DESTINATION FOLDER")
        self.download_fileButtonLabel.pack()
        self.download_fileButton = ttk.Button(self.downloadTab, text="select", command=self.selectFolder)
        self.download_fileButton.pack()

        self.download_allowOverwrite = ttk.Checkbutton(self.downloadTab, text="allow overwrite", variable=self.download_allow_overwrite)
        self.download_allowOverwrite.pack()

        self.download_repoTypeLabel = ttk.Label(self.downloadTab, text="REPOSITORY TYPE")
        self.download_repoTypeLabel.pack()
        self.download_repoTypeListbox = ttk.Combobox(self.downloadTab, exportselection=0, textvariable=self.download_repo_type)
        self.download_repoTypeListbox.pack()
        self.download_repoTypeListbox["values"] = repoTypeList
        self.download_repoTypeListbox.current()

        self.download_depIdLabel = ttk.Label(self.downloadTab, text="DEPOSIT ID")
        self.download_depIdLabel.pack()
        self.download_depIdEntry = ttk.Entry(self.downloadTab, textvariable=self.download_dep_id)
        self.download_depIdEntry.pack()

        self.download_contentTypeLabel = ttk.Label(self.downloadTab, text="CONTENT TYPE")
        self.download_contentTypeLabel.pack()
        self.download_contentTypeListbox = ttk.Combobox(self.downloadTab, exportselection=0, textvariable=self.download_content_type)
        self.download_contentTypeListbox.pack()
        self.download_contentTypeListbox["values"] = [key for key in contentTypeInfoD.keys()]

        self.download_milestoneLabel = ttk.Label(self.downloadTab, text="MILESTONE")
        self.download_milestoneLabel.pack()
        self.download_milestoneListbox = ttk.Combobox(self.downloadTab, exportselection=0, textvariable=self.download_mile_stone)
        self.download_milestoneListbox.pack()
        self.download_milestoneListbox["values"] = milestoneList
        self.download_milestoneListbox.current()

        self.download_partLabel = ttk.Label(self.downloadTab, text="PART NUMBER")
        self.download_partLabel.pack()
        self.download_partNumberEntry = ttk.Entry(self.downloadTab, textvariable=self.download_part_number)
        self.download_partNumberEntry.insert(1, "1")
        self.download_partNumberEntry.pack()

        self.download_contentFormatLabel = ttk.Label(self.downloadTab, text="CONTENT FORMAT")
        self.download_contentFormatLabel.pack()
        self.download_contentFormatListbox = ttk.Combobox(self.downloadTab, exportselection=0, textvariable=self.download_file_format)
        self.download_contentFormatListbox.pack()
        self.download_contentFormatListbox["values"] = [key for key in fileFormatExtensionD.keys()]

        self.download_versionLabel = ttk.Label(self.downloadTab, text="VERSION")
        self.download_versionLabel.pack()
        self.download_versionEntry = ttk.Entry(self.downloadTab, textvariable=self.download_version_number)
        self.download_versionEntry.insert(1, "1")
        self.download_versionEntry.pack()

        self.downloadButton = ttk.Button(self.downloadTab, text="submit", command=self.download)
        self.downloadButton.pack()

        self.download_statusLabel = ttk.Label(self.downloadTab, textvariable=self.download_status)
        self.download_statusLabel.pack()

        self.download_resetButton = ttk.Button(self.downloadTab, text="reset", command=self.reset)
        self.download_resetButton.pack()

        # LIST DIRECTORY

        self.list_repo_type = tk.StringVar(master)
        self.list_dep_id = tk.StringVar(master)

        self.list_repoTypeLabel = ttk.Label(self.listTab, text="REPOSITORY TYPE")
        self.list_repoTypeLabel.pack()
        self.list_repoTypeListbox = ttk.Combobox(self.listTab, exportselection=0, textvariable=self.list_repo_type)
        self.list_repoTypeListbox.pack()
        self.list_repoTypeListbox["values"] = repoTypeList
        self.list_repoTypeListbox.current()

        self.list_depIdLabel = ttk.Label(self.listTab, text="DEPOSIT ID")
        self.list_depIdLabel.pack()
        self.list_depIdEntry = ttk.Entry(self.listTab, textvariable=self.list_dep_id)
        self.list_depIdEntry.pack()

        self.listButton = ttk.Button(self.listTab, text="submit", command=self.listDir)
        self.listButton.pack()

        self.list_Listbox = tk.Listbox(self.listTab, exportselection=0, width=50)
        self.list_Listbox.pack(pady=50)

        self.list_resetButton = ttk.Button(self.listTab, text="reset", command=self.reset)
        self.list_resetButton.pack()

    def selectFile(self):
        self.file_path = askopenfilename()
        self.fileButton.config(text="\u2713")

    def selectFolder(self):
        self.file_path = askdirectory()
        self.download_fileButton.config(text="\u2713")

    def uploadFile(self):
        global headerD
        global hashType
        global chunkSize
        global base_url
        global contentTypeInfoD
        global fileFormatExtensionD
        global cP
        global iou
        t1 = time.perf_counter()
        readFilePath = self.file_path
        resumable = self.resumable.get() == 1
        allowOverwrite = self.allow_overwrite.get() == 1
        COMPRESS = self.compress.get() == 1
        DECOMPRESS = self.decompress.get() == 1
        repositoryType = self.repo_type.get()
        depId = self.dep_id.get()
        contentType = self.content_type.get()
        milestone = self.mile_stone.get()
        partNumber = self.part_number.get()
        contentFormat = self.file_format.get()
        version = self.version_number.get()
        if not readFilePath or not repositoryType or not depId or not contentType or not partNumber or not contentFormat or not version:
            print("error - missing values")
            sys.exit()
        if not os.path.exists(readFilePath):
            sys.exit(f"error - file does not exist: {readFilePath}")
        if milestone.lower() == "none":
            milestone = ""
        # compress, then hash, then upload
        if COMPRESS:
            tempPath = readFilePath + ".gz"
            with open(readFilePath, "rb") as r:
                with gzip.open(tempPath, "wb") as w:
                    w.write(r.read())
            readFilePath = tempPath
        # hash
        hD = CryptUtils().getFileHash(readFilePath, hashType=hashType)
        fullTestHash = hD["hashDigest"]
        fileSize = os.path.getsize(readFilePath)
        chunkIndex = 0
        expectedChunks = 0
        if chunkSize < fileSize:
            expectedChunks = fileSize // chunkSize
            if fileSize % chunkSize:
                expectedChunks = expectedChunks + 1
        else:
            expectedChunks = 1
        # upload chunks sequentially
        saveFilePath = None
        uploadId = None
        parameters = {"repositoryType": repositoryType,
                      "depId": depId,
                      "contentType": contentType,
                      "milestone": milestone,
                      "partNumber": partNumber,
                      "contentFormat": contentFormat,
                      "version": version,
                      "hashDigest": fullTestHash,
                      "allowOverwrite": allowOverwrite,
                      "resumable": resumable
                      }
        if FORWARDING:
            saveFilePath, chunkIndex, uploadId = asyncio.run(iou.getUploadParameters(**parameters))
        else:
            url = os.path.join(base_url, "file-v2", "getUploadParameters")
            response = requests.get(
                url,
                params=parameters,
                headers=headerD,
                timeout=None
            )
            print(f"status code {response.status_code}")
            if response.status_code == 200:
                result = json.loads(response.text)
                if result:
                    # result = eval(result)
                    saveFilePath = result["filePath"]
                    chunkIndex = result["chunkIndex"]
                    uploadId = result["uploadId"]
                # print(f"result = {result}")
        if not saveFilePath or not uploadId:
            print("error - no file path or upload id were formed")
            sys.exit()
        mD = {
            # chunk parameters
            "chunkSize": chunkSize,
            "chunkIndex": chunkIndex,
            "expectedChunks": expectedChunks,
            # upload file parameters
            "uploadId": uploadId,
            "hashType": hashType,
            "hashDigest": fullTestHash,
            "resumable": resumable,
            # save file parameters
            "filePath": saveFilePath,
            "resumable": resumable,
            "allowOverwrite": allowOverwrite
        }
        # chunk file and upload
        offset = chunkIndex * chunkSize
        responses = []
        tmp = io.BytesIO()
        with open(readFilePath, "rb") as to_upload:
            to_upload.seek(offset)
            url = os.path.join(base_url, "file-v2", "upload")
            for x in range(chunkIndex, mD["expectedChunks"]):
                packet_size = min(
                    int(fileSize) - (int(mD["chunkIndex"]) * int(chunkSize)),
                    int(chunkSize),
                )
                tmp.truncate(packet_size)
                tmp.seek(0)
                tmp.write(to_upload.read(packet_size))
                tmp.seek(0)
                if FORWARDING:
                    mD["chunk"] = tmp
                    response = asyncio.run(iou.upload(**(deepcopy(mD))))
                else:
                    response = requests.post(
                        url,
                        data=deepcopy(mD),
                        headers=headerD,
                        files={"chunk": tmp},
                        stream=True,
                        timeout=None,
                    )
                    if response.status_code != 200:
                        print(
                            f"error - status code {response.status_code} {response.text}...terminating"
                        )
                        break
                responses.append(response)
                mD["chunkIndex"] += 1
                self.status = math.ceil((mD["chunkIndex"] / mD["expectedChunks"]) * 100)
                self.upload_status.set(f"{self.status}%")
                self.master.update()
        print(responses)
        print(f"time {time.perf_counter() - t1} s")
        return

    def upload(self):
        global headerD
        global hashType
        global chunkSize
        global base_url
        global contentTypeInfoD
        global fileFormatExtensionD
        global cP
        global iou
        t1 = time.perf_counter()
        readFilePath = self.file_path
        resumable = self.resumable.get() == 1
        allowOverwrite = self.allow_overwrite.get() == 1
        COMPRESS = self.compress.get() == 1
        DECOMPRESS = self.decompress.get() == 1
        repositoryType = self.repo_type.get()
        depId = self.dep_id.get()
        contentType = self.content_type.get()
        milestone = self.mile_stone.get()
        partNumber = self.part_number.get()
        contentFormat = self.file_format.get()
        version = self.version_number.get()
        if not readFilePath or not repositoryType or not depId or not contentType or not partNumber or not contentFormat or not version:
            print("error - missing values")
            sys.exit()
        if not os.path.exists(readFilePath):
            sys.exit(f"error - file does not exist: {readFilePath}")
        if milestone.lower() == "none":
            milestone = ""
        # compress, then hash, then upload
        if COMPRESS:
            tempPath = readFilePath + ".gz"
            with open(readFilePath, "rb") as r:
                with gzip.open(tempPath, "wb") as w:
                    w.write(r.read())
            readFilePath = tempPath
        # get upload parameters
        response = self.__cU.getUploadParameters(readFilePath, repositoryType, depId, contentType, milestone, partNumber, contentFormat, version, allowOverwrite, resumable)
        if not response:
            print("error in get upload parameters")
            return
        saveFilePath, chunkIndex, expectedChunks, uploadId, fullTestHash = response
        # upload chunks
        for index in range(chunkIndex, expectedChunks):
            response = self.__cU.uploadChunk(readFilePath, saveFilePath, index, expectedChunks, uploadId, fullTestHash, DECOMPRESS, allowOverwrite, resumable)
            if not response:
                print("error in upload chunk")
                break
            self.status = math.ceil((index / expectedChunks) * 100)
            self.upload_status.set(f"{self.status}%")
            self.master.update()
        self.status = 100
        self.upload_status.set(f"{self.status}%")
        self.master.update()
        print(response)
        print(f"time {time.perf_counter() - t1} s")
        return

    def download(self):
        global headerD
        global chunkSize
        global hashType
        global base_url
        global contentTypeInfoD
        global fileFormatExtensionD
        t1 = time.perf_counter()
        allowOverwrite = self.download_allow_overwrite.get() == 1
        repositoryType = self.download_repo_type.get()
        depId = self.download_dep_id.get()
        contentType = self.download_content_type.get()
        milestone = self.download_mile_stone.get()
        convertedMilestone = None
        if milestone and milestone.lower() != "none":
            convertedMilestone = f"-{milestone}"
        else:
            convertedMilestone = ""
        partNumber = self.download_part_number.get()
        contentFormat = self.download_file_format.get()
        version = self.download_version_number.get()
        folderPath = self.file_path
        if not folderPath or not repositoryType or not depId or not contentType or not partNumber or not contentFormat or not version:
            print("error - missing values")
            sys.exit()
        convertedContentFormat = fileFormatExtensionD[contentFormat]
        fileName = f"{depId}_{contentType}{convertedMilestone}_P{partNumber}.{convertedContentFormat}.V{version}"
        downloadFilePath = os.path.join(folderPath, fileName)
        if not os.path.exists(folderPath):
            print(f"error - folder does not exist: {downloadFilePath}")
            sys.exit()
        if os.path.exists(downloadFilePath):
            if not allowOverwrite:
                print(f"error - file already exists: {downloadFilePath}")
                sys.exit()
            os.remove(downloadFilePath)
        if milestone.lower() == "none":
            milestone = ""
        downloadDict = {
            "depId": depId,
            "repositoryType": repositoryType,
            "contentType": contentType,
            "contentFormat": contentFormat,
            "partNumber": partNumber,
            "version": str(version),
            "hashType": hashType,
            "milestone": milestone
        }
        url = os.path.join(base_url, "file-v1", "downloadSize")
        fileSize = requests.get(url, params=downloadDict, headers=headerD, timeout=None).text
        if not fileSize or not fileSize.isnumeric():
            print(f"error - no response for {downloadFilePath}")
            return None
        fileSize = int(fileSize)
        chunks = math.ceil(fileSize / chunkSize)
        url = os.path.join(base_url, "file-v1", "download")
        url = f"{url}?repositoryType={repositoryType}&depId={depId}&contentType={contentType}&milestone={milestone}&partNumber={partNumber}&contentFormat={contentFormat}&version={version}&hashType={hashType}"
        responseCode = None
        count = 0
        with requests.get(url, headers=headerD, timeout=None, stream=True) as response:
            with open(downloadFilePath, "ab") as ofh:
                for chunk in response.iter_content(chunk_size=chunkSize):
                    if chunk:
                        ofh.write(chunk)
                    count += 1
                    self.status = math.ceil((count / chunks) * 100)
                    self.download_status.set(f"{self.status}%")
                    self.master.update()
            responseCode = response.status_code
            rspHashType = response.headers["rcsb_hash_type"]
            rspHashDigest = response.headers["rcsb_hexdigest"]
            thD = CryptUtils().getFileHash(downloadFilePath, hashType=rspHashType)
            if not thD["hashDigest"] == rspHashDigest:
                print("error - hash comparison failed")
                sys.exit()
        print(f"response {responseCode}")
        print(f"time {time.perf_counter() - t1} s")

    def listDir(self):
        t1 = time.perf_counter()
        self.list_Listbox.delete(0, tk.END)
        depId = self.list_dep_id.get()
        repoType = self.list_repo_type.get()
        parameters = {
            "repositoryType": repoType,
            "depId": depId
        }
        if not depId or not repoType:
            print("error - missing values")
            sys.exit()
        url = os.path.join(base_url, "file-v1", "list-dir")
        responseCode = None
        dirList = None
        with requests.get(url, params=parameters, headers=headerD, timeout=None) as response:
            responseCode = response.status_code
            if responseCode == 200:
                resp = response.text
                if resp:
                    if not isinstance(resp, dict):
                        resp = json.loads(resp)
                    dirList = resp["dirList"]
        print(f"response {responseCode}")
        index = 1
        if responseCode == 200:
            for fi in sorted(dirList):
                print(f"\t{fi}")
                self.list_Listbox.insert(index, fi)
                index += 1
        print(f"time {time.perf_counter() - t1} s")

    def reset(self):
        self.fileButton.config(text="select")
        self.download_fileButton.config(text="select")
        self.list_Listbox.delete(0, tk.END)
        self.upload_status.set(f"0%")
        self.download_status.set(f"0%")

        self.repo_type.set("")
        self.dep_id.set("")
        self.content_type.set("")
        self.mile_stone.set("none")
        self.part_number.set("1")
        self.file_format.set("")
        self.version_number.set("next")
        self.allow_overwrite.set(0)
        self.compress.set(0)
        self.decompress.set(0)
        self.upload_radio.set(1)

        self.download_repo_type.set("")
        self.download_dep_id.set("")
        self.download_content_type.set("")
        self.download_mile_stone.set("")
        self.download_part_number.set("1")
        self.download_file_format.set("")
        self.download_version_number.set("1")
        self.download_allow_overwrite.set(0)

        self.list_repo_type.set("")
        self.list_dep_id.set("")

        self.master.update()

if __name__=="__main__":
    root = tk.Tk()
    gui = Gui(root)
    gui.mainloop()
