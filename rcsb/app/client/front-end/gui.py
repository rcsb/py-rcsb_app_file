import tkinter as tk
from tkinter import ttk
from tkinter.filedialog import askopenfilename, askdirectory
import sys
import os
from PIL import ImageTk, Image
import math
import time
from rcsb.app.client.ClientUtility import ClientUtility
from rcsb.app.file.Definitions import Definitions
from rcsb.app.file.IoUtility import IoUtility
from rcsb.app.file.UploadUtility import UploadUtility

# author James Smith 2023


class Gui(tk.Frame):
    def __init__(self, master):
        super().__init__(master)
        # master.geometry("500x500")
        master.title("FILE ACCESS AND DEPOSITION APPLICATION")
        self.__cU = ClientUtility()
        self._COMPRESSION = self.__cU.compressionType

        self.tabs = ttk.Notebook(master)
        self.splashTab = ttk.Frame(master)
        self.uploadTab = ttk.Frame(master)
        self.downloadTab = ttk.Frame(master)
        self.listTab = ttk.Frame(master)
        self.tabs.add(self.splashTab, text="START")
        self.tabs.add(self.uploadTab, text="UPLOAD")
        self.tabs.add(self.downloadTab, text="DOWNLOAD")
        self.tabs.add(self.listTab, text="LIST")
        self.tabs.pack(expand=1, fill="both")

        HERE = os.path.abspath(os.path.dirname(__file__))
        load = Image.open(os.path.join(HERE, "resources/onedep_logo.png"))
        render = ImageTk.PhotoImage(load)
        img = ttk.Label(self.splashTab, image=render)
        img.image = render
        img.place(relx=0.5, rely=0.5, anchor=tk.CENTER)
        img.pack(fill=tk.BOTH, expand=1)

        dF = Definitions()
        repoTypeList = dF.getRepoTypeList()
        contentTypeInfoD = dF.getContentTypeD()
        milestoneList = dF.getMilestoneList()
        # milestoneList.append("none")
        fileFormatExtensionD = dF.getFileFormatExtD()

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
        self.compress_chunks = tk.IntVar(master)
        self.no_chunks = tk.IntVar(master)
        self.upload_status = tk.StringVar(master)
        self.upload_status.set("0%")
        self.file_path = None

        self.fileButtonLabel = ttk.Label(self.uploadTab, text="UPLOAD FILE")
        self.fileButtonLabel.pack()
        self.fileButton = ttk.Button(
            self.uploadTab, text="select", command=self.selectFile
        )
        self.fileButton.pack()

        self.repoTypeLabel = ttk.Label(self.uploadTab, text="REPOSITORY TYPE")
        self.repoTypeLabel.pack()
        self.repoTypeListbox = ttk.Combobox(
            self.uploadTab, exportselection=0, textvariable=self.repo_type
        )
        self.repoTypeListbox.pack()
        self.repoTypeListbox["values"] = repoTypeList
        self.repoTypeListbox.current()

        self.depIdLabel = ttk.Label(self.uploadTab, text="DEPOSIT ID")
        self.depIdLabel.pack()
        self.depIdEntry = ttk.Entry(self.uploadTab, textvariable=self.dep_id)
        self.depIdEntry.pack()

        self.contentTypeLabel = ttk.Label(self.uploadTab, text="CONTENT TYPE")
        self.contentTypeLabel.pack()
        self.contentTypeListbox = ttk.Combobox(
            self.uploadTab, exportselection=0, textvariable=self.content_type
        )
        self.contentTypeListbox.pack()
        self.contentTypeListbox["values"] = [key for key in contentTypeInfoD.keys()]

        self.milestoneLabel = ttk.Label(self.uploadTab, text="MILESTONE")
        self.milestoneLabel.pack()
        self.milestoneListbox = ttk.Combobox(
            self.uploadTab, exportselection=0, textvariable=self.mile_stone
        )
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
        self.contentFormatListbox = ttk.Combobox(
            self.uploadTab, exportselection=0, textvariable=self.file_format
        )
        self.contentFormatListbox.pack()
        self.contentFormatListbox["values"] = [
            key for key in fileFormatExtensionD.keys()
        ]

        self.versionLabel = ttk.Label(self.uploadTab, text="VERSION")
        self.versionLabel.pack()
        self.versionEntry = ttk.Entry(self.uploadTab, textvariable=self.version_number)
        self.versionEntry.insert(1, "next")
        self.versionEntry.pack()

        self.upload_group = ttk.Frame(self.uploadTab)
        self.resumableButton = ttk.Checkbutton(
            self.upload_group, text="resumable", variable=self.resumable
        )
        self.resumableButton.pack(anchor=tk.W)
        self.allowOverwriteButton = ttk.Checkbutton(
            self.upload_group, text="allow overwrite", variable=self.allow_overwrite
        )
        self.allowOverwriteButton.pack(anchor=tk.W)
        self.compressCheckbox = ttk.Checkbutton(
            self.upload_group,
            text="compress complete file",
            variable=self.compress,
            command=self.resetChunks,
        )
        self.compressCheckbox.pack(anchor=tk.W)
        self.decompressCheckbox = ttk.Checkbutton(
            self.upload_group,
            text="decompress after upload",
            variable=self.decompress,
            command=self.resetChunks,
        )
        self.decompressCheckbox.pack(anchor=tk.W)
        self.upload_group.pack()
        self.compressChunksCheckbox = ttk.Checkbutton(
            self.upload_group,
            text="compress chunks",
            variable=self.compress_chunks,
            command=self.resetCompress,
        )
        self.compressChunksCheckbox.pack(anchor=tk.W)
        self.noChunksCheckbox = ttk.Checkbutton(
            self.upload_group,
            text="don't chunk file",
            variable=self.no_chunks,
            command=self.resetChunks,
        )
        self.noChunksCheckbox.pack(anchor=tk.W)
        self.upload_group.pack()

        self.uploadButton = ttk.Button(
            self.uploadTab, text="submit", command=self.upload
        )
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
        self.download_chunked_file = tk.IntVar(master)
        self.download_status = tk.StringVar(master)
        self.download_status.set("0%")
        self.download_file_path = None

        self.download_fileButtonLabel = ttk.Label(
            self.downloadTab, text="DESTINATION FOLDER"
        )
        self.download_fileButtonLabel.pack()
        self.download_fileButton = ttk.Button(
            self.downloadTab, text="select", command=self.selectFolder
        )
        self.download_fileButton.pack()

        self.download_allowOverwrite = ttk.Checkbutton(
            self.downloadTab,
            text="allow overwrite",
            variable=self.download_allow_overwrite,
        )
        self.download_allowOverwrite.pack()

        self.download_chunkedFile = ttk.Checkbutton(
            self.downloadTab,
            text="chunk file",
            variable=self.download_chunked_file,
        )
        self.download_chunkedFile.pack()

        self.download_repoTypeLabel = ttk.Label(
            self.downloadTab, text="REPOSITORY TYPE"
        )
        self.download_repoTypeLabel.pack()
        self.download_repoTypeListbox = ttk.Combobox(
            self.downloadTab, exportselection=0, textvariable=self.download_repo_type
        )
        self.download_repoTypeListbox.pack()
        self.download_repoTypeListbox["values"] = repoTypeList
        self.download_repoTypeListbox.current()

        self.download_depIdLabel = ttk.Label(self.downloadTab, text="DEPOSIT ID")
        self.download_depIdLabel.pack()
        self.download_depIdEntry = ttk.Entry(
            self.downloadTab, textvariable=self.download_dep_id
        )
        self.download_depIdEntry.pack()

        self.download_contentTypeLabel = ttk.Label(
            self.downloadTab, text="CONTENT TYPE"
        )
        self.download_contentTypeLabel.pack()
        self.download_contentTypeListbox = ttk.Combobox(
            self.downloadTab, exportselection=0, textvariable=self.download_content_type
        )
        self.download_contentTypeListbox.pack()
        self.download_contentTypeListbox["values"] = [
            key for key in contentTypeInfoD.keys()
        ]

        self.download_milestoneLabel = ttk.Label(self.downloadTab, text="MILESTONE")
        self.download_milestoneLabel.pack()
        self.download_milestoneListbox = ttk.Combobox(
            self.downloadTab, exportselection=0, textvariable=self.download_mile_stone
        )
        self.download_milestoneListbox.pack()
        self.download_milestoneListbox["values"] = milestoneList
        self.download_milestoneListbox.current()

        self.download_partLabel = ttk.Label(self.downloadTab, text="PART NUMBER")
        self.download_partLabel.pack()
        self.download_partNumberEntry = ttk.Entry(
            self.downloadTab, textvariable=self.download_part_number
        )
        self.download_partNumberEntry.insert(1, "1")
        self.download_partNumberEntry.pack()

        self.download_contentFormatLabel = ttk.Label(
            self.downloadTab, text="CONTENT FORMAT"
        )
        self.download_contentFormatLabel.pack()
        self.download_contentFormatListbox = ttk.Combobox(
            self.downloadTab, exportselection=0, textvariable=self.download_file_format
        )
        self.download_contentFormatListbox.pack()
        self.download_contentFormatListbox["values"] = [
            key for key in fileFormatExtensionD.keys()
        ]

        self.download_versionLabel = ttk.Label(self.downloadTab, text="VERSION")
        self.download_versionLabel.pack()
        self.download_versionEntry = ttk.Entry(
            self.downloadTab, textvariable=self.download_version_number
        )
        self.download_versionEntry.insert(1, "1")
        self.download_versionEntry.pack()

        self.downloadButton = ttk.Button(
            self.downloadTab, text="submit", command=self.download
        )
        self.downloadButton.pack()

        self.download_statusLabel = ttk.Label(
            self.downloadTab, textvariable=self.download_status
        )
        self.download_statusLabel.pack()

        self.download_resetButton = ttk.Button(
            self.downloadTab, text="reset", command=self.reset
        )
        self.download_resetButton.pack()

        # LIST DIRECTORY

        self.list_repo_type = tk.StringVar(master)
        self.list_dep_id = tk.StringVar(master)

        self.list_repoTypeLabel = ttk.Label(self.listTab, text="REPOSITORY TYPE")
        self.list_repoTypeLabel.pack()
        self.list_repoTypeListbox = ttk.Combobox(
            self.listTab, exportselection=0, textvariable=self.list_repo_type
        )
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

        self.list_resetButton = ttk.Button(
            self.listTab, text="reset", command=self.reset
        )
        self.list_resetButton.pack()

    def resetCompress(self):
        self.compress.set(0)
        self.decompress.set(0)
        self.no_chunks.set(0)
        self.master.update()

    def resetChunks(self):
        self.compress_chunks.set(0)
        self.master.update()

    def selectFile(self):
        self.file_path = askopenfilename()
        self.fileButton.config(text="\u2713")

    def selectFolder(self):
        self.file_path = askdirectory()
        self.download_fileButton.config(text="\u2713")

    def upload(self):
        t1 = time.perf_counter()
        repositoryType = self.repo_type.get()
        depId = self.dep_id.get()
        contentType = self.content_type.get()
        milestone = self.mile_stone.get()
        partNumber = self.part_number.get()
        contentFormat = self.file_format.get()
        version = self.version_number.get()
        readFilePath = self.file_path
        COMPRESS_FILE = self.compress.get() == 1
        DECOMPRESS_FILE = self.decompress.get() == 1
        if COMPRESS_FILE:
            DECOMPRESS_FILE = True
        COMPRESS_CHUNKS = self.compress_chunks.get() == 1
        NO_CHUNKS = self.no_chunks.get() == 1
        allowOverwrite = self.allow_overwrite.get() == 1
        resumable = self.resumable.get() == 1
        if (
            not readFilePath
            or not repositoryType
            or not depId
            or not contentType
            or not partNumber
            or not contentFormat
            or not version
        ):
            print("error - missing values")
            sys.exit()
        if not os.path.exists(readFilePath):
            sys.exit(f"error - file does not exist: {readFilePath}")
        if milestone.lower() == "none":
            milestone = ""

        if NO_CHUNKS:
            decompress = False
            fileExtension = ""
            extractChunk = True
            response = self.__cU.upload(
                readFilePath,
                repositoryType,
                depId,
                contentType,
                milestone,
                partNumber,
                contentFormat,
                version,
                decompress,
                fileExtension,
                allowOverwrite,
                resumable,
                extractChunk,
            )
            if response:
                status_code = response["status_code"]
                if not status_code == 200:
                    print("error in upload %d" % status_code)
                else:
                    self.upload_status.set("100%")
                    self.master.update()
            else:
                print("error in upload - no response")
            print(f"time {time.perf_counter() - t1} s")
            return

        # get upload parameters
        response = self.__cU.getUploadParameters(
            repositoryType,
            depId,
            contentType,
            milestone,
            partNumber,
            contentFormat,
            version,
            allowOverwrite,
            resumable,
        )
        if not response or response["status_code"] != 200:
            print("error in get upload parameters %r" % response)
            return
        saveFilePath = response["filePath"]
        chunkIndex = response["chunkIndex"]
        uploadId = response["uploadId"]
        # compress, then hash and compute file size parameter, then upload
        if COMPRESS_FILE:
            print("compressing file")
            readFilePath = UploadUtility(self.__cU.cP).compressFile(
                readFilePath, saveFilePath, self._COMPRESSION
            )
            print("new file name %s" % readFilePath)
        # hash
        hashType = self.__cU.cP.get("HASH_TYPE")
        fullTestHash = IoUtility().getHashDigest(readFilePath, hashType=hashType)
        # compute expected chunks
        fileSize = os.path.getsize(readFilePath)
        chunkSize = self.__cU.cP.get("CHUNK_SIZE")
        expectedChunks = 1
        if chunkSize < fileSize:
            expectedChunks = math.ceil(fileSize / chunkSize)
        fileExtension = os.path.splitext(readFilePath)[-1]
        extractChunk = True
        if DECOMPRESS_FILE or not COMPRESS_CHUNKS:
            extractChunk = False
        # upload chunks sequentially
        mD = {
            # chunk parameters
            "chunkSize": chunkSize,
            "chunkIndex": chunkIndex,
            "expectedChunks": expectedChunks,
            # upload file parameters
            "uploadId": uploadId,
            "hashType": hashType,
            "hashDigest": fullTestHash,
            # save file parameters
            "saveFilePath": saveFilePath,
            "fileSize": fileSize,
            "fileExtension": fileExtension,
            "decompress": DECOMPRESS_FILE,
            "allowOverwrite": allowOverwrite,
            "resumable": resumable,
            "extractChunk": extractChunk,
        }
        self.upload_status.set("0%")
        print(
            "decompress %s extract chunk %s compress chunks %s expected chunks %d"
            % (DECOMPRESS_FILE, extractChunk, COMPRESS_CHUNKS, expectedChunks)
        )
        for index in range(chunkIndex, expectedChunks):
            mD["chunkIndex"] = index
            status_code = self.__cU.uploadChunk(readFilePath, **mD)
            if not status_code == 200:
                print("error in upload %d" % status_code)
                break
            percentage = ((index + 1) / expectedChunks) * 100
            self.upload_status.set("%.0f%%" % percentage)
            self.master.update()
        print(f"time {time.perf_counter() - t1} s")

    def download(self):
        t1 = time.perf_counter()
        repositoryType = self.download_repo_type.get()
        depId = self.download_dep_id.get()
        contentType = self.download_content_type.get()
        milestone = self.download_mile_stone.get()
        partNumber = self.download_part_number.get()
        contentFormat = self.download_file_format.get()
        version = self.download_version_number.get()
        folderPath = self.file_path
        allowOverwrite = self.download_allow_overwrite.get() == 1
        chunkFile = self.download_chunked_file.get() == 1
        if (
            not folderPath
            or not repositoryType
            or not depId
            or not contentType
            or not partNumber
            or not contentFormat
            or not version
        ):
            print("error - missing values")
            sys.exit()
        if not os.path.exists(folderPath):
            print(f"error - folder does not exist: {folderPath}")
            sys.exit()
        if not os.path.isdir(folderPath):
            print("error - download folder is a file")
            sys.exit()
        if milestone.lower() == "none":
            milestone = ""

        # compute expected chunks
        response = self.__cU.fileSize(
            repositoryType,
            depId,
            contentType,
            milestone,
            partNumber,
            contentFormat,
            version,
        )
        if not response or response["status_code"] != 200:
            print("error computing file size")
            return
        fileSize = int(response["fileSize"])
        chunkSize = self.__cU.cP.get("CHUNK_SIZE")
        expectedChunks = 1
        if not chunkFile:
            chunkSize = None
        elif chunkSize < fileSize:
            expectedChunks = math.ceil(fileSize / chunkSize)

        for chunkIndex in range(0, expectedChunks):
            response = self.__cU.download(
                repositoryType=repositoryType,
                depId=depId,
                contentType=contentType,
                milestone=milestone,
                partNumber=partNumber,
                contentFormat=contentFormat,
                version=version,
                downloadFolder=folderPath,
                allowOverwrite=allowOverwrite,
                chunkSize=chunkSize,
                chunkIndex=chunkIndex,
                expectedChunks=expectedChunks,
            )
            if response and response["status_code"] == 200:
                percentage = ((chunkIndex + 1) / expectedChunks) * 100
                self.download_status.set("%.0f%%" % percentage)
                self.master.update()
            else:
                print("error - %d" % response["status_code"])
                return None

        print(f"time {time.perf_counter() - t1} s")

    def listDir(self):
        t1 = time.perf_counter()
        self.list_Listbox.delete(0, tk.END)
        depId = self.list_dep_id.get()
        repoType = self.list_repo_type.get()
        response = self.__cU.listDir(repoType, depId)
        if response and "dirList" in response and "status_code" in response:
            if response["status_code"] == 200:
                dirList = response["dirList"]
                index = 1
                if len(dirList) > 0:
                    print(f"{repoType} {depId}")
                    for fi in sorted(dirList):
                        print(f"\t{fi}")
                        self.list_Listbox.insert(index, fi)
                        index += 1
            print(response["status_code"])
        else:
            print("\nerror - not found\n")
        print(f"time {time.perf_counter() - t1} s")

    def reset(self):
        self.fileButton.config(text="select")
        self.download_fileButton.config(text="select")
        self.list_Listbox.delete(0, tk.END)
        self.upload_status.set("0%")
        self.download_status.set("0%")

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
        self.resumable.set(0)
        self.compress_chunks.set(0)
        self.no_chunks.set(0)

        self.download_repo_type.set("")
        self.download_dep_id.set("")
        self.download_content_type.set("")
        self.download_mile_stone.set("")
        self.download_part_number.set("1")
        self.download_file_format.set("")
        self.download_version_number.set("1")
        self.download_allow_overwrite.set(0)
        self.download_chunked_file.set(0)

        self.list_repo_type.set("")
        self.list_dep_id.set("")

        self.master.update()


if __name__ == "__main__":
    root = tk.Tk()
    gui = Gui(root)
    gui.mainloop()
