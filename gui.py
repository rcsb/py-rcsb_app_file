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
from rcsb.utils.io.CryptUtils import CryptUtils
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.ConfigProvider import ConfigProvider

""" modifiable variables
"""
base_url = "http://0.0.0.0:8000"
maxChunkSize = 1024 * 1024 * 8  # default

""" development testing variables 
    set sleep = true for slow motion testing of small files with min chunk size
"""
SLEEP = False
minChunkSize = 1024

""" do not alter from here
"""
os.environ["CACHE_PATH"] = os.path.join(
    ".", "rcsb", "app", "tests-file", "test-data", "data"
)
os.environ["CONFIG_FILE"] = os.path.join(".", "rcsb", "app", "config", "config.yml")
cachePath = os.environ.get("CACHE_PATH")
configFilePath = os.environ.get("CONFIG_FILE")
cP = ConfigProvider(cachePath)
cP.getConfig()
subject = cP.get("JWT_SUBJECT")
headerD = {
    "Authorization": "Bearer "
    + JWTAuthToken(cachePath, configFilePath).createToken({}, subject)
}
hashType = "MD5"

repoTypeList = ["deposit", "archive", "workflow", "session"]

milestoneList = ["upload", "upload-convert", "deposit", "annotate", "release", "review", "none"]

fileFormatExtensions = """pdbx: cif
    pdb: pdb
    cifeps: cifeps
    pdbml: xml
    nmr - star: str
    gz: gz
    tgz: tgz
    mtz: mtz
    html: html
    jpg: jpg
    png: png
    svg: svg
    gif: gif
    tif: tif
    tiff: tiff
    sdf: sdf
    ccp4: ccp4
    mrc2000: mrc
    pic: pic
    txt: txt
    xml: xml
    pdf: pdf
    map: map
    bcif: bcif
    amber: amber
    amber - aux: amber - aux
    cns: cns
    cyana: cyana
    xplor: xplor
    xplor - nih: xplor - nih
    pdb - mr: mr
    mr: mr
    json: json
    fsa: fsa
    fasta: fasta
    any: dat
    mdl: mdl
    tar: tar"""
fileFormatExtensionD = dict()
for s in fileFormatExtensions.split('\n'):
    key = s.split(':')[0].lstrip().rstrip()
    val = s.split(':')[1].lstrip().rstrip()
    fileFormatExtensionD[key] = val

contentTypes = """model:
      -
        - pdbx
        - pdb
        - pdbml
        - cifeps
      - model
    model-emd:
      -
        - pdbx
        - xml
      - model-emd
    model-aux:
      -
        - pdbx
      - model-aux
    model-legacy-rcsb:
      -
        - pdbx
        - pdb
      - model-legacy-rcsb
    structure-factors:
      -
        - pdbx
        - mtz
        - txt
      - sf
    structure-factors-legacy-rcsb:
      -
        - pdbx
        - mtz
      - sf-legacy-rcsb
    nmr-data-config:
      -
        - json
      - nmr-data-config
    nmr-data-nef:
      -
        - nmr-star
        - pdbx
      - nmr-data-nef
    nmr-data-str:
      -
        - nmr-star
        - pdbx
      - nmr-data-str
    nmr-data-nef-report:
      -
        - json
      - nmr-data-nef-report
    nmr-data-str-report:
      -
        - json
      - nmr-data-str-report
    nmr-restraints:
      -
        - any
        - nmr-star
        - amber
        - amber-aux
        - cns
        - cyana
        - xplor
        - xplor-nih
        - pdb-mr
        - mr
      - mr
    nmr-chemical-shifts:
      -
        - nmr-star
        - pdbx
        - any
      - cs
    nmr-chemical-shifts-raw:
      -
        - nmr-star
        - pdbx
      - cs-raw
    nmr-chemical-shifts-auth:
      -
        - nmr-star
        - pdbx
      - cs-auth
    nmr-chemical-shifts-upload-report:
      -
        - pdbx
      - nmr-chemical-shifts-upload-report
    nmr-chemical-shifts-atom-name-report:
      -
        - pdbx
      - nmr-chemical-shifts-atom-name-report
    nmr-shift-error-report:
      -
        - json
      - nmr-shift-error-report
    nmr-bmrb-entry:
      -
        - nmr-star
        - pdbx
      - nmr-bmrb-entry
    nmr-harvest-file:
      -
        - tgz
      - nmr-harvest-file
    nmr-peaks:
      -
        - any
      - nmr-peaks
    nmr-nef:
      -
        - nmr-star
        - pdbx
      - nmr-nef
    nmr-cs-check-report:
      -
        - html
      - nmr-cs-check-report
    nmr-cs-xyz-check-report:
      -
        - html
      - nmr-cs-xyz-check-report
    nmr-cs-path-list:
      -
        - txt
      - nmr-cs-path-list
    nmr-cs-auth-file-name-list:
      -
        - txt
      - nmr-cs-auth-file-name-list
    nmr-mr-path-list:
      -
        - json
      - nmr-mr-path-list
    component-image:
      -
        - jpg
        - png
        - gif
        - svg
        - tif
        - tiff
      - ccimg
    component-definition:
      -
        - pdbx
        - sdf
      - ccdef
    em-volume:
      -
        - map
        - ccp4
        - mrc2000
        - bcif
      - em-volume
    em-mask-volume:
      -
        - map
        - ccp4
        - mrc2000
        - bcif
      - em-mask-volume
    em-additional-volume:
      -
        - map
        - ccp4
        - mrc2000
        - bcif
      - em-additional-volume
    em-half-volume:
      -
        - map
        - ccp4
        - mrc2000
        - bcif
      - em-half-volume
    em-raw-volume:
      -
        - map
        - ccp4
        - mrc2000
        - bcif
      - em-raw-volume
    em-fsc-half-mask-volume:
      -
        - map
        - ccp4
        - mrc2000
        - bcif
      - em-fsc-half-mask-volume
    em-fsc-map-model-mask-volume:
      -
        - map
        - ccp4
        - mrc2000
        - bcif
      - em-fsc-map-model-mask-volume
    em-alignment-mask-volume:
      -
        - map
        - ccp4
        - mrc2000
        - bcif
      - em-alignment-mask-volume
    em-focused-refinement-mask-volume:
      -
        - map
        - ccp4
        - mrc2000
        - bcif
      - em-focused-refinement-mask-volume
    em-3d-classification-additional-volume:
      -
        - map
        - ccp4
        - mrc2000
        - bcif
      - em-3d-classification-additional-volume
    em-focus-refinement-additional-volume:
      -
        - map
        - ccp4
        - mrc2000
        - bcif
      - em-focus-refinement-additional-volume
    em-segmentation-volume:
      -
        - map
        - ccp4
        - mrc2000
        - bcif
      - em-segmentation-volume
    em-volume-wfcfg:
      -
        - json
      - em-volume-wfcfg
    em-mask-volume-wfcfg:
      -
        - json
      - em-mask-volume-wfcfg
    em-additional-volume-wfcfg:
      -
        - json
      - em-additional-volume-wfcfg
    em-half-volume-wfcfg:
      -
        - json
      - em-half-volume-wfcfg
    em-volume-report:
      -
        - json
      - em-volume-report
    em-volume-header:
      -
        - xml
      - em-volume-header
    em-model-emd:
      -
        - pdbx
      - em-model-emd
    em-structure-factors:
      -
        - pdbx
        - mtz
      - em-sf
    emd-xml-header-report:
      -
        - txt
      - emd-xml-header-report
    validation-report-depositor:
      -
        - pdf
      - valdep
    seqdb-match:
      -
        - pdbx
        - pic
      - seqdb-match
    blast-match:
      -
        - xml
      - blast-match
    seq-assign:
      -
        - pdbx
      - seq-assign
    partial-seq-annotate:
      -
        - txt
      - partial-seq-annotate
    seq-data-stats:
      -
        - pic
      - seq-data-stats
    seq-align-data:
      -
        - pic
      - seq-align-data
    pre-seq-align-data:
      -
        - pic
      - pre-seq-align-data
    seqmatch:
      -
        - pdbx
      - seqmatch
    mismatch-warning:
      -
        - pic
      - mismatch-warning
    polymer-linkage-distances:
      -
        - pdbx
        - json
      - poly-link-dist
    polymer-linkage-report:
      -
        - html
      - poly-link-report
    geometry-check-report:
      -
        - pdbx
      - geometry-check-report
    chem-comp-link:
      -
        - pdbx
      - cc-link
    chem-comp-assign:
      -
        - pdbx
      - cc-assign
    chem-comp-assign-final:
      -
        - pdbx
      - cc-assign-final
    chem-comp-assign-details:
      -
        - pic
      - cc-assign-details
    chem-comp-depositor-info:
      -
        - pdbx
      - cc-dpstr-info
    prd-search:
      -
        - pdbx
      - prd-summary
    assembly-report:
      -
        - txt
        - xml
      - assembly-report
    assembly-assign:
      -
        - pdbx
        - txt
      - assembly-assign
    assembly-depinfo-update:
      -
        - txt
      - assembly-depinfo-update
    interface-assign:
      -
        - xml
      - interface-assign
    assembly-model:
      -
        - pdb
        - pdbx
      - assembly-model
    assembly-model-xyz:
      -
        - pdb
        - pdbx
      - assembly-model-xyz
    site-assign:
      -
        - pdbx
      - site-assign
    dict-check-report:
      -
        - txt
      - dict-check-report
    dict-check-report-r4:
      -
        - txt
      - dict-check-report-r4
    dict-check-report-next:
      -
        - txt
      - dict-check-report-next
    tom-complex-report:
      -
        - txt
      - tom-upload-report
    tom-merge-report:
      -
        - txt
      - tom-merge-report
    format-check-report:
      -
        - txt
      - format-check-report
    misc-check-report:
      -
        - txt
      - misc-check-report
    special-position-report:
      -
        - txt
      - special-position-report
    merge-xyz-report:
      -
        - txt
      - merge-xyz-report
    model-issues-report:
      -
        - json
      - model-issues-report
    structure-factor-report:
      -
        - json
      - structure-factor-report
    validation-report:
      -
        - pdf
      - val-report
    validation-report-full:
      -
        - pdf
      - val-report-full
    validation-report-slider:
      -
        - png
        - svg
      - val-report-slider
    validation-data:
      -
        - pdbx
        - xml
      - val-data
    validation-report-2fo-map-coef:
      -
        - pdbx
      - val-report-wwpdb-2fo-fc-edmap-coef
    validation-report-fo-map-coef:
      -
        - pdbx
      - val-report-wwpdb-fo-fc-edmap-coef
    validation-report-images:
      -
        - tar
      - val-report-images
    map-xray:
      -
        - bcif
      - map-xray
    map-2fofc:
      -
        - map
      - map-2fofc
    map-fofc:
      -
        - map
      - map-fofc
    map-omit-2fofc:
      -
        - map
      - map-omit-2fofc
    map-omit-fofc:
      -
        - map
      - map-omit-fofc
    sf-convert-report:
      -
        - pdbx
        - txt
      - sf-convert-report
    em-sf-convert-report:
      -
        - pdbx
        - txt
      - em-sf-convert-report
    dcc-report:
      -
        - pdbx
        - txt
      - dcc-report
    mapfix-header-report:
      -
        - json
      - mapfix-header-report
    mapfix-report:
      -
        - txt
      - mapfix-report
    secondary-structure-topology:
      -
        - txt
      - ss-topology
    sequence-fasta:
      -
        - fasta
        - fsa
      - fasta
    messages-from-depositor:
      -
        - pdbx
      - messages-from-depositor
    messages-to-depositor:
      -
        - pdbx
      - messages-to-depositor
    notes-from-annotator:
      -
        - pdbx
      - notes-from-annotator
    correspondence-to-depositor:
      -
        - txt
      - correspondence-to-depositor
    correspondence-legacy-rcsb:
      -
        - pdbx
      - correspondence-legacy-rcsb
    correspondence-info:
      -
        - pdbx
      - correspondence-info
    map-header-data:
      -
        - json
        - pic
        - txt
      - map-header-data
    deposit-volume-params:
      -
        - pic
      - deposit-volume-params
    fsc:
      -
        - xml
      - fsc-xml
    fsc-report:
      -
        - txt
      - fsc-report
    res-est-fsc:
      -
        - xml
      - res-est-fsc
    res-est-fsc-report:
      -
        - txt
      - res-est-fsc-report
    map-model-fsc:
      -
        - xml
      - map-model-fsc
    map-model-fsc-report:
      -
        - txt
      - map-model-fsc-report
    em2em-report:
      -
        - txt
      - em2em-report
    img-emdb:
      -
        - jpg
        - png
        - gif
        - svg
        - tif
      - img-emdb
    img-emdb-report:
      -
        - txt
      - img-emdb-report
    layer-lines:
      -
        - txt
      - layer-lines
    auxiliary-file:
      -
        - any
      - aux-file
    status-history:
      -
        - pdbx
      - status-history
    virus-matrix:
      -
        - any
      - virus
    parameter-file:
      -
        - any
      - parm
    structure-def-file:
      -
        - any
      - struct
    topology-file:
      -
        - any
      - topo
    cmd-line-args:
      -
        - txt
      - cmd-line-args
    sd-dat:
      -
        - a
        - n
        - y
      - sd-dat
    sx-pr:
      -
        - a
        - n
        - y
      - sx-pr
    sm-fit:
      -
        - a
        - n
        - y
      - sm-fit
    deposition-info:
      -
        - pdbx
        - json
      - deposition-info
    deposition-store:
      -
        - tar
      - deposition-store
    bundle-session-archive:
      -
        - tar
        - tgz
      - bundle-session-archive
    bundle-session-deposit:
      -
        - tar
        - tgz
      - bundle-session-deposit
    bundle-session-upload:
      -
        - tar
        - tgz
      - bundle-session-upload
    bundle-session-tempdep:
      -
        - tar
        - tgz
      - bundle-session-tempdep
    bundle-session-uitemp:
      -
        - tar
        - tgz
      - bundle-session-uitemp
    bundle-session-workflow:
      -
        - tar
        - tgz
      - bundle-session-workflow
    session-backup:
      -
        - tar
        - tgz
      - bundle-session-workflow
    manifest-session:
      -
        - json
      - manifest-session
    manifest-session-bundle:
      -
        - json
      - manifest-session-bundle
    any:
      -
        - any
      - any"""
contentTypeInfoD = dict()
key = None
val = None
prev = None
for content in contentTypes.split('\n'):
    contentType = content.replace(':','').replace('- ','').lstrip().rstrip()
    if content.find(':') >= 0:
        if key and val:
            contentTypeInfoD.update({key: val})
        key = contentType
    val = contentType

class Gui(tk.Frame):
    def __init__(self, master):
        super().__init__(master)
        # master.geometry("500x500")
        master.title("FILE ACCESS AND DEPOSITION APPLICATION")

        self.tabs = ttk.Notebook(master)
        self.splashTab = ttk.Frame(master)
        # self.instructionsTab = ttk.Frame(master)
        self.uploadTab = ttk.Frame(master)
        self.downloadTab = ttk.Frame(master)
        self.listTab = ttk.Frame(master)
        self.tabs.add(self.splashTab, text='HOME')
        # self.tabs.add(self.instructionsTab, text='INSTRUCTIONS')
        self.tabs.add(self.uploadTab, text='UPLOAD')
        self.tabs.add(self.downloadTab, text='DOWNLOAD')
        self.tabs.add(self.listTab, text='LIST')
        self.tabs.pack(expand=1, fill='both')

        load = Image.open("./onedep_logo.png")
        render = ImageTk.PhotoImage(load)
        img = ttk.Label(self.splashTab, image=render)
        img.image = render
        img.place(relx=0.5, rely=0.5, anchor=tk.CENTER)
        img.pack(fill=tk.BOTH, expand=1)

#         instructionsText = """
# UPLOAD FILE
#     You may upload one file at a time.
# DOWNLOAD FILE
#     You may download one file at a time.
# LIST FILES
#     Specify deposit id to list files within the specified directory.
#         """
#         instructionsLabel = tk.Label(self.instructionsTab, text=instructionsText)
#         instructionsLabel.pack()

        # UPLOADS

        self.repo_type = tk.StringVar(master)
        self.dep_id = tk.StringVar(master)
        self.content_type = tk.StringVar(master)
        self.mile_stone = tk.StringVar(master)
        self.part_number = tk.StringVar(master)
        self.file_format = tk.StringVar(master)
        self.version_number = tk.StringVar(master)
        self.allow_overwrite = tk.IntVar(master)
        self.expedite = tk.IntVar(master)
        self.chunk = tk.IntVar(master)
        self.compress = tk.IntVar(master)
        self.upload_status = tk.StringVar(master)
        self.upload_status.set('0%')
        self.file_path = None

        self.fileButtonLabel = ttk.Label(self.uploadTab, text="UPLOAD FILE")
        self.fileButtonLabel.pack()
        self.fileButton = ttk.Button(self.uploadTab, text="select", command=self.selectfile)
        self.fileButton.pack()

        self.expediteCheckbox = ttk.Checkbutton(self.uploadTab, text="expedite", variable=self.expedite)
        self.expediteCheckbox.pack()

        self.allowCheckbox = ttk.Checkbutton(self.uploadTab, text="allow overwrite", variable=self.allow_overwrite)
        self.allowCheckbox.pack()

        self.chunkCheckbox = ttk.Checkbutton(self.uploadTab, text="chunk file", variable=self.chunk)
        self.chunkCheckbox.pack()

        # full file compression was slow so there was no point to compression
        # self.compressCheckbox = ttk.Checkbutton(self.uploadTab, text="compress", variable=self.compress)
        # self.compressCheckbox.pack()

        self.repoTypeLabel = ttk.Label(self.uploadTab, text="REPOSITORY TYPE")
        self.repoTypeLabel.pack()
        self.repoTypeListbox = ttk.Combobox(self.uploadTab, exportselection=0, textvariable=self.repo_type)
        self.repoTypeListbox.pack()
        self.repoTypeListbox['values'] = repoTypeList
        self.repoTypeListbox.current()
        # self.repoTypeListbox.insert(1, "deposit")
        # self.repoTypeListbox.insert(2, "archive")
        # self.repoTypeListbox.insert(3, "workflow")
        # self.repoTypeListbox.insert(4, "session")

        self.depIdLabel = ttk.Label(self.uploadTab, text="DEPOSIT ID")
        self.depIdLabel.pack()
        self.depIdEntry = ttk.Entry(self.uploadTab, textvariable=self.dep_id)
        self.depIdEntry.pack()

        self.contentTypeLabel = ttk.Label(self.uploadTab, text="CONTENT TYPE")
        self.contentTypeLabel.pack()
        self.contentTypeListbox = ttk.Combobox(self.uploadTab, exportselection=0, textvariable=self.content_type)
        self.contentTypeListbox.pack()
        self.contentTypeListbox['values'] = [key for key in contentTypeInfoD.keys()]
        # for index, key in enumerate(contentTypeInfoD.keys()):
        #     self.contentTypeListbox.insert(index + 1, key)

        self.milestoneLabel = ttk.Label(self.uploadTab, text="MILESTONE")
        self.milestoneLabel.pack()
        # self.milestoneEntry = ttk.Entry(self.uploadTab, textvariable=self.mile_stone)
        # self.milestoneEntry.pack()
        self.milestoneListbox = ttk.Combobox(self.uploadTab, exportselection=0, textvariable=self.mile_stone)
        self.milestoneListbox.pack()
        self.milestoneListbox['values'] = milestoneList
        self.milestoneListbox.current()

        self.partLabel = ttk.Label(self.uploadTab, text="PART NUMBER")
        self.partLabel.pack()
        self.partNumberEntry = ttk.Entry(self.uploadTab, textvariable=self.part_number)
        self.partNumberEntry.insert(1, "1")
        self.partNumberEntry.pack()

        self.fileFormatLabel = ttk.Label(self.uploadTab, text="CONTENT FORMAT")
        self.fileFormatLabel.pack()
        self.fileFormatListbox = ttk.Combobox(self.uploadTab, exportselection=0, textvariable=self.file_format)
        self.fileFormatListbox.pack()
        self.fileFormatListbox['values'] = [key for key in fileFormatExtensionD.keys()]
        # for index, key in enumerate(fileFormatExtensionD.keys()):
        #     self.fileFormatListbox.insert(index + 1, key)

        self.versionLabel = ttk.Label(self.uploadTab, text="VERSION")
        self.versionLabel.pack()
        self.versionEntry = ttk.Entry(self.uploadTab, textvariable=self.version_number)
        self.versionEntry.insert(1, "next")
        self.versionEntry.pack()

        self.uploadButton = ttk.Button(self.uploadTab, text='submit', command=self.upload)
        self.uploadButton.pack()

        self.statusLabel = ttk.Label(self.uploadTab, textvariable=self.upload_status)
        self.statusLabel.pack()

        self.resetButton = ttk.Button(self.uploadTab, text='reset', command=self.reset)
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
        self.download_status.set('0%')
        self.download_file_path = None

        self.download_fileButtonLabel = ttk.Label(self.downloadTab, text="DESTINATION FOLDER")
        self.download_fileButtonLabel.pack()
        self.download_fileButton = ttk.Button(self.downloadTab, text="select", command=self.selectfolder)
        self.download_fileButton.pack()

        self.download_allowOverwrite = ttk.Checkbutton(self.downloadTab, text="allow overwrite", variable=self.download_allow_overwrite)
        self.download_allowOverwrite.pack()

        self.download_repoTypeLabel = ttk.Label(self.downloadTab, text="REPOSITORY TYPE")
        self.download_repoTypeLabel.pack()
        self.download_repoTypeListbox = ttk.Combobox(self.downloadTab, exportselection=0, textvariable=self.download_repo_type)
        self.download_repoTypeListbox.pack()
        self.download_repoTypeListbox['values'] = repoTypeList
        # self.download_repoTypeListbox['values'] = ('deposit', 'archive', 'workflow', 'session')
        self.download_repoTypeListbox.current()

        self.download_depIdLabel = ttk.Label(self.downloadTab, text="DEPOSIT ID")
        self.download_depIdLabel.pack()
        self.download_depIdEntry = ttk.Entry(self.downloadTab, textvariable=self.download_dep_id)
        self.download_depIdEntry.pack()

        self.download_contentTypeLabel = ttk.Label(self.downloadTab, text="CONTENT TYPE")
        self.download_contentTypeLabel.pack()
        self.download_contentTypeListbox = ttk.Combobox(self.downloadTab, exportselection=0, textvariable=self.download_content_type)
        self.download_contentTypeListbox.pack()
        self.download_contentTypeListbox['values'] = [key for key in contentTypeInfoD.keys()]

        self.download_milestoneLabel = ttk.Label(self.downloadTab, text="MILESTONE")
        self.download_milestoneLabel.pack()
        # self.download_milestoneEntry = ttk.Entry(self.downloadTab, textvariable=self.download_mile_stone)
        # self.download_milestoneEntry.pack()
        self.download_milestoneListbox = ttk.Combobox(self.downloadTab, exportselection=0, textvariable=self.download_mile_stone)
        self.download_milestoneListbox.pack()
        self.download_milestoneListbox['values'] = milestoneList
        self.download_milestoneListbox.current()

        self.download_partLabel = ttk.Label(self.downloadTab, text="PART NUMBER")
        self.download_partLabel.pack()
        self.download_partNumberEntry = ttk.Entry(self.downloadTab, textvariable=self.download_part_number)
        self.download_partNumberEntry.insert(1, "1")
        self.download_partNumberEntry.pack()

        self.download_fileFormatLabel = ttk.Label(self.downloadTab, text="CONTENT FORMAT")
        self.download_fileFormatLabel.pack()
        self.download_fileFormatListbox = ttk.Combobox(self.downloadTab, exportselection=0, textvariable=self.download_file_format)
        self.download_fileFormatListbox.pack()
        self.download_fileFormatListbox['values'] = [key for key in fileFormatExtensionD.keys()]

        self.download_versionLabel = ttk.Label(self.downloadTab, text="VERSION")
        self.download_versionLabel.pack()
        self.download_versionEntry = ttk.Entry(self.downloadTab, textvariable=self.download_version_number)
        self.download_versionEntry.insert(1, "1")
        self.download_versionEntry.pack()

        self.downloadButton = ttk.Button(self.downloadTab, text='submit', command=self.download)
        self.downloadButton.pack()

        self.download_statusLabel = ttk.Label(self.downloadTab, textvariable=self.download_status)
        self.download_statusLabel.pack()

        self.download_resetButton = ttk.Button(self.downloadTab, text='reset', command=self.reset)
        self.download_resetButton.pack()

        # LIST DIRECTORY

        self.list_repo_type = tk.StringVar(master)
        self.list_dep_id = tk.StringVar(master)

        self.list_repoTypeLabel = ttk.Label(self.listTab, text="REPOSITORY TYPE")
        self.list_repoTypeLabel.pack()
        self.list_repoTypeListbox = ttk.Combobox(self.listTab, exportselection=0, textvariable=self.list_repo_type)
        self.list_repoTypeListbox.pack()
        self.list_repoTypeListbox['values'] = repoTypeList
        # self.list_repoTypeListbox['values'] = ('deposit', 'archive', 'workflow', 'session')
        self.list_repoTypeListbox.current()

        self.list_depIdLabel = ttk.Label(self.listTab, text="DEPOSIT ID")
        self.list_depIdLabel.pack()
        self.list_depIdEntry = ttk.Entry(self.listTab, textvariable=self.list_dep_id)
        self.list_depIdEntry.pack()

        self.listButton = ttk.Button(self.listTab, text='submit', command=self.listDir)
        self.listButton.pack()

        self.list_Listbox = tk.Listbox(self.listTab, exportselection=0, width=50)
        self.list_Listbox.pack(pady=50)

        self.list_resetButton = ttk.Button(self.listTab, text='reset', command=self.reset)
        self.list_resetButton.pack()

    def selectfile(self):
        self.file_path = askopenfilename()
        self.fileButton.config(text='\u2713')

    def selectfolder(self):
        self.file_path = askdirectory()
        self.download_fileButton.config(text='\u2713')

    def upload(self):
        global headerD
        global SLEEP
        global maxChunkSize
        global minChunkSize
        t1 = time.perf_counter()
        filePath = self.file_path
        EXPEDITE = self.expedite.get() == 1
        allowOverwrite = self.allow_overwrite.get() == 1
        CHUNK = self.chunk.get() == 1
        COMPRESS = self.compress.get() == 1
        repositoryType = self.repo_type.get()
        # repositoryType = self.repoTypeListbox.get(self.repoTypeListbox.curselection()[0])
        depId = self.dep_id.get()
        contentType = self.content_type.get()
        milestone = self.mile_stone.get()
        convertedMilestone = None
        if milestone and milestone.lower() != "none":
            convertedMilestone = f'-{milestone}'
        else:
            convertedMilestone = ""
        partNumber = self.part_number.get()
        fileFormat = self.file_format.get()
        version = self.version_number.get()
        if not filePath or not repositoryType or not depId or not contentType or not partNumber or not fileFormat or not version:
            print('error - missing values')
            sys.exit()
        if not os.path.exists(filePath):
            sys.exit(f'error - file does not exist: {filePath}')
        if milestone.lower() == "none":
            milestone = ""
        if COMPRESS:
            tempPath = filePath + ".gz"
            with open(filePath, "rb") as r:
                with gzip.open(tempPath, "wb") as w:
                    w.write(r.read())
            filePath = tempPath
        hD = CryptUtils().getFileHash(filePath, hashType=hashType)
        fullTestHash = hD["hashDigest"]
        chunkSize = maxChunkSize
        if SLEEP:
            chunkSize = minChunkSize
        fileSize = os.path.getsize(filePath)
        expectedChunks = 0
        if chunkSize < fileSize:
            expectedChunks = fileSize // chunkSize
            if fileSize % chunkSize:
                expectedChunks = expectedChunks + 1
        else:
            expectedChunks = 1
        chunkIndex = 0
        chunkOffset = 0
        chunkMode = "sequential"
        copyMode = "native"
        if COMPRESS:
            copyMode = "gzip_decompress"
        if EXPEDITE and not CHUNK:
            mD = {
                # upload file parameters
                "filePath": filePath,
                "uploadId": None,
                "fileSize": fileSize,
                "copyMode": copyMode,
                "hashType": hashType,
                "hashDigest": fullTestHash,
                # save file parameters
                "repositoryType": repositoryType,
                "depId": depId,
                "contentType": contentType,
                "milestone": milestone,
                "partNumber": partNumber,
                "contentFormat": fileFormat,
                "version": version,
                "allowOverwrite": allowOverwrite
            }
            response = None
            with open(filePath, "rb") as to_upload:
                url = os.path.join(base_url, "file-v2", "expediteFile")
                response = requests.post(
                    url,
                    data=deepcopy(mD),
                    headers=headerD,
                    files={"uploadFile": to_upload},
                    timeout=None,
                )
            if response.status_code != 200:
                print(
                    f"error - status code {response.status_code} {response.text}...terminating"
                )
            self.upload_status.set(f'100%')
            self.master.update()
            print(response)
            print(f'time {time.perf_counter() - t1} s')
            return
        if EXPEDITE and CHUNK:
            mD = {
                # upload file parameters
                "uploadId": None,
                "fileSize": fileSize,
                "copyMode": copyMode,
                "hashType": hashType,
                "hashDigest": fullTestHash,
                # chunk parameters
                "chunkSize": chunkSize,
                "chunkIndex": chunkIndex,
                "chunkOffset": chunkOffset,
                "expectedChunks": expectedChunks,
                "chunkMode": chunkMode,
                # save file parameters
                "filePath": filePath
            }
            readFilePath = mD["filePath"]
            url = os.path.join(base_url, "file-v2", "prepChunks")
            parameters = {"repositoryType": repositoryType,
                          "depId": depId,
                          "contentType": contentType,
                          "milestone": milestone,
                          "partNumber": str(partNumber),
                          "contentFormat": fileFormat,
                          "allowOverwrite": allowOverwrite
                          }
            response = requests.get(
                url,
                params=parameters,
                headers=headerD,
                timeout=None
            )
            if response.status_code == 200:
                result = json.loads(response.text)
                if result:
                    mD["filePath"] = str(result)
            url = os.path.join(base_url, "file-v2", "getNewUploadId")
            response = requests.get(
                url,
                headers=headerD,
                timeout=None
            )
            if response.status_code == 200:
                result = json.loads(response.text)
                if result:
                    mD["uploadId"] = str(result)
            # chunk file and upload
            offset = 0
            offsetIndex = 0
            responses = []
            tmp = io.BytesIO()
            with open(readFilePath, "rb") as to_upload:
                to_upload.seek(offset)
                url = os.path.join(base_url, "file-v2", "expediteChunk")
                for x in range(offsetIndex, mD["expectedChunks"]):
                    packet_size = min(
                        int(mD["fileSize"]) - (int(mD["chunkIndex"]) * int(mD["chunkSize"])),
                        int(mD["chunkSize"]),
                    )
                    tmp.truncate(packet_size)
                    tmp.seek(0)
                    tmp.write(to_upload.read(packet_size))
                    tmp.seek(0)
                    response = requests.post(
                        url,
                        data=deepcopy(mD),
                        headers=headerD,
                        files={"uploadFile": tmp},
                        timeout=None,
                    )
                    if response.status_code != 200:
                        print(
                            f"error - status code {response.status_code} {response.text}...terminating"
                        )
                        break
                    responses.append(response)
                    mD["chunkIndex"] += 1
                    mD["chunkOffset"] = mD["chunkIndex"] * mD["chunkSize"]
                    if SLEEP:
                        time.sleep(1)
                    self.status = math.ceil((mD["chunkIndex"] / mD["expectedChunks"]) * 100)
                    self.upload_status.set(f'{self.status}%')
                    self.master.update()
            print(responses)
            print(f'time {time.perf_counter() - t1} s')
            return
        mD = {
            # upload file parameters
            "filePath": filePath,
            "uploadId": None,
            "fileSize": fileSize,
            "copyMode": copyMode,
            "hashType": hashType,
            "hashDigest": fullTestHash,
            # chunk parameters
            "chunkSize": chunkSize,
            "chunkIndex": chunkIndex,
            "chunkOffset": chunkOffset,
            "expectedChunks": expectedChunks,
            "chunkMode": chunkMode,
            # save file parameters
            "repositoryType": repositoryType,
            "depId": depId,
            "contentType": contentType,
            "milestone": milestone,
            "partNumber": partNumber,
            "contentFormat": fileFormat,
            "version": version,
            "allowOverwrite": allowOverwrite
        }
        # test for resumed upload
        uploadId = mD["uploadId"]
        url = os.path.join(base_url, "file-v2", "uploadStatus")
        parameters = {"repositoryType": mD["repositoryType"],
                      "depId": mD["depId"],
                      "contentType": mD["contentType"],
                      "milestone": mD["milestone"],
                      "partNumber": str(mD["partNumber"]),
                      "contentFormat": mD["contentFormat"],
                      "hashDigest": mD["hashDigest"]
                      }
        response = requests.get(
            url,
            params=parameters,
            headers=headerD,
            timeout=None
        )
        offsetIndex = 0
        offset = 0
        if response.status_code == 200:
            result = json.loads(response.text)
            if result:
                if not isinstance(result, dict):
                    result = eval(result)
                offsetIndex = int(result["uploadCount"])
                packet_size = min(
                    int(mD["fileSize"]) - (int(mD["chunkIndex"]) * int(mD["chunkSize"])),
                    int(mD["chunkSize"]),
                )
                offset = offsetIndex * packet_size
                mD["chunkIndex"] = offsetIndex
                mD["chunkOffset"] = offset
        # chunk file and upload
        responses = []
        tmp = io.BytesIO()
        with open(mD["filePath"], "rb") as to_upload:
            to_upload.seek(offset)
            url = os.path.join(base_url, "file-v2", "upload")
            for x in range(offsetIndex, mD["expectedChunks"]):
                packet_size = min(
                    int(mD["fileSize"]) - (int(mD["chunkIndex"]) * int(mD["chunkSize"])),
                    int(mD["chunkSize"]),
                )
                tmp.truncate(packet_size)
                tmp.seek(0)
                tmp.write(to_upload.read(packet_size))
                tmp.seek(0)
                response = requests.post(
                    url,
                    data=deepcopy(mD),
                    headers=headerD,
                    files={"uploadFile": tmp},
                    timeout=None,
                )
                if response.status_code != 200:
                    print(
                        f"error - status code {response.status_code} {response.text}...terminating"
                    )
                    break
                responses.append(response)
                mD["chunkIndex"] += 1
                mD["chunkOffset"] = mD["chunkIndex"] * mD["chunkSize"]
                if SLEEP:
                    time.sleep(1)
                self.status = math.ceil((mD["chunkIndex"] / mD["expectedChunks"]) * 100)
                self.upload_status.set(f'{self.status}%')
                self.master.update()
        print(responses)
        print(f'time {time.perf_counter() - t1} s')

    def download(self):
        global headerD
        global SLEEP
        global maxChunkSize
        global minChunkSize
        t1 = time.perf_counter()
        allowOverwrite = self.download_allow_overwrite.get() == 1
        repositoryType = self.download_repo_type.get()
        depId = self.download_dep_id.get()
        contentType = self.download_content_type.get()
        milestone = self.download_mile_stone.get()
        convertedMilestone = None
        if milestone and milestone.lower() != 'none':
            convertedMilestone = f'-{milestone}'
        else:
            convertedMilestone = ""
        partNumber = self.download_part_number.get()
        fileFormat = self.download_file_format.get()
        version = self.download_version_number.get()
        folderPath = self.file_path
        if not folderPath or not repositoryType or not depId or not contentType or not partNumber or not fileFormat or not version:
            print('error - missing values')
            sys.exit()
        convertedFileFormat = fileFormatExtensionD[fileFormat]
        fileName = f'{depId}_{contentType}{convertedMilestone}_P{partNumber}.{convertedFileFormat}.V{version}'
        downloadFilePath = os.path.join(folderPath, fileName)
        if not os.path.exists(folderPath):
            print(f'error - folder does not exist: {downloadFilePath}')
            sys.exit()
        if os.path.exists(downloadFilePath):
            if not allowOverwrite:
                print(f'error - file already exists: {downloadFilePath}')
                sys.exit()
            os.remove(downloadFilePath)
        if milestone.lower() == "none":
            milestone = ""
        downloadDict = {
            "depId": depId,
            "repositoryType": repositoryType,
            "contentType": contentType,
            "contentFormat": fileFormat,
            "partNumber": partNumber,
            "version": str(version),
            "hashType": hashType,
            "milestone": milestone
        }
        url = os.path.join(base_url, "file-v1", "downloadSize")
        fileSize = requests.get(url, params=downloadDict, headers=headerD, timeout=None).text
        if not fileSize.isnumeric():
            print(f'error - no response for {downloadFilePath}')
            return None
        fileSize = int(fileSize)
        chunkSize = maxChunkSize
        if SLEEP:
            chunkSize = minChunkSize
        chunks = math.ceil(fileSize / chunkSize)
        url = os.path.join(base_url, "file-v1", "download", repositoryType)
        responseCode = None
        count = 0
        with requests.get(url, params=downloadDict, headers=headerD, timeout=None, stream=True) as response:
            with open(downloadFilePath, "ab") as ofh:
                for chunk in response.iter_content(chunk_size=chunkSize):
                    # print(f'writing chunk {count} of {chunks} size {chunkSize}')
                    if chunk:
                        ofh.write(chunk)
                    # ofh.flush()
                    # os.fsync(ofh.fileno())
                    count += 1
                    if SLEEP:
                        time.sleep(1)
                    self.status = math.ceil((count / chunks) * 100)
                    self.download_status.set(f'{self.status}%')
                    self.master.update()
            responseCode = response.status_code
            rspHashType = response.headers["rcsb_hash_type"]
            rspHashDigest = response.headers["rcsb_hexdigest"]
            thD = CryptUtils().getFileHash(downloadFilePath, hashType=rspHashType)
            if not thD["hashDigest"] == rspHashDigest:
                print('error - hash comparison failed')
                sys.exit()
        print(f'response {responseCode}')
        print(f'time {time.perf_counter() - t1} s')

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
            print('error - missing values')
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
        print(f'response {responseCode}')
        index = 1
        if responseCode == 200:
            for fi in sorted(dirList):
                print(f'\t{fi}')
                self.list_Listbox.insert(index, fi)
                index += 1
        print(f'time {time.perf_counter() - t1} s')

    def reset(self):
        self.fileButton.config(text='select')
        self.download_fileButton.config(text='select')
        self.list_Listbox.delete(0, tk.END)
        self.upload_status.set(f'0%')
        self.download_status.set(f'0%')

        self.repo_type.set("")
        self.dep_id.set("")
        self.content_type.set("")
        self.mile_stone.set("none")
        self.part_number.set("1")
        self.file_format.set("")
        self.version_number.set("next")
        self.allow_overwrite.set(0)
        self.expedite.set(0)
        self.compress.set(0)
        self.chunk.set(0)

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



if __name__=='__main__':
    root = tk.Tk()
    gui = Gui(root)
    gui.mainloop()
