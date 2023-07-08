##
# File:    Definitions.py
# Author:  dwp
# Date:    16-Feb-2023
# Version: 0.001
#
# Updates:
#
"""
Assorted definitions used by other application components.
"""

__docformat__ = "google en"
__author__ = "Dennis Piehl"
__email__ = "dennis.piehl@rcsb.org"
__license__ = "Apache 2.0"


class Definitions(object):
    def __init__(self):
        self.contentTypeD = self.getContentTypeD()
        self.fileFormatExtD = self.getFileFormatExtD()
        self.milestoneList = self.getMilestoneList()
        self.repoTypeList = self.getRepoTypeList()

    def getMilestoneList(self):
        return ['upload', 'upload-convert', 'deposit', 'annotate', 'release', 'review', '', None]

    def getRepoTypeList(self):
        return ['deposit', 'archive', 'workflow', 'session', 'onedep-deposit', 'onedep-archive', 'onedep-workflow', 'onedep-session', 'test', 'tests', 'unit-test', 'unit-tests']

    def getContentTypeD(self):
        contentTypeD = {
            "model": [
                [
                    "pdbx",
                    "pdb",
                    "pdbml",
                    "cifeps"
                ],
                "model"
            ],
            "model-emd": [
                [
                    "pdbx",
                    "xml"
                ],
                "model-emd"
            ],
            "model-aux": [
                [
                    "pdbx"
                ],
                "model-aux"
            ],
            "model-legacy-rcsb": [
                [
                    "pdbx",
                    "pdb"
                ],
                "model-legacy-rcsb"
            ],
            "structure-factors": [
                [
                    "pdbx",
                    "mtz",
                    "txt"
                ],
                "sf"
            ],
            "structure-factors-legacy-rcsb": [
                [
                    "pdbx",
                    "mtz"
                ],
                "sf-legacy-rcsb"
            ],
            "nmr-data-config": [
                [
                    "json"
                ],
                "nmr-data-config"
            ],
            "nmr-data-nef": [
                [
                    "nmr-star",
                    "pdbx"
                ],
                "nmr-data-nef"
            ],
            "nmr-data-str": [
                [
                    "nmr-star",
                    "pdbx"
                ],
                "nmr-data-str"
            ],
            "nmr-data-nef-report": [
                [
                    "json"
                ],
                "nmr-data-nef-report"
            ],
            "nmr-data-str-report": [
                [
                    "json"
                ],
                "nmr-data-str-report"
            ],
            "nmr-restraints": [
                [
                    "any",
                    "nmr-star",
                    "amber",
                    "amber-aux",
                    "cns",
                    "cyana",
                    "xplor",
                    "xplor-nih",
                    "pdb-mr",
                    "mr"
                ],
                "mr"
            ],
            "nmr-chemical-shifts": [
                [
                    "nmr-star",
                    "pdbx",
                    "any"
                ],
                "cs"
            ],
            "nmr-chemical-shifts-raw": [
                [
                    "nmr-star",
                    "pdbx"
                ],
                "cs-raw"
            ],
            "nmr-chemical-shifts-auth": [
                [
                    "nmr-star",
                    "pdbx"
                ],
                "cs-auth"
            ],
            "nmr-chemical-shifts-upload-report": [
                [
                    "pdbx"
                ],
                "nmr-chemical-shifts-upload-report"
            ],
            "nmr-chemical-shifts-atom-name-report": [
                [
                    "pdbx"
                ],
                "nmr-chemical-shifts-atom-name-report"
            ],
            "nmr-shift-error-report": [
                [
                    "json"
                ],
                "nmr-shift-error-report"
            ],
            "nmr-bmrb-entry": [
                [
                    "nmr-star",
                    "pdbx"
                ],
                "nmr-bmrb-entry"
            ],
            "nmr-harvest-file": [
                [
                    "tgz"
                ],
                "nmr-harvest-file"
            ],
            "nmr-peaks": [
                [
                    "any"
                ],
                "nmr-peaks"
            ],
            "nmr-nef": [
                [
                    "nmr-star",
                    "pdbx"
                ],
                "nmr-nef"
            ],
            "nmr-cs-check-report": [
                [
                    "html"
                ],
                "nmr-cs-check-report"
            ],
            "nmr-cs-xyz-check-report": [
                [
                    "html"
                ],
                "nmr-cs-xyz-check-report"
            ],
            "nmr-cs-path-list": [
                [
                    "txt"
                ],
                "nmr-cs-path-list"
            ],
            "nmr-cs-auth-file-name-list": [
                [
                    "txt"
                ],
                "nmr-cs-auth-file-name-list"
            ],
            "nmr-mr-path-list": [
                [
                    "json"
                ],
                "nmr-mr-path-list"
            ],
            "component-image": [
                [
                    "jpg",
                    "png",
                    "gif",
                    "svg",
                    "tif",
                    "tiff"
                ],
                "ccimg"
            ],
            "component-definition": [
                [
                    "pdbx",
                    "sdf"
                ],
                "ccdef"
            ],
            "em-volume": [
                [
                    "map",
                    "ccp4",
                    "mrc2000",
                    "bcif"
                ],
                "em-volume"
            ],
            "em-mask-volume": [
                [
                    "map",
                    "ccp4",
                    "mrc2000",
                    "bcif"
                ],
                "em-mask-volume"
            ],
            "em-additional-volume": [
                [
                    "map",
                    "ccp4",
                    "mrc2000",
                    "bcif"
                ],
                "em-additional-volume"
            ],
            "em-half-volume": [
                [
                    "map",
                    "ccp4",
                    "mrc2000",
                    "bcif"
                ],
                "em-half-volume"
            ],
            "em-raw-volume": [
                [
                    "map",
                    "ccp4",
                    "mrc2000",
                    "bcif"
                ],
                "em-raw-volume"
            ],
            "em-fsc-half-mask-volume": [
                [
                    "map",
                    "ccp4",
                    "mrc2000",
                    "bcif"
                ],
                "em-fsc-half-mask-volume"
            ],
            "em-fsc-map-model-mask-volume": [
                [
                    "map",
                    "ccp4",
                    "mrc2000",
                    "bcif"
                ],
                "em-fsc-map-model-mask-volume"
            ],
            "em-alignment-mask-volume": [
                [
                    "map",
                    "ccp4",
                    "mrc2000",
                    "bcif"
                ],
                "em-alignment-mask-volume"
            ],
            "em-focused-refinement-mask-volume": [
                [
                    "map",
                    "ccp4",
                    "mrc2000",
                    "bcif"
                ],
                "em-focused-refinement-mask-volume"
            ],
            "em-3d-classification-additional-volume": [
                [
                    "map",
                    "ccp4",
                    "mrc2000",
                    "bcif"
                ],
                "em-3d-classification-additional-volume"
            ],
            "em-focus-refinement-additional-volume": [
                [
                    "map",
                    "ccp4",
                    "mrc2000",
                    "bcif"
                ],
                "em-focus-refinement-additional-volume"
            ],
            "em-segmentation-volume": [
                [
                    "map",
                    "ccp4",
                    "mrc2000",
                    "bcif"
                ],
                "em-segmentation-volume"
            ],
            "em-volume-wfcfg": [
                [
                    "json"
                ],
                "em-volume-wfcfg"
            ],
            "em-mask-volume-wfcfg": [
                [
                    "json"
                ],
                "em-mask-volume-wfcfg"
            ],
            "em-additional-volume-wfcfg": [
                [
                    "json"
                ],
                "em-additional-volume-wfcfg"
            ],
            "em-half-volume-wfcfg": [
                [
                    "json"
                ],
                "em-half-volume-wfcfg"
            ],
            "em-volume-report": [
                [
                    "json"
                ],
                "em-volume-report"
            ],
            "em-volume-header": [
                [
                    "xml"
                ],
                "em-volume-header"
            ],
            "em-model-emd": [
                [
                    "pdbx"
                ],
                "em-model-emd"
            ],
            "em-structure-factors": [
                [
                    "pdbx",
                    "mtz"
                ],
                "em-sf"
            ],
            "emd-xml-header-report": [
                [
                    "txt"
                ],
                "emd-xml-header-report"
            ],
            "validation-report-depositor": [
                [
                    "pdf"
                ],
                "valdep"
            ],
            "seqdb-match": [
                [
                    "pdbx",
                    "pic"
                ],
                "seqdb-match"
            ],
            "blast-match": [
                [
                    "xml"
                ],
                "blast-match"
            ],
            "seq-assign": [
                [
                    "pdbx"
                ],
                "seq-assign"
            ],
            "partial-seq-annotate": [
                [
                    "txt"
                ],
                "partial-seq-annotate"
            ],
            "seq-data-stats": [
                [
                    "pic"
                ],
                "seq-data-stats"
            ],
            "seq-align-data": [
                [
                    "pic"
                ],
                "seq-align-data"
            ],
            "pre-seq-align-data": [
                [
                    "pic"
                ],
                "pre-seq-align-data"
            ],
            "seqmatch": [
                [
                    "pdbx"
                ],
                "seqmatch"
            ],
            "mismatch-warning": [
                [
                    "pic"
                ],
                "mismatch-warning"
            ],
            "polymer-linkage-distances": [
                [
                    "pdbx",
                    "json"
                ],
                "poly-link-dist"
            ],
            "polymer-linkage-report": [
                [
                    "html"
                ],
                "poly-link-report"
            ],
            "geometry-check-report": [
                [
                    "pdbx"
                ],
                "geometry-check-report"
            ],
            "chem-comp-link": [
                [
                    "pdbx"
                ],
                "cc-link"
            ],
            "chem-comp-assign": [
                [
                    "pdbx"
                ],
                "cc-assign"
            ],
            "chem-comp-assign-final": [
                [
                    "pdbx"
                ],
                "cc-assign-final"
            ],
            "chem-comp-assign-details": [
                [
                    "pic"
                ],
                "cc-assign-details"
            ],
            "chem-comp-depositor-info": [
                [
                    "pdbx"
                ],
                "cc-dpstr-info"
            ],
            "prd-search": [
                [
                    "pdbx"
                ],
                "prd-summary"
            ],
            "assembly-report": [
                [
                    "txt",
                    "xml"
                ],
                "assembly-report"
            ],
            "assembly-assign": [
                [
                    "pdbx",
                    "txt"
                ],
                "assembly-assign"
            ],
            "assembly-depinfo-update": [
                [
                    "txt"
                ],
                "assembly-depinfo-update"
            ],
            "interface-assign": [
                [
                    "xml"
                ],
                "interface-assign"
            ],
            "assembly-model": [
                [
                    "pdb",
                    "pdbx"
                ],
                "assembly-model"
            ],
            "assembly-model-xyz": [
                [
                    "pdb",
                    "pdbx"
                ],
                "assembly-model-xyz"
            ],
            "site-assign": [
                [
                    "pdbx"
                ],
                "site-assign"
            ],
            "dict-check-report": [
                [
                    "txt"
                ],
                "dict-check-report"
            ],
            "dict-check-report-r4": [
                [
                    "txt"
                ],
                "dict-check-report-r4"
            ],
            "dict-check-report-next": [
                [
                    "txt"
                ],
                "dict-check-report-next"
            ],
            "tom-complex-report": [
                [
                    "txt"
                ],
                "tom-upload-report"
            ],
            "tom-merge-report": [
                [
                    "txt"
                ],
                "tom-merge-report"
            ],
            "format-check-report": [
                [
                    "txt"
                ],
                "format-check-report"
            ],
            "misc-check-report": [
                [
                    "txt"
                ],
                "misc-check-report"
            ],
            "special-position-report": [
                [
                    "txt"
                ],
                "special-position-report"
            ],
            "merge-xyz-report": [
                [
                    "txt"
                ],
                "merge-xyz-report"
            ],
            "model-issues-report": [
                [
                    "json"
                ],
                "model-issues-report"
            ],
            "structure-factor-report": [
                [
                    "json"
                ],
                "structure-factor-report"
            ],
            "validation-report": [
                [
                    "pdf"
                ],
                "val-report"
            ],
            "validation-report-full": [
                [
                    "pdf"
                ],
                "val-report-full"
            ],
            "validation-report-slider": [
                [
                    "png",
                    "svg"
                ],
                "val-report-slider"
            ],
            "validation-data": [
                [
                    "pdbx",
                    "xml"
                ],
                "val-data"
            ],
            "validation-report-2fo-map-coef": [
                [
                    "pdbx"
                ],
                "val-report-wwpdb-2fo-fc-edmap-coef"
            ],
            "validation-report-fo-map-coef": [
                [
                    "pdbx"
                ],
                "val-report-wwpdb-fo-fc-edmap-coef"
            ],
            "validation-report-images": [
                [
                    "tar"
                ],
                "val-report-images"
            ],
            "map-xray": [
                [
                    "bcif"
                ],
                "map-xray"
            ],
            "map-2fofc": [
                [
                    "map"
                ],
                "map-2fofc"
            ],
            "map-fofc": [
                [
                    "map"
                ],
                "map-fofc"
            ],
            "map-omit-2fofc": [
                [
                    "map"
                ],
                "map-omit-2fofc"
            ],
            "map-omit-fofc": [
                [
                    "map"
                ],
                "map-omit-fofc"
            ],
            "sf-convert-report": [
                [
                    "pdbx",
                    "txt"
                ],
                "sf-convert-report"
            ],
            "em-sf-convert-report": [
                [
                    "pdbx",
                    "txt"
                ],
                "em-sf-convert-report"
            ],
            "dcc-report": [
                [
                    "pdbx",
                    "txt"
                ],
                "dcc-report"
            ],
            "mapfix-header-report": [
                [
                    "json"
                ],
                "mapfix-header-report"
            ],
            "mapfix-report": [
                [
                    "txt"
                ],
                "mapfix-report"
            ],
            "secondary-structure-topology": [
                [
                    "txt"
                ],
                "ss-topology"
            ],
            "sequence-fasta": [
                [
                    "fasta",
                    "fsa"
                ],
                "fasta"
            ],
            "messages-from-depositor": [
                [
                    "pdbx"
                ],
                "messages-from-depositor"
            ],
            "messages-to-depositor": [
                [
                    "pdbx"
                ],
                "messages-to-depositor"
            ],
            "notes-from-annotator": [
                [
                    "pdbx"
                ],
                "notes-from-annotator"
            ],
            "correspondence-to-depositor": [
                [
                    "txt"
                ],
                "correspondence-to-depositor"
            ],
            "correspondence-legacy-rcsb": [
                [
                    "pdbx"
                ],
                "correspondence-legacy-rcsb"
            ],
            "correspondence-info": [
                [
                    "pdbx"
                ],
                "correspondence-info"
            ],
            "map-header-data": [
                [
                    "json",
                    "pic",
                    "txt"
                ],
                "map-header-data"
            ],
            "deposit-volume-params": [
                [
                    "pic"
                ],
                "deposit-volume-params"
            ],
            "fsc": [
                [
                    "xml"
                ],
                "fsc-xml"
            ],
            "fsc-report": [
                [
                    "txt"
                ],
                "fsc-report"
            ],
            "res-est-fsc": [
                [
                    "xml"
                ],
                "res-est-fsc"
            ],
            "res-est-fsc-report": [
                [
                    "txt"
                ],
                "res-est-fsc-report"
            ],
            "map-model-fsc": [
                [
                    "xml"
                ],
                "map-model-fsc"
            ],
            "map-model-fsc-report": [
                [
                    "txt"
                ],
                "map-model-fsc-report"
            ],
            "em2em-report": [
                [
                    "txt"
                ],
                "em2em-report"
            ],
            "img-emdb": [
                [
                    "jpg",
                    "png",
                    "gif",
                    "svg",
                    "tif"
                ],
                "img-emdb"
            ],
            "img-emdb-report": [
                [
                    "txt"
                ],
                "img-emdb-report"
            ],
            "layer-lines": [
                [
                    "txt"
                ],
                "layer-lines"
            ],
            "auxiliary-file": [
                [
                    "any"
                ],
                "aux-file"
            ],
            "status-history": [
                [
                    "pdbx"
                ],
                "status-history"
            ],
            "virus-matrix": [
                [
                    "any"
                ],
                "virus"
            ],
            "parameter-file": [
                [
                    "any"
                ],
                "parm"
            ],
            "structure-def-file": [
                [
                    "any"
                ],
                "struct"
            ],
            "topology-file": [
                [
                    "any"
                ],
                "topo"
            ],
            "cmd-line-args": [
                [
                    "txt"
                ],
                "cmd-line-args"
            ],
            "sd-dat": [
                [
                    "a",
                    "n",
                    "y"
                ],
                "sd-dat"
            ],
            "sx-pr": [
                [
                    "a",
                    "n",
                    "y"
                ],
                "sx-pr"
            ],
            "sm-fit": [
                [
                    "a",
                    "n",
                    "y"
                ],
                "sm-fit"
            ],
            "deposition-info": [
                [
                    "pdbx",
                    "json"
                ],
                "deposition-info"
            ],
            "deposition-store": [
                [
                    "tar"
                ],
                "deposition-store"
            ],
            "bundle-session-archive": [
                [
                    "tar",
                    "tgz"
                ],
                "bundle-session-archive"
            ],
            "bundle-session-deposit": [
                [
                    "tar",
                    "tgz"
                ],
                "bundle-session-deposit"
            ],
            "bundle-session-upload": [
                [
                    "tar",
                    "tgz"
                ],
                "bundle-session-upload"
            ],
            "bundle-session-tempdep": [
                [
                    "tar",
                    "tgz"
                ],
                "bundle-session-tempdep"
            ],
            "bundle-session-uitemp": [
                [
                    "tar",
                    "tgz"
                ],
                "bundle-session-uitemp"
            ],
            "bundle-session-workflow": [
                [
                    "tar",
                    "tgz"
                ],
                "bundle-session-workflow"
            ],
            "session-backup": [
                [
                    "tar",
                    "tgz"
                ],
                "bundle-session-workflow"
            ],
            "manifest-session": [
                [
                    "json"
                ],
                "manifest-session"
            ],
            "manifest-session-bundle": [
                [
                    "json"
                ],
                "manifest-session-bundle"
            ],
            "any": [
                [
                    "any"
                ],
                "any"
            ]
        }

        return contentTypeD

    def getFileFormatExtD(self):
        fileFormatExtD = {
            "pdbx": "cif",
            "pdb": "pdb",
            "cifeps": "cifeps",
            "pdbml": "xml",
            "nmr-star": "str",
            "gz": "gz",
            "tgz": "tgz",
            "mtz": "mtz",
            "html": "html",
            "jpg": "jpg",
            "png": "png",
            "svg": "svg",
            "gif": "gif",
            "tif": "tif",
            "tiff": "tiff",
            "sdf": "sdf",
            "ccp4": "ccp4",
            "mrc2000": "mrc",
            "pic": "pic",
            "txt": "txt",
            "xml": "xml",
            "pdf": "pdf",
            "map": "map",
            "bcif": "bcif",
            "amber": "amber",
            "amber-aux": "amber-aux",
            "cns": "cns",
            "cyana": "cyana",
            "xplor": "xplor",
            "xplor-nih": "xplor-nih",
            "pdb-mr": "mr",
            "mr": "mr",
            "json": "json",
            "fsa": "fsa",
            "fasta": "fasta",
            "any": "dat",
            "mdl": "mdl",
            "tar": "tar",
        }

        return fileFormatExtD
