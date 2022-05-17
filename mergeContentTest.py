import requests
from fastapi import File
from mmcif.io.PdbxReader import PdbxReader
from mmcif.io.PdbxWriter import PdbxWriter

import gzip


# pdbIDScript = "1yy9"
# filePath = "/Users/cparker/RCSBWork/py-rcsb_app_file/mmcifData/" + pdbIDScript + "_sifts.cif"
# url = "http://0.0.0.0/file-v1/merge"

# mergeDict = {
#     "pdbID": "1yy9"
# }

# sf = gzip.open("/Users/cparker/RCSBWork/py-rcsb_app_file/rcsb/app/tests-file/test-data/mmcif/" + pdbIDScript + "_sifts_only.cif.gz", "rb")
# siftFile = sf.read()
# f = open("/Users/cparker/RCSBWork/py-rcsb_app_file/mmcifData/" + pdbIDScript + "_sifts.cif", "wb")
# f.write(siftFile)
# f.close()

# ifh = open(filePath, "rb")
# files = {"siftsFile": ifh}
# r = requests.post("http://0.0.0.0/file-v1/merge", files=files, data=mergeDict)
# print(r.text)

# with TestClient(app) as client:
#     with open(filePath, "r") as ifh:
#         files = {"uploadFile": ifh}
#         response = client.post("/file-v1/merge", files=files, data=mergeDict)

# print(r.text)
# files = {"uploadFile": open(filePath, "r")}
# response = requests.post(url, files=files, data=mergeDict)


class mergeUpload():

    def merge(
        self,
        siftsFile: bytes = File(...),
        pdbID: str = None
    ):
        cachePath = "/Users/cparker/RCSBWork/py-rcsb_app_file/rcsb/app/tests-file/test-data/mmcif/"
        pdbIDHash = pdbID[1:3]

        cifUrl = "https://ftp.wwpdb.org/pub/pdb/data/structures/divided/mmCIF/" + pdbIDHash + "/" + pdbID + ".cif.gz"
        cifPath = cachePath + pdbID + ".cif"
        cifTempPath = cachePath + pdbID + "_temp.cif.gz"

        ofh = open(cifTempPath, "wb")
        response = requests.get(cifUrl)
        ofh.write(response.content)
        ofh.close()

        ifh = gzip.open(cifTempPath, "rb")
        unzipMMCIF = ifh.read()
        ofh = open(cifPath, "wb")
        ofh.write(unzipMMCIF)
        ofh.close()

        siftsList = []
        siftsRead = PdbxReader(siftsFile)
        siftsRead.read(siftsList)

        cifList = []
        with open(cachePath + pdbID + ".cif", "r") as ifh:
            cifRead = PdbxReader(ifh)
            cifRead.read(cifList)

        siftsCatNames = siftsList[0].getObjNameList()
        
        siftsAtomSite = siftsList[0].getObj("atom_site")
        siftsCatNames.pop(0)

        siftsAttributes = siftsAtomSite.getAttributeList()

        for i in siftsAttributes:
            cifList[0].getObj("atom_site").appendAttributeExtendRows(i)

        for i in siftsList[0].getObj("atom_site").getAttributeList():
            j = 0
            while j < len(siftsList[0].getObj("atom_site").getAttributeValueList(i)):
                cifList[0].getObj("atom_site").setValue(siftsList[0].getObj("atom_site").getAttributeValueList(i)[j], i, j)
                # print(i, ":", siftsList[0].getObj("atom_site").getAttributeValueList(i)[j])
                j += 1
        
        # for i in siftsCatNames:
        #     tempObj = siftsList[0].getObj(i)
        #     cifList[0].append(tempObj)

        with open(cachePath + pdbID + "_merged.cif", "w") as ofh:
            pdbxW = PdbxWriter(ofh)
            pdbxW.write(cifList)


pdbIDScript = "1yy9"

mergeDict = {
    "pdbID": pdbIDScript
}

sf = gzip.open("/Users/cparker/RCSBWork/py-rcsb_app_file/rcsb/app/tests-file/test-data/mmcif/" + pdbIDScript + "_sifts_only.cif.gz", "rb")
siftFile = sf.read()
f = open("/Users/cparker/RCSBWork/py-rcsb_app_file/rcsb/app/tests-file/test-data/mmcif/" + pdbIDScript + "_sifts.cif", "wb")
f.write(siftFile)
f.close()

with open("/Users/cparker/RCSBWork/py-rcsb_app_file/rcsb/app/tests-file/test-data/mmcif/" + pdbIDScript + "_sifts.cif", "r") as f:
    functionCall = mergeUpload()
    functionCall.merge(f, pdbIDScript)
