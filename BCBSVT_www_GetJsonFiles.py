from bs4 import BeautifulSoup
import asyncio
import requests
import json
from datetime import datetime
import gzip

#GLOBALS
mrfs_url = 'https://mrf-api.changehealthcare.com/list?payer=bcbsvt'
downloadPath = "D:\\HealthPlanTransparency\\BCBSVT\\JsonFiles"
fnames = []

class Fname:
    def __init__(self, key):
        self.key = key
        self.fname = ""
        self.fid=""
        self.LastModifiedLong=""
        self.LastModifiedDate=""
        self.LastModified=""
        self.Etag = ""
        self.Size = ""
        self.ownerdisplay = ""
        self.ownerid = ""
        self.findex = ""

def FileDateCheck(f):
    if any(x.fid == f.fid for x in fnames):
        indx = [x.fid for x in fnames].index(f.fid)
        newDate = f.LastModified
        oldDate = fnames[indx].LastModified
        if newDate >= oldDate:
            fnames.pop(indx)
            return True
        else:
            return False
    else:
        return True

def CreateFileIndex(f):
    if "index" in f.fname:
        f.fid = f.fname.split("_")[2] + "_index"
    else:
        f.fid = f.fname.split("_")[2] + "_" + f.fname.split("_")[5].replace(".json","").replace(".gz", "")
    return f

def DownloadFile(f):
    baseUrl = "https://mrf-api.changehealthcare.com/downloadUrl?fileName="
    u = baseUrl + f.fname + "&payer=bcbsvt"
    fn = f.fname.replace(".gz","")
    if "json.gz" in f.fname:
        d = requests.get(u)
        o = requests.get(d.content).content
        o1 = gzip.decompress(o)
        output = open(downloadPath + "\\" + fn, "w")
        output.write(o1.decode("utf-8"))
        output.close()
    else:
        d = requests.get(u)
        o = requests.get(d.content).content
        #OutputFile(o, fn)
        output = open(downloadPath + "\\" + fn, "w")
        output.write(o.decode("utf-8"))
        output.close()

        #json_data = json.loads(requests.get(d.content).content)
    return 1

def OutputFile(o, fn):
    output = open(downloadPath + "\\" + fn, "w")
    output.write(o.decode("utf-8"))
    output.close()


def GetMostRecentFiles():
    r = requests.get(mrfs_url)
    j = json.loads(r.content)
    for i in range(len(j)):
        key = j[i]["Key"]
        if(".json" in key):
            f = Fname(key)
            f.fname = key.replace("bcbsvt/","")
            f.LastModifiedLong = j[i]["LastModified"]
            d = f.LastModifiedLong.split("T")[0]
            f.LastModified = d.replace("-","")
            f.LastModifiedDate = datetime.strptime(f.LastModifiedLong.split("T")[0], '%Y-%m-%d')
            f.Etag = j[i]["ETag"]
            f.Size = j[i]["Size"]
            f.ownerdisplay = j[i]["Owner"]["DisplayName"]
            f.ownerid = j[i]["Owner"]["ID"]
            f = CreateFileIndex(f) #bcbsvt specific
            #TODO
            #find index of fid and compare the last modified date
            #remove if current LastModified is more recent than 
            #old LastModified
            if FileDateCheck(f):
                fnames.append(f)

#fnames.sort(key=lambda x: x.LastModified, reverse=True)
GetMostRecentFiles()
res = list(map(DownloadFile, fnames))
print("stop")

