import json
import os
import os.path
import pyodbc
import datetime;


class Price:
    def __init__(self, name, billing_code_type, billing_code, arrangement):
        self.reportingentity = "BCBSVT"
        self.entity_type = "Health Insurance Issuer"
        self.name = name
        self.billing_code_type = billing_code_type
        self.billing_code = billing_code
        self.arrangement = arrangement
        self.npi = ""
        self.tin_type = ""
        self.val = ""
        self.negotiated_type = ""
        self.negotiated_rate = ""
        self.expiration_date = ""
        self.billing_class = ""
        self.service_code = ""
        self.fname = ""
        self.npi_id=""


def ProcessJsonLine(nr, p, fname):
    p.fname = fname
    for i in range(len(nr)): 
        pgs = nr[i]["provider_groups"]
        nps = nr[i]["negotiated_prices"]
        for j in range(len(pgs)):
            p.npi=pgs[j]["npi"]
            p.tin_type=pgs[j]["tin"]["type"]
            p.val=pgs[j]["tin"]["value"]
            p.npi_id = NpiIDCheck(p.val, p.tin_type,p.npi)
            p.negotiated_type=nps[0]["negotiated_type"]
            p.negotiated_rate=nps[0]["negotiated_rate"]
            p.expiration_date=nps[0]["expiration_date"]
            p.service_code=nps[0]["service_code"]
            p.billing_class=nps[0]["billing_class"]
            qry = GetQuery(p)
            ExecuteQuery(qry)

def NpiIDCheck(tin_val, tin_type, npi_group):
    npi_group_str = "|".join(map(str,npi_group))
    q = "select npi_id from NpiGroups where npilist = '" + npi_group_str + "' and tin_val='" + tin_val + "' and tin_type='" + tin_type + "'"
    cur.execute(q)
    res = cur.fetchall()
    npi_id = res[0][0]
    if npi_id == "":
        GetNewNpiID(tin_val, tin_type, npi_group_str)
        npi_id = NpiIDCheck(tin_val, tin_type, npi_group)
    return npi_id


def GetNewNpiID(tin_val, tin_type, npi_group_str):
    q = "insert into NpiGroups (tin_val, tin_type, npilist) values ('" + npi_group_str + ",'" +  tin_val + "','" + tin_type + "'"
    ExecuteQuery(q)



def GetQuery(p):
    qry = ("insert into inet (reportingentity, entity_type, arrangement, billing_class, descript, billing_code_type, " 
             "billing_code, expiration_date, tin_type, tin_val, npi_id, negotiated_type, negotiated_rate, fname, load_date) "
             "values (" + 
                    "'" + p.reportingentity + "'," 
                    "'" + p.entity_type + "'," 
                    "'" + p.arrangement + "'," 
                    "'" + p.billing_class + "', " 
                    "'" + p.name.replace("'", ' ') + "', " 
                    "'" + p.billing_code_type + "', " 
                    "'" + p.billing_code + "', " 
                    "'" + p.expiration_date + "', " 
                    "'" + p.tin_type + "', " 
                    "'" + p.val + "', " 
                    "'" + str(p.npi_id) + "', " 
                    "'" + str(p.negotiated_type) + "', " 
                    "'" + str(p.negotiated_rate) + "', " 
                    "'" + p.fname + "', " 
                    "'" + str(datetime.datetime.now()).split(".")[0] + "')"
                )
    return qry

def ExecuteQuery(qry):
    cur.execute(qry)
    conn.commit()

def CleanUp(jsonSepDir, fname):
    os.remove(jsonSepDir + "\\" + fname)
    os.remove(jsonSepDir + "\\" + "root.json")
    os.rmdir(jsonSepDir)



#main program
conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};SERVER=localhost\\SQLEXPRESS;DATABASE=files;Trusted_Connection=yes;")
cur = conn.cursor()

dir = "D:\\HealthPlanTransparency\\BCBSVT\\JsonFiles"

for jsonFile in os.listdir(dir):
    #split file
    fullpath = dir + "\\" + jsonFile
    jsonSepDir = "D:\\HealthPlanTransparency\\BCBSVT\\JsonFiles\\" + jsonFile.replace(".json", "_json") 
    if not os.path.exists(jsonSepDir):
        if not "in-network" in jsonSepDir: continue
        os.system('cmd /c "jsplit "' + fullpath + '"')
        #os.system('cmd /c "jsplit "' + fullpath + '" ')
        for fname in os.listdir(jsonSepDir):
            if fname != "root.json":
                f = open(jsonSepDir + "\\" + fname, 'r')
                count = 0
                while True:
                    count += 1
                    l = f.readline()
                    if not l:
                        break
                    j = json.loads(l)
                    nr = j["negotiated_rates"]
                    p = Price(j["name"], j["billing_code_type"],j["billing_code"], j["negotiation_arrangement"])
                    p = ProcessJsonLine(nr,p, jsonFile)
                f.close()
        CleanUp(jsonSepDir, fname)
conn.close()



