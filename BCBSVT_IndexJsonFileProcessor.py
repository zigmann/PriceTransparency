import os
import pyodbc
import json

class Plan:
    def __init__(self, plan_name):
        self.plan_name=plan_name
        self.plan_id_type=""
        self.plan_id =""
        self.plan_market_type=""
        self.fplan_id=""



def ReadIndex(fn):
    if fn != "root.json":
        f = open(jsonSepDir + "\\" + fn, "r")
        count=0
        while True:
            count += 1
            l = f.readline()
            if not l:
                break
            j = json.loads(l)
            p = ProcessFileLine(j)
        f.close()
        CleanUp(jsonSepDir, fn)

def ProcessFileLine(j):
    voi = j["reporting_plans"]
    for i in range(len(voi)):
        p = Plan(voi[i]["plan_name"])
        p.plan_id_type= voi[i]["plan_id_type"]
        p.plan_id= voi[i]["plan_id"]
        p.plan_market_type= voi[i]["plan_market_type"]
        p.fplan_id = p.plan_id[:2] + "-" + p.plan_id[2:]

        qry = GetQuery(p)
        ExecuteQuery(qry)

def ExecuteQuery(qry):
    cur.execute(qry)
    conn.commit()

def GetQuery(p):
    q = ("insert into indices (plan_name, plan_id_type, plan_id, fplan_id, plan_market_type) values (" +
           "'" + p.plan_name.replace("'","") + "', " +
           "'" + p.plan_id_type + "', " + 
           "'" + p.plan_id + "', " + 
           "'" + p.fplan_id + "', " +
           "'" + p.plan_market_type + "')"  
            )
    return q

def CleanUp(jsonSepDir, fname):
    os.remove(jsonSepDir + "\\" + fname)
    os.remove(jsonSepDir + "\\" + "root.json")
    os.rmdir(jsonSepDir)

conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};SERVER=localhost\\SQLEXPRESS;DATABASE=files;Trusted_Connection=yes;")
cur = conn.cursor()

dir = "D:\\HealthPlanTransparency\\BCBSVT\\JsonFiles"
fn = str([i for i in os.listdir(dir) if "index" in i]).replace("['","").replace("']","")
fullpath = dir + "\\" + fn
os.system('cmd /c "jsplit "' + fullpath + '"')

jsonSepDir = "D:\\HealthPlanTransparency\\BCBSVT\\JsonFiles\\" + fn.replace(".json", "_json")
dList = os.listdir(jsonSepDir)

res = list(map(ReadIndex, dList))
conn.close()

