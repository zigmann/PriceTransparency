import pyodbc


f = open("D:\\HealthPlanTransparency\\npis.csv","r")
next(f)

conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};SERVER=localhost\\SQLEXPRESS;DATABASE=files;Trusted_Connection=yes;")
cur = conn.cursor()

count=0
while True:
    count += 1
    l = f.readline()
    if not l:
        break
    s = l.split(",")
    npi = s[0].replace('"','')
    eType = s[1].replace('"','')
    ein = s[3].replace('"','')
    org = s[4].replace('"','').replace("'","")
    lname = s[5].replace('"','').replace("'","")
    fname = s[7].replace('"','').replace("'","")
    orgAddr = s[20].replace('"','').replace("'","")
    fTin = ein[:2] + "-" + ein[2:]
    parentOrgTin = s[310]
    if fTin == '-':
        fTin = ''

    if org != '' or (fname != '' and lname != ''):
        qry = ("insert into NPIs (npi, eType, TIN, Org, fTin, lname, fname, orgAddr) values ('" +
             npi + "','" + eType + "','" + ein + "','" + org + "','" + fTin + "','" + lname + "','" + fname + "','" + orgAddr + "')")
                
        cur.execute(qry)
        conn.commit()

f.close()
conn.close()