import pyodbc


#f = open("D:\\HealthPlanTransparency\\NE_TaxExempt_EINs.txt","r") #North East
#f = open("D:\\HealthPlanTransparency\\MW_TaxExemptEINs.txt","r") #Midwest
#f = open("D:\\HealthPlanTransparency\\GCP_TaxExemptEINs.txt","r") #Gulf-Pacific
f = open("D:\\HealthPlanTransparency\\AO_TaxExemptEINs.txt","r") #All other

next(f)

conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};SERVER=localhost\\SQLEXPRESS;DATABASE=files;Trusted_Connection=yes;")
cur = conn.cursor()

count = 0
while True:
    count += 1
    l = f.readline()
    if not l:
        break
    s = l.split(",")
    tin = s[0]
    org = s[1]
    fTin = tin[:2] + "-" + tin[2:]
    st = s[5]
    grp = s[7]
    subsection = s[8]
    affil = s[9]
    clss= s[10]
    foundation = s[13]
    organiation = s[15]

    qry = ("insert into TaxExemptEINs (TIN, Org, fTIN, st, grp, subsection, affil, clss, foundation, organization) values (" +
            "'" + tin + "','" + org + "','" + fTin + "', '" + st + "', '" + grp + "', '" + subsection + "', '" + affil + "', '" + clss + "', '" + foundation + "', '" + organiation + "')")
    cur.execute(qry)
    conn.commit()

f.close()
conn.close()
