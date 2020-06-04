
import argparse
import datatable as dt
import cov_dates as cd
from math import nan
import dateutil.parser
import os
import pm_util as pmu
from dateutil.parser import parse

def hashBase(record):
    attrs = record['attributes']
    baseString = attrs["Landkreis"]+ attrs["Altersgruppe"]+attrs["Geschlecht"]+str(attrs["Meldedatum"])
    if  attrs["IstErkrankungsbeginn"] != 0:
        baseString = baseString + str(attrs["Refdatum"])
    return baseString

def caseGroupHash(record):
    return hash(hashBase(record))

def cval(tableCell):
    return tableCell.to_list()[0][0]

def cstr(tableCell):
    return str(tableCell.to_list()[0][0])

def cint(tableCell):
    return int(tableCell.to_list()[0][0])

def hashCases(table):
    dss = table[0,"Datenstand"]
    ds = cd.datetimeFromDatenstandAny(dss)

    dsdy = cd.dayFromDate(ds)
    hasRefdatum = "Refdatum" in table.names
    hasErkrankungsbeginn = "Erkrankungsbeginn" in table.names
    t = table
    if not hasRefdatum:
        t = t[:, dt.f[:].extend({"Refdatum": str(cd.day0d)})]
    if not hasErkrankungsbeginn:
        t = t[:, dt.f[:].extend({"IstErkrankungsbeginn": 0})]

    t = t[:, dt.f[:].extend({"FallGruppe":"", "MeldeTag":nan, "RefTag":nan, "DatenstandTag":dsdy})]

    for r in range(t.nrows):
        mds = t[r,"Meldedatum"]
        if pmu.is_int(mds):
            md = cd.datetimeFromStampStr(mds)
        else:
            md = dateutil.parser.isoparse(mds)
            md = md.replace(tzinfo=None)
        mdy = cd.dayFromDate(md)
        t[r,"MeldeTag"] = mdy
        if not hasRefdatum:
            t[r, "Refdatum"] = str(md)
            t[r, "RefTag"] = mdy
        fg = str(t[r,"IdLandkreis"]) + t[r,"Altersgruppe"]+ t[r,"Geschlecht"]+str(int(t[r,"MeldeTag"]))

        if int(t[r,"IstErkrankungsbeginn"]) == 1:
            rds = t[r, "Refdatum"]
            if pmu.is_int(rds):
                rd = cd.datetimeFromStampStr(rds)
            else:
                rd = dateutil.parser.isoparse(rds)
                rd = rd.replace(tzinfo=None)
            rdy = cd.dayFromDate(rd)
            t[r, "RefTag"] = rdy
            fg = fg+":"+str(rdy)
        t[r,dt.f.FallGruppe] = fg
    return t

# much faster than the version above
def hashCases2(table):
    dss = table[0,"Datenstand"]
    ds = cd.datetimeFromDatenstandAny(dss)

    dsdy = cd.dayFromDate(ds)
    hasRefdatum = "Refdatum" in table.names
    hasErkrankungsbeginn = "Erkrankungsbeginn" in table.names
    t = table
    if not hasRefdatum:
        t = t[:, dt.f[:].extend({"Refdatum": str(cd.day0d)})]
    if not hasErkrankungsbeginn:
        t = t[:, dt.f[:].extend({"IstErkrankungsbeginn": 0})]

    t = t[:, dt.f[:].extend({"FallGruppe":"", "MeldeTag":nan, "RefTag":nan, "DatenstandTag":dsdy})]

    d = t.to_dict()

    for r in range(t.nrows):
        mds = d["Meldedatum"][r]
        if pmu.is_int(mds):
            md = cd.datetimeFromStampStr(mds)
        else:
            md = dateutil.parser.parse(mds)
            md = md.replace(tzinfo=None)
        mdy = cd.dayFromDate(md)
        d["MeldeTag"][r] = mdy
        if not hasRefdatum:
            d["Refdatum"][r]= str(md)
            d["RefTag"][r]= mdy

        fg = str(d["IdLandkreis"][r]) + d["Altersgruppe"][r]+ d["Geschlecht"][r]+str(int(d["MeldeTag"][r]))

        if int(d["IstErkrankungsbeginn"][r]) == 1:
            rds = d["Refdatum"][r]
            if pmu.is_int(rds):
                rd = cd.datetimeFromStampStr(rds)
            else:
                rd = dateutil.parser.parse(rds)
                rd = rd.replace(tzinfo=None)
            rdy = cd.dayFromDate(rd)
            d["RefTag"][r] = rdy
            fg = fg+":"+str(rdy)
        d["FallGruppe"][r] = fg
    return dt.Frame(d)


def save(table, origFileName, destDir="."):
    path = os.path.normpath(origFileName)
    fileName = path.split(os.sep)[-1]
    newFile =  destDir+"/X-"+fileName
    print("Saving "+newFile)
    table.to_csv(newFile)

def tableData(dataFilename):
    print("Loading " + dataFilename)
    fullTable = dt.fread(dataFilename)
    print("Loading done loading table from ‘" + dataFilename + "‘, keys:")
    print(fullTable.keys())
    cases = fullTable[(dt.f.NeuerFall == 0) | (dt.f.NeuerFall == 1), 'AnzahlFall'].sum()[0, 0]
    new_cases = fullTable[(dt.f.NeuerFall == -1) | (dt.f.NeuerFall == 1), 'AnzahlFall'].sum()[0, 0]
    dead = fullTable[(dt.f.NeuerTodesfall == 0) | (dt.f.NeuerTodesfall == 1), 'AnzahlTodesfall'].sum()[0, 0]
    new_dead = fullTable[(dt.f.NeuerTodesfall == -1) | (dt.f.NeuerTodesfall == 1), 'AnzahlTodesfall'].sum()[0, 0]

    recovered = 0
    new_recovered = 0
    if "NeuGenesen" in fullTable.keys():
        recovered = fullTable[(dt.f.NeuGenesen == 0) | (dt.f.NeuGenesen == 1), 'AnzahlGenesen'].sum()[0, 0]
        new_recovered = fullTable[(dt.f.NeuGenesen == -1) | (dt.f.NeuGenesen == 1), 'AnzahlGenesen'].sum()[0, 0]
    #lastDay=fullTable[:,'MeldeDay'].max()[0,0]
    #lastnewCaseOnDay=fullTable[:,'newCaseOnDay'].max()[0,0]
    print("{}: cases {} (+{}), dead {} (+{}), recovered {} (+{})".format(
        dataFilename, int(cases), int(new_cases), int(dead), int(new_dead), int(recovered),int(new_recovered)))
    return fullTable

def main():
    parser = argparse.ArgumentParser(description='Extract number from a data file')
    parser.add_argument('files', metavar='fileName', type=str, nargs='+',
                        help='.NPGEO COVID19 Germany data as .csv file')
    parser.add_argument('-d', '--output-dir', dest='outputDir', default=".")

    args = parser.parse_args()
    #print(args)
    for f in args.files:
        t = tableData(f)
        print("Hashing " + f)
        newTable = hashCases2(t)
        save(newTable,f,args.outputDir)


if __name__ == "__main__":
    # execute only if run as a script
    main()