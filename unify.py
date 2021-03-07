
import argparse
import datatable as dt
import cov_dates as cd
from math import nan
import dateutil.parser
import os
import pm_util as pmu
import csv
import sys
import glob
csv.field_size_limit(sys.maxsize)

# cd ~/Corona/archive_ard/
# python ~/Corona/unify.py -d ~/Corona/archive_v2 ~/Corona/archive_ard/NPGEO-RKI-*.csv

from dateutil.parser import parse

# def hashBase(record):
#     attrs = record['attributes']
#     baseString = attrs["Landkreis"]+ attrs["Altersgruppe"]+attrs["Geschlecht"]+str(attrs["Meldedatum"])
#     if  attrs["IstErkrankungsbeginn"] != 0:
#         baseString = baseString + str(attrs["Refdatum"])
#     return baseString
#
# def caseGroupHash(record):
#     return hash(hashBase(record))
#
# def cval(tableCell):
#     return tableCell.to_list()[0][0]
#
# def cstr(tableCell):
#     return str(tableCell.to_list()[0][0])
#
# def cint(tableCell):
#     return int(tableCell.to_list()[0][0])
#
# def hashCases(table):
#     dss = table[0,"Datenstand"]
#     ds = cd.datetimeFromDatenstandAny(dss)
#
#     dsdy = cd.dayFromDate(ds)
#     hasRefdatum = "Refdatum" in table.names
#     hasErkrankungsbeginn = "Erkrankungsbeginn" in table.names
#     t = table
#     if not hasRefdatum:
#         t = t[:, dt.f[:].extend({"Refdatum": str(cd.day0d)})]
#     if not hasErkrankungsbeginn:
#         t = t[:, dt.f[:].extend({"IstErkrankungsbeginn": 0})]
#
#     t = t[:, dt.f[:].extend({"FallGruppe":"", "MeldeTag":nan, "RefTag":nan, "DatenstandTag":dsdy})]
#
#     for r in range(t.nrows):
#         mds = t[r,"Meldedatum"]
#         if pmu.is_int(mds):
#             md = cd.datetimeFromStampStr(mds)
#         else:
#             md = dateutil.parser.isoparse(mds)
#             md = md.replace(tzinfo=None)
#         mdy = cd.dayFromDate(md)
#         t[r,"MeldeTag"] = mdy
#         if not hasRefdatum:
#             t[r, "Refdatum"] = str(md)
#             t[r, "RefTag"] = mdy
#         fg = str(t[r,"IdLandkreis"]) + t[r,"Altersgruppe"]+ t[r,"Geschlecht"]+str(int(t[r,"MeldeTag"]))
#
#         if int(t[r,"IstErkrankungsbeginn"]) == 1:
#             rds = t[r, "Refdatum"]
#             if pmu.is_int(rds):
#                 rd = cd.datetimeFromStampStr(rds)
#             else:
#                 rd = dateutil.parser.isoparse(rds)
#                 rd = rd.replace(tzinfo=None)
#             rdy = cd.dayFromDate(rd)
#             t[r, "RefTag"] = rdy
#             fg = fg+":"+str(rdy)
#         t[r,dt.f.FallGruppe] = fg
#     return t


def loadCensus(fileName="../CensusByRKIAgeGroups.csv"):
    census = dt.fread(fileName)
    sKeys = census[:, "IdLandkreis"].to_list()[0]
    values = census[:, "Landkreis"].to_list()[0]
    valuesDict = dict(zip(sKeys, values))
    #print(valuesDict)
    return valuesDict

# def loadLandkreisBeveolkerung(fileName="../Landkreise-Bevoelkerung.csv"):
#     result = {}
#     with open(fileName, newline='') as csvfile:
#         reader = csv.DictReader(csvfile, delimiter=';')
#         for i, row in enumerate(reader):
#             #print(row)
#             LandkreisID = int(row['LandkreisID'])
#             BevoelkerungStr = row['Bevoelkerung']
#             if BevoelkerungStr != "-":
#                 result[LandkreisID] = int(BevoelkerungStr)
#     return result

def loadLandkreisFlaeche(fileName="../covid-19-germany-landkreise.csv"):
    result = {}
    with open(fileName, newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        for i, row in enumerate(reader):
            #print(row)
            LandkreisID = int(row['Regional code'])
            FlaecheStr = row['Cadastral area']
            if FlaecheStr != "":
                #print(FlaecheStr)
                result[LandkreisID] = float(FlaecheStr)
    #print("Flaeche:", result)
    return result

def checkLandkreisData(data, row, Census, Flaeche):
    missingIds = []
    missingNames = []
    Landkreis = data["Landkreis"][row]
    IdLandkreis = int(data["IdLandkreis"][row])
    if IdLandkreis == 3152:
        # change old Landkreis ID for Göttingen to new Id
        IdLandkreis = 3159
        data["IdLandkreis"][row] = 3159
        print("#Info: Changed bad Göttingen Landkreis Id from 3152 to 3159")
    if Landkreis == "LK Aachen" or IdLandkreis == 5354:
        # change bad Landkreis for Aachen to Stadtregion
        print("#Info: Bad record in row:",row)
        print("#Info: Changing bad '{}' Kreis with Id {} to ‘StadtRegion Aachen‘ id 5334".format(Landkreis, IdLandkreis))
        Landkreis = "StadtRegion Aachen"
        IdLandkreis = 5334
        data["Landkreis"][row] = Landkreis
        data["IdLandkreis"][row] = IdLandkreis

    #print(record)
    #print(record["IdLandkreis"])
    #censusLK = Census[dt.f.IdLandkreis == IdLandkreis,:]
    if IdLandkreis >= 0:
        if IdLandkreis not in Census:
            print("No census data for Landkreis Id: {} Name: {}".format(IdLandkreis, Landkreis))
            exit(1)

        if IdLandkreis not in Flaeche:
            print("No area data for Landkreis Id: {} Name: {}".format(IdLandkreis, Landkreis))
            exit(1)

    #if IdLandkreis in Bevoelkerung:
    #    KreisBevoelkerung = Bevoelkerung[IdLandkreis]
    #    data["Bevoelkerung"][row] = KreisBevoelkerung
    #    data["FaellePro100k"][row] = data["AnzahlFall"][row]*100000/KreisBevoelkerung
    #    data["TodesfaellePro100k"][row] = data["AnzahlTodesfall"][row]*100000/KreisBevoelkerung
    #    isStadt = Landkreis[:2] != "LK"
    #    data["isStadt"][row] = int(isStadt)
    #
    #     if IdLandkreis in Flaeche:
    #         KreisFlaeche = Flaeche[IdLandkreis]
    #         data["Flaeche"][row] = KreisFlaeche
    #         data["FaelleProKm2"][row] = data["AnzahlFall"][row]*KreisFlaeche
    #         data["TodesfaelleProKm2"][row] = data["AnzahlTodesfall"][row]*KreisFlaeche
    #         data["Dichte"][row] = float(KreisBevoelkerung)/float(KreisFlaeche)
    #     else:
    #         if IdLandkreis not in missingIds:
    #             missingIds.append(IdLandkreis)
    #             missingNames.append(Landkreis)
    #             print("Flaeche missing:",missingIds)
    #             print("Flaeche missing:",missingNames)
    # else:
    #     if IdLandkreis not in missingIds:
    #         missingIds.append(IdLandkreis)
    #         missingNames.append(Landkreis)
    #         print("Bevoelkerung missing:", missingIds)
    #         print("Bevoelkerung missing:", missingNames)
    return data

Flaeche = loadLandkreisFlaeche()
Census = loadCensus()

# much faster than the version above
def unify(table):
    dss = table[0,"Datenstand"]
    ds = cd.datetimeFromDatenstandAny(dss)

    dsdy = cd.dayFromDate(ds)
    hasRefdatum = "Refdatum" in table.names
    hasErkrankungsbeginn = "IstErkrankungsbeginn" in table.names
    #t = table.copy()
    t = table
    if "Altersgruppe2" in table.names:
        t = t[:,dt.f[:].remove(dt.f["Altersgruppe2"])]
    if not "DatenstandISO" in table.names:
        isodate = cd.dateStrYMDFromDay(dsdy)
        t = t[:, dt.f[:].extend({"DatenstandISO": isodate})]
    if not hasRefdatum:
        t = t[:, dt.f[:].extend({"Refdatum": str(cd.day0d), "RefdatumISO": dt.f.MeldedatumISO})]
    if not hasErkrankungsbeginn:
        t = t[:, dt.f[:].extend({"IstErkrankungsbeginn": 0})]

    if "NeuGenesen" not in table.names:
        t = t[:, dt.f[:].extend({"NeuGenesen":-9, "AnzahlGenesen":0})]

    t = t[:, dt.f[:].extend({"FallGruppe":"", "MeldeTag":nan, "RefTag":nan, "DatenstandTag":dsdy})]

    #t = t[:, dt.f[:].extend({"Bevoelkerung":0, "FaellePro100k":0.0, "TodesfaellePro100k":0.0, "isStadt":False})]
    #t = t[:, dt.f[:].extend({"Flaeche":0.0, "FaelleProKm2":0.0, "TodesfaelleProKm2":0.0, "Dichte":0.0})]

    #print("unified fields", t.names)

    #Bevoelkerung = loadLandkreisBeveolkerung()
    #Flaeche = loadLandkreisFlaeche()
    #Census = loadCensus()

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
        checkLandkreisData(d, r, Census, Flaeche)
    return dt.Frame(d)

def save(table, origFileName, destDir="."):
    path = os.path.normpath(origFileName)
    fileName = path.split(os.sep)[-1]
    newFile =  destDir+"/X-"+fileName
    print("Saving "+newFile)
    table.to_csv(newFile)

def isNewData(dataFilename, daysIncluded):
    peekTable = dt.fread(dataFilename, max_nrows=1)
    print("Checking "+dataFilename)
    ##print(peekTable)
    ##datenStand = peekTable[0, dt.f.DatenstandISO]
    dss = peekTable[0, "Datenstand"]
    print("Datenstand", dss)
    ds = cd.datetimeFromDatenstandAny(dss)
    dsdy = cd.dayFromDate(ds)
    isNew = dsdy not in daysIncluded
    if isNew:
        print("contains new day {}".format(dsdy))
    else:
        print("contains day {} already in full table".format(dsdy))

    return isNew

def tableData(dataFilename):
    print("Loading " + dataFilename)
    fullTable = dt.fread(dataFilename)
    print("Loading done loading table from ‘" + dataFilename + "‘, keys:")
    #print(fullTable.keys())
    if "NeuerFall" not in fullTable.keys():
        print("NeuerFall/NeuerTodesfall not in data file, creating column with default value 1 (Neuer Fall nur heute)")
        fullTable = fullTable[:, dt.f[:].extend({"NeuerFall":1, "NeuerTodesfall":1})]

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

def checkColumns(list1, list2):
    diff12 = set(list1) - set(list2)
    if len(diff12):
        print("missing in List1",diff12)
    diff21 = set(list2) - set(list1)
    if len(diff21):
        print("missing in List2",diff21)

def main():
    parser = argparse.ArgumentParser(description='Create a unfied data file from daily dumps')
    parser.add_argument('files', metavar='fileName', type=str, nargs='+',
                        help='.NPGEO COVID19 Germany data as .csv file')
    parser.add_argument('-d', '--output-dir', dest='outputDir', default=".")

    args = parser.parse_args()
    print(args)
    fullTable = None
    jayPath = args.outputDir+"/all-data.jay"
    print(jayPath)

    daysIncluded = []
    if os.path.isfile(jayPath):
        print("Loading " + jayPath)
        fullTable = dt.fread(jayPath)
        daysIncluded = sorted(fullTable[:, [dt.first(dt.f.DatenstandTag)],dt.by(dt.f.DatenstandTag)].to_list()[0])
        print("Days in full table:")
        print(daysIncluded)

    for fa in args.files:
        files = sorted(glob.glob(fa))
        for f in files:
            if isNewData(f, daysIncluded):
                t = tableData(f)
                print("Hashing " + f)
                newTable = unify(t)
                save(newTable,f,args.outputDir)
                if fullTable is None:
                    fullTable = newTable
                else:
                    #print("full fields", fullTable.names)
                    checkColumns(fullTable.names, newTable.names)
                    fullTable.rbind(newTable)

    pmu.saveJayTable(fullTable, "all-data.jay", args.outputDir)
    #pmu.saveCsvTable(fullTable, "all-data.csv", args.outputDir)

if __name__ == "__main__":
    # execute only if run as a script
    main()