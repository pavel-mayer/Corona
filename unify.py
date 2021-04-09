
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
import time
import pandas as pd # pd.to_datetime(ds)
import calendar

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
        #print("#Info: Changed bad Göttingen Landkreis Id from 3152 to 3159")
    if Landkreis == "LK Aachen" or IdLandkreis == 5354:
        # change bad Landkreis for Aachen to Stadtregion
        #print("#Info: Bad record in row:",row)
        #print("#Info: Changing bad '{}' Kreis with Id {} to ‘StadtRegion Aachen‘ id 5334".format(Landkreis, IdLandkreis))
        Landkreis = "StadtRegion Aachen"
        IdLandkreis = 5334
        data["Landkreis"][row] = Landkreis
        data["IdLandkreis"][row] = IdLandkreis

    if Landkreis == "LK Saarpfalz-Kreis":
        #print("#Info: Bad record in row:",row)
        #print("#Info: Changing bad '{}' Kreis with Id {} to ‘LK Saar-Pfalz-Kreis‘ id 5334".format(Landkreis, IdLandkreis))
        Landkreis = "LK Saar-Pfalz-Kreis"
        data["Landkreis"][row] = Landkreis

    ## TODO: Normalize all Landkreis Names

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

def datetimeFromAnyDateStr2(s) -> str:
    dt = dateutil.parser.parse(s)
    dt = dt.replace(tzinfo=None)
    return dt

def datetimeFromAnyDateStr(s) -> str:
    dt = pd.to_datetime(s)
    dt = dt.replace(tzinfo=None)
    #dt2 = datetimeFromAnyDateStr2(s)
    #print(s)
    #if cd.dayFromDate(dt) != cd.dayFromDate(dt2):
    #    print("bad date parse '{}' dt={} dt2={}".format(s,dt,dt2))
    return dt

from datetime import datetime

def datetimeFrom(st):
    stf = time.mktime(st)
    sdt = datetime.fromtimestamp(stf, tz=None)
    return sdt

# Parse date string like "2020/11/25 00:00:00"
def datetimeFromDateStr3(ds):
    st = None
    if ds.isnumeric():
        return datetime.fromtimestamp(int(ds) / 1000)
    elif ds.endswith("T00:00:00.000Z"):
        st = time.strptime(ds, "%Y-%m-%dT00:00:00.000Z")
        return datetimeFrom(st)
    elif ds.endswith(" 00:00") and ds[2] == '.':
        st = time.strptime(ds, "%d.%m.%y 00:00")
        return datetimeFrom(st)
    elif ds.endswith("T00:00:00Z"):
        st = time.strptime(ds, "%Y-%m-%dT00:00:00Z")
        return datetimeFrom(st)
    elif ds.endswith(" 00:00:00+00:00"):
        st = time.strptime(ds, "%Y-%m-%d 00:00:00+00:00")
        return datetimeFrom(st)
    elif ds.endswith(" 12:00:00 AM"):
        st = time.strptime(ds, "%m/%d/%Y 12:00:00 AM")
        return datetimeFrom(st)
    elif ds.endswith(" 00:00:00") and ds[4] == '/':
        st = time.strptime(ds, "%Y/%m/%d 00:00:00")
        return datetimeFrom(st)
    elif len(ds) == 10:
        st = time.strptime(ds, "%Y-%m-%d")
        return datetimeFrom(st)
    else:
        print(ds)
        dt = dateutil.parser.parse(ds)
        dt = dt.replace(tzinfo=None)
        return dt

def ticksFromDateTime(dt):
    return calendar.timegm(dt.timetuple())*1000

import timeit

def testDatePerf():
    s = "2020-03-15T00:00:00.000Z"
    n = 10000
    r = 3
    #print("usecs:",[t/n*1000000 for t in timeit.repeat(lambda: datetimeFromAnyDateStr(s), number = n, repeat = r)])
    print("per sec:",[n/t for t in timeit.repeat(lambda: datetimeFromAnyDateStr(s), number = n, repeat = r)])
    print("per sec:",[n/t for t in timeit.repeat(lambda: datetimeFromAnyDateStr2(s), number = n, repeat = r)])
    print("per sec:",[n/t for t in timeit.repeat(lambda: datetimeFromDateStr3(s), number = n, repeat = r)])
    exit(1)

Flaeche = loadLandkreisFlaeche()
Census = loadCensus()

# much faster than the version above
def unify(table, makeFallGruppe=False):
    dss = table[0,"Datenstand"]
    ds = cd.datetimeFromDatenstandAny(dss)

    if 'FID' in table.names:
        table.names = {"FID":"ObjectId"}

    dsdy = cd.dayFromDate(ds)
    dsisodate = cd.dateStrYMDFromDay(dsdy)
    hasRefdatum = "Refdatum" in table.names
    hasErkrankungsbeginn = "IstErkrankungsbeginn" in table.names
    #t = table.copy()
    t = table
    if "Altersgruppe2" in table.names:
        t = t[:,dt.f[:].remove(dt.f["Altersgruppe2"])]
    if not "DatenstandISO" in table.names:
        t = t[:, dt.f[:].extend({"DatenstandISO": dsisodate})]
    if not hasRefdatum:
        t = t[:, dt.f[:].extend({"Refdatum": 0})]

    hasRefdatumISO = "RefdatumISO" in table.names
    if not hasRefdatumISO:
        #print("t1",t.names)
        t = t[:, dt.f[:].extend({"RefdatumISO": ""})]
        #print("t2",t.names)

    hasMeldedatumISO = "MeldedatumISO" in table.names
    if not hasMeldedatumISO:
        t = t[:, dt.f[:].extend({"MeldedatumISO": ""})]

    if not hasErkrankungsbeginn:
        t = t[:, dt.f[:].extend({"IstErkrankungsbeginn": 0})]

    if "NeuGenesen" not in table.names:
        t = t[:, dt.f[:].extend({"NeuGenesen":-9, "AnzahlGenesen":0})]

    if makeFallGruppe:
        t = t[:, dt.f[:].extend({"FallGruppe":"", "MeldeTag":nan, "RefTag":nan, "DatenstandTag":dsdy})]
    t = t[:, dt.f[:].extend({"MeldeTag":nan, "RefTag":nan, "DatenstandTag":dsdy})]

    pmu.printMemoryUsage("unify pre dict")
    d = t.to_dict()
    pmu.printMemoryUsage("unify post dict")

    print("> iterating through {} rows".format(t.nrows))
    start = time.perf_counter()
    for r in range(t.nrows):
        mds = d["Meldedatum"][r]
        if pmu.is_int(mds):
            md = cd.datetimeFromStampStr(mds)
        else:
            md = datetimeFromDateStr3(mds)
            d["Meldedatum"][r]=ticksFromDateTime(md)

        mdy = cd.dayFromDate(md)
        d["MeldeTag"][r] = mdy
        if not hasRefdatum:
            d["Refdatum"][r]= ticksFromDateTime(md)
            d["RefTag"][r]= mdy
        if not hasMeldedatumISO:
            d["MeldedatumISO"][r] = cd.dateStrYMDFromDay(mdy)

        if makeFallGruppe:
            fg = str(d["IdLandkreis"][r]) + d["Altersgruppe"][r]+ d["Geschlecht"][r]+str(int(d["MeldeTag"][r]))

        #if int(d["IstErkrankungsbeginn"][r]) == 1:
        rds = d["Refdatum"][r]
        if pmu.is_int(rds):
            rd = cd.datetimeFromStampStr(rds)
        else:
            rd = datetimeFromDateStr3(rds)
            d["Refdatum"][r]=ticksFromDateTime(rd)
        rdy = cd.dayFromDate(rd)
        d["RefTag"][r] = rdy
        if not hasRefdatumISO:
            d["RefdatumISO"][r] = cd.dateStrYMDFromDay(rdy)
        if makeFallGruppe:
            fg = fg+":"+str(rdy)

        if makeFallGruppe:
            d["FallGruppe"][r] = fg
        checkLandkreisData(d, r, Census, Flaeche)

    finish = time.perf_counter()

    print("< iterating through {} rows done, {:.1f} rows/sec".format(t.nrows, t.nrows/(finish-start)))

    pmu.printMemoryUsage("end of unify, pre frame")
    t = dt.Frame(d)
    pmu.printMemoryUsage("end of unify, post frame")
    return t

def save(table, origFileName, destDir="."):
    path = os.path.normpath(origFileName)
    fileName = path.split(os.sep)[-1]
    newFile =  destDir+"/X-"+fileName
    print("Saving "+newFile)
    table.to_csv(newFile)

def load(origFileName, destDir="."):
    path = os.path.normpath(origFileName)
    fileName = path.split(os.sep)[-1]
    newFile =  destDir+"/X-"+fileName
    if os.path.isfile(newFile):
        print("Loading "+newFile)
        result = dt.fread(newFile)
        return result
    return None

def isNewData(dataFilename, daysIncluded):
    pmu.printMemoryUsage("begin of isNewData")
    peekTable = dt.fread(dataFilename, max_nrows=1)
    print("Checking "+dataFilename)
    ##print(peekTable)
    ##datenStand = peekTable[0, dt.f.DatenstandISO]
    dss = peekTable[0, "Datenstand"]
    print("Datenstand", dss)
    ds = cd.datetimeFromDatenstandAny(dss)
    dsdy = cd.dayFromDate(ds)
    pmu.printMemoryUsage("isNewData")
    if dsdy in [28,29]:
        print("contains day {} which is not a proper dump, so we ignored it".format(dsdy))
        return False

    isNew = dsdy not in daysIncluded
    if isNew:
        print("contains new day {}".format(dsdy))
    else:
        print("contains day {} already in full table".format(dsdy))
    pmu.printMemoryUsage("end of isNewData")

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
    #testDatePerf()
    start = time.perf_counter()
    lastCheckPointTime = start
    parser = argparse.ArgumentParser(description='Create a unfied data file from daily dumps')
    parser.add_argument('files', metavar='fileName', type=str, nargs='+',
                        help='.NPGEO COVID19 Germany data as .csv file')
    parser.add_argument('-d', '--output-dir', dest='outputDir', default=".")
    parser.add_argument('-t', '--temp-dir', dest='tempDir', default=".")
    parser.add_argument("--flushread", help="flush full table an re-read after checkpoint lower memory footprint",
                        action="store_true")
    parser.add_argument("--partition", help="save data in partionions instead of one file; slower, but you can see progress and maybe need less memory, but ymmv",
                        action="store_true")
    parser.add_argument("--backup", help="create backup files before overwriting",
                        action="store_true")
    parser.add_argument("--resume", help="create backup files before overwriting",
                        action="store_true")
    parser.add_argument("--unsafe", help="directly overwrite output files, will corrupt the output file when killed while writing, but uses less disk space (only applies to single .jay file in non-partition mode)",
                        action="store_true")
    parser.add_argument("--force", help="build new database anyway", action="store_true")
    parser.add_argument("--destructivesave", help="release memory gradually while saving and reload after saving (experimental, untested, only applies to partiioned write)",
                    action="store_true")
    #parser.add_argument("--incremental", help="only load partial data", action="store_true")
    parser.add_argument("-v","--verbose", help="make more noise",
                        action="store_true")
    parser.add_argument("--partitionsize",  type=int, help="number of records per partition", default = 10000000)
    parser.add_argument("--memorylimit",  type=int, help="maximum memory limit for a database file")
    parser.add_argument("--checkpoint",  type=int, help="write checkpoint after amount of minutes elapsed", default = 60)
    parser.add_argument("--nthreads", type=int, help="number of concurrent threads used by python dataframes, 0 = as many as cores, 1 single-thread, -3 = 3 threads less than cores", default = 0)

    args = parser.parse_args()
    print(args)
    # print("args.inMemory",args.inMemory)
    # print("args.materializeNew",args.materializeNew)
    # print("args.noMaterialize",args.noMaterialize)

    if args.nthreads != 0:
        dt.options.nthreads = args.nthreads
    print("dt.options.nthreads", dt.options.nthreads)

    fullTable = None
    jayFile = "all-data.jay"
    jayPath = os.path.join(args.outputDir,jayFile)
    print(jayPath)
    pmu.printMemoryUsage("after start")

    partitioned = False
    if not args.force:
        if os.path.isfile(jayPath):
            print("Loading " + jayPath)
            fullTable = dt.fread(jayPath, tempdir=args.tempDir, memory_limit=args.memorylimit, verbose=args.verbose)
        elif len(pmu.getJayTablePartitions(jayPath)) > 0:
            fullTable = pmu.loadJayTablePartioned(jayPath, tempdir=args.tempDir, memory_limit=args.memorylimit, verbose=args.verbose)
            if fullTable == None:
                print("The file {} is not a valid jay file, please remove it and retry")
                exit(1)
            partitioned = True

    daysIncluded = []
    addedData = False
    version = 1
    lastversion = 0
    for fa in args.files:
        files = sorted(glob.glob(fa))
        for f in files:
            if fullTable is not None and version != lastversion:
                pmu.printMemoryUsage("after load")
                daysIncluded = sorted(
                    fullTable[:, [dt.first(dt.f.DatenstandTag)], dt.by(dt.f.DatenstandTag)].to_list()[0])
                print("Days in full table:")
                print(daysIncluded)
                pmu.printMemoryUsage("after first query")
                lastversion = version

            if isNewData(f, daysIncluded):
                pmu.printMemoryUsage("after isNewData query")
                fstart = time.perf_counter()
                unifiedTable = None
                if args.resume:
                    unifiedTable = load(f,args.outputDir)

                addedData = True
                version = version + 1
                if unifiedTable is None:
                    t = tableData(f)
                    pmu.printMemoryUsage("after tabledata query")

                    print("Unifying " + f)
                    unifiedTable = unify(t)
                    pmu.printMemoryUsage("after hashing")
                    save(unifiedTable,f,args.outputDir)
                    pmu.printMemoryUsage("after unifiedTable save")
                if fullTable is None:
                    fullTable = unifiedTable
                else:
                    #print("full fields", fullTable.names)
                    checkColumns(fullTable.names, unifiedTable.names)
                    #print("unifiedTable.names",unifiedTable.names)
                    pmu.printMemoryUsage("before fulltable rbind")
                    fullTable.rbind(unifiedTable)  # memory gets used here
                    #print("fullTable.names",fullTable.names)

                    pmu.printMemoryUsage("after rbind")
                ffinish = time.perf_counter()
                secs = ffinish - fstart
                #print("fullTable", fullTable)
                print("unifiedTable rows = {}".format(unifiedTable.nrows))
                print("fullTable rows = {}".format(fullTable.nrows))
                print("-> File time {:.1f} secs or {:.1f} mins or {:.1f} hours".format(secs, secs/60, secs/60/60))
                if time.perf_counter() - lastCheckPointTime > float(args.checkpoint) * 60:
                    print("Saving checkpoint @ {}".format(datetime.now()))
                    if args.partition:
                        pmu.saveJayTablePartioned(fullTable, jayFile, args.outputDir, args.partitionsize, True, args.destructivesave)
                        if args.flushread or args.destructivesave:
                            print("Re-reading checkpoint @ {}".format(datetime.now()))
                            fullTable = None
                            fullTable = pmu.loadJayTablePartioned(jayPath, tempdir=args.tempDir, memory_limit=args.memorylimit, verbose=args.verbose)
                    else:
                        pmu.saveJayTable(fullTable, "all-data.jay", args.outputDir, args.backup, args.unsafe)

                    lastCheckPointTime = time.perf_counter()
                    print("Checkpoint done @ {}".format(datetime.now()))

    if addedData or (args.partition != partitioned):
        pmu.printMemoryUsage("before full save")
        if args.partition:
            pmu.saveJayTablePartioned(fullTable, "all-data.jay", args.outputDir, args.partitionsize, True, args.destructivesave)
        else:
            pmu.saveJayTable(fullTable, "all-data.jay", args.outputDir,args.backup, args.unsafe)
        pmu.printMemoryUsage("after full save")
    else:
        print("No new data added, not saving.'")
    #pmu.saveCsvTable(fullTable, "all-data.csv", args.outputDir)
    finish = time.perf_counter()
    secs = finish - start
    print("Finished in {:.1f} secs or {:.1f} mins or {:.1f} hours".format(secs, secs/60, secs/60/60))

if __name__ == "__main__":
    # execute only if run as a script
    main()
