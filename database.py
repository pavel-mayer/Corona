
import argparse
import datatable as dt
import cov_dates as cd
from math import nan
import dateutil.parser
import os
import pm_util as pmu
from dateutil.parser import parse
import time


def cval(tableCell):
    return tableCell.to_list()[0][0]

def cstr(tableCell):
    return str(tableCell.to_list()[0][0])

def cint(tableCell):
    return int(tableCell.to_list()[0][0])

def analyzeDay(fullTable, day):
    dayTable = fullTable[(dt.f.DatenstandTag == day),:]
    cases = dayTable[(dt.f.NeuerFall == 0) | (dt.f.NeuerFall == 1), 'AnzahlFall'].sum()[0, 0]
    new_cases = dayTable[(dt.f.NeuerFall == -1) | (dt.f.NeuerFall == 1), 'AnzahlFall'].sum()[0, 0]
    dead = dayTable[(dt.f.NeuerTodesfall == 0) | (dt.f.NeuerTodesfall == 1), 'AnzahlTodesfall'].sum()[0, 0]
    new_dead = dayTable[(dt.f.NeuerTodesfall == -1) | (dt.f.NeuerTodesfall == 1), 'AnzahlTodesfall'].sum()[0, 0]

    recovered = 0
    new_recovered = 0
    if "NeuGenesen" in dayTable.keys():
        recovered = dayTable[(dt.f.NeuGenesen == 0) | (dt.f.NeuGenesen == 1), 'AnzahlGenesen'].sum()[0, 0]
        new_recovered = dayTable[(dt.f.NeuGenesen == -1) | (dt.f.NeuGenesen == 1), 'AnzahlGenesen'].sum()[0, 0]
    #lastDay=fullTable[:,'MeldeDay'].max()[0,0]
    #lastnewCaseOnDay=fullTable[:,'newCaseOnDay'].max()[0,0]
    print("{} Day {}: cases {} (+{}), dead {} (+{}), recovered {} (+{})".format(
        cd.dateStrDMFromDay(day), day, int(cases), int(new_cases), int(dead), int(new_dead), int(recovered),int(new_recovered)))

def analyzeDayRange(fullTable, fromDay, toDay):
    dayTable = fullTable[(dt.f.DatenstandTag >= fromDay) & (dt.f.DatenstandTag < toDay),:]

    cases_to_count = dayTable[(dt.f.NeuerFall == 0) | (dt.f.NeuerFall == 1),:]
    cases = cases_to_count[:, [dt.sum(dt.f.AnzahlFall)],dt.by(dt.f.DatenstandTag)]

    new_cases_to_count = dayTable[(dt.f.NeuerFall == -1) | (dt.f.NeuerFall == 1),:]
    new_cases = new_cases_to_count[:, [dt.sum(dt.f.AnzahlFall)],dt.by(dt.f.DatenstandTag)]

    dead_to_count = dayTable[(dt.f.NeuerTodesfall == 0) | (dt.f.NeuerTodesfall == 1),:]
    dead = dead_to_count[:, [dt.sum(dt.f.AnzahlTodesfall)],dt.by(dt.f.DatenstandTag)]

    new_dead_to_count = dayTable[(dt.f.NeuerTodesfall == -1) | (dt.f.NeuerTodesfall == 1),:]
    new_dead = new_dead_to_count[:, [dt.sum(dt.f.AnzahlTodesfall)],dt.by(dt.f.DatenstandTag)]

    if "NeuGenesen" in dayTable.keys():
        recovered_to_count = dayTable[(dt.f.NeuGenesen == 0) | (dt.f.NeuGenesen == 1),:]
        recovered = recovered_to_count[:, [dt.sum(dt.f.AnzahlGenesen)],dt.by(dt.f.DatenstandTag)]
        new_recovered_to_count = dayTable[(dt.f.NeuGenesen == -1) | (dt.f.NeuGenesen == 1),:]
        new_recovered = new_recovered_to_count[:, [dt.sum(dt.f.AnzahlGenesen)],dt.by(dt.f.DatenstandTag)]

    #lastDay=fullTable[:,'MeldeDay'].max()[0,0]
    #lastnewCaseOnDay=fullTable[:,'newCaseOnDay'].max()[0,0]
    print("From {}-{} Day {}-{}: cases {} (+{}), dead {} (+{}), recovered {} (+{})".format(
        cd.dateStrDMFromDay(fromDay), cd.dateStrDMFromDay(toDay), fromDay, toDay,
        cases.to_list(), new_cases.to_list(), dead.to_list(), new_dead.to_list(), recovered.to_list(), new_recovered.to_list()))
    return cases, new_cases, dead, new_dead, recovered, new_recovered


#def analyzeDayRangeBy(fullTable, fromDay, toDay, forIdLandkreis):
def analyzeDaily(fullTable, filter, postfix):

    #print("----- analyzeDaily:"+postfix)
    #dayTable = fullTable[(dt.f.DatenstandTag >= fromDay) & (dt.f.DatenstandTag < toDay) & (dt.f.IdLandkreis == forIdLandkreis),:]
    dayTable = fullTable[filter,:]

    cases_to_count = dayTable[(dt.f.NeuerFall == 0) | (dt.f.NeuerFall == 1),:]
    cases = cases_to_count[:, [dt.sum(dt.f.AnzahlFall)],dt.by(dt.f.DatenstandTag)]
    cases.names = ["DatenstandTag", "AnzahlFall"+postfix]
    cases.key = "DatenstandTag"
    print("cases rows = {}, cases_to_count = {}".format(cases.nrows, cases_to_count.nrows))
    #print(cases)

    new_cases_to_count = dayTable[(dt.f.NeuerFall == -1) | (dt.f.NeuerFall == 1),:]
    new_cases = new_cases_to_count[:, [dt.sum(dt.f.AnzahlFall)],dt.by(dt.f.DatenstandTag)]
    new_cases.names = ["DatenstandTag", "AnzahlFallNeu"+postfix]
    new_cases.key = "DatenstandTag"
    print("new_cases rows = {}, new_cases_to_count = {}".format(new_cases.nrows, new_cases_to_count.nrows))
    #new_cases_to_count.to_csv("new_cases_to_count.csv")

    new_cases_to_count_delay = new_cases_to_count[(dt.f.AnzahlFall > 0),:] # measure delay only for positive cases
    new_cases_to_count_delay.materialize()
    new_cases_delay = new_cases_to_count_delay[:, [dt.min(dt.f.MeldeDelay),dt.max(dt.f.MeldeDelay),
                      dt.mean(dt.f.MeldeDelay),dt.median(dt.f.MeldeDelay),
                      dt.sd(dt.f.MeldeDelay), dt.sum(dt.f.AnzahlFall),
                      dt.max(dt.f.DatenstandTag)],dt.by(dt.f.DatenstandTag)]
    new_cases_delay.names = ["DatenstandTag",
                             "MeldeDauerFallNeu-Min" + postfix,"MeldeDauerFallNeu-Max" + postfix,
                             "MeldeDauerFallNeu-Schnitt" + postfix, "MeldeDauerFallNeu-Median" + postfix,
                             "MeldeDauerFallNeu-StdAbw" + postfix, "MeldeDauerFallNeu-Fallbasis" + postfix,
                             "DatenstandTag-Max" + postfix]
    new_cases_delay.key = "DatenstandTag"
    print("new_cases_delay rows = {}, new_cases_to_count_delay = {}".format(new_cases_delay.nrows, new_cases_to_count_delay.nrows))
    #new_cases_delay = new_cases_to_count_delay[:, [dt.mean(dt.f.DatenstandTag-dt.f.MeldeTag)],dt.by(dt.f.DatenstandTag)]

    #     delays = delayRecs[:, [dt.mean(dt.f.MeldeDelay), dt.median(dt.f.MeldeDelay), dt.sd(dt.f.MeldeDelay), dt.sum(dt.f.AnzahlFall)], dt.by(dt.f.Landkreis)]

    # new_cases_stddev = new_cases_to_count_delay[:, [dt.mean(dt.f.DatenstandTag - dt.f.MeldeTag)],
    #                   dt.by(dt.f.DatenstandTag)]
    # new_cases_delay.names = ["DatenstandTag", "AnzahlFallNeu-MeldeDauer" + postfix]
    # new_cases_delay.key = "DatenstandTag"
    # print("new_cases_delay rows = {}, new_cases_to_count_delay = {}".format(new_cases_delay.nrows,
    #                                                                         new_cases_to_count_delay.nrows))

    new_cases_to_count_strict = new_cases_to_count[(dt.f.DatenstandTag - dt.f.MeldeTag < 7) | (dt.f.AnzahlFall < 0) ,:]
    new_cases_strict = new_cases_to_count_strict[:, [dt.sum(dt.f.AnzahlFall)],dt.by(dt.f.DatenstandTag)]
    new_cases_strict.names = ["DatenstandTag", "AnzahlFallNeu-Meldung-letze-7-Tage"+postfix]
    new_cases_strict.key = "DatenstandTag"
    print("new_cases_strict rows = {}, new_cases_to_count_strict = {}".format(new_cases_strict.nrows, new_cases_to_count_strict.nrows))
    #new_cases_to_count_strict.to_csv("new_cases_to_count_strict.csv")

    new_cases_to_count_strict_14 = new_cases_to_count[(dt.f.DatenstandTag - dt.f.MeldeTag < 14) | (dt.f.AnzahlFall < 0) ,:]
    new_cases_strict_14 = new_cases_to_count_strict_14[:, [dt.sum(dt.f.AnzahlFall)],dt.by(dt.f.DatenstandTag)]
    new_cases_strict_14.names = ["DatenstandTag", "AnzahlFallNeu-Meldung-letze-14-Tage"+postfix]
    new_cases_strict_14.key = "DatenstandTag"
    print("new_cases_strict_14 rows = {}, new_cases_to_count_strict_14 = {}".format(new_cases_strict_14.nrows, new_cases_to_count_strict_14.nrows))

    dead_to_count = dayTable[(dt.f.NeuerTodesfall == 0) | (dt.f.NeuerTodesfall == 1),:]
    dead = dead_to_count[:, [dt.sum(dt.f.AnzahlTodesfall)],dt.by(dt.f.DatenstandTag)]
    dead.names = ["DatenstandTag", "AnzahlTodesfall"+postfix]
    dead.key = "DatenstandTag"
    #print("dead rows = {}".format(dead.nrows))

    new_dead_to_count = dayTable[(dt.f.NeuerTodesfall == -1) | (dt.f.NeuerTodesfall == 1),:]
    new_dead = new_dead_to_count[:, [dt.sum(dt.f.AnzahlTodesfall)],dt.by(dt.f.DatenstandTag)]
    new_dead.names = ["DatenstandTag", "AnzahlTodesfallNeu"+postfix]
    new_dead.key = "DatenstandTag"
    #print("new_dead rows = {}".format(new_dead.nrows))

    recovered_to_count = dayTable[(dt.f.NeuGenesen == 0) | (dt.f.NeuGenesen == 1),:]
    recovered = recovered_to_count[:, [dt.sum(dt.f.AnzahlGenesen)],dt.by(dt.f.DatenstandTag)]
    recovered.names = ["DatenstandTag", "AnzahlGenesen"+postfix]
    recovered.key = "DatenstandTag"
    #print("recovered rows = {}".format(recovered.nrows))

    new_recovered_to_count = dayTable[(dt.f.NeuGenesen == -1) | (dt.f.NeuGenesen == 1),:]
    new_recovered = new_recovered_to_count[:, [dt.sum(dt.f.AnzahlGenesen)],dt.by(dt.f.DatenstandTag)]
    new_recovered.names = ["DatenstandTag", "AnzahlGenesenNeu"+postfix]
    new_recovered.key = "DatenstandTag"
    #print("new_recovered rows = {}".format(new_recovered.nrows))

    byDayTable = cases[:,:,dt.join(new_cases)]\
                     [:,:,dt.join(dead)][:,:,dt.join(new_dead)][:,:,dt.join(recovered)][:,:,dt.join(new_recovered)]\
        [:,:,dt.join(new_cases_strict)][:,:,dt.join(new_cases_strict_14)][:,:,dt.join(new_cases_delay)]
    byDayTable.key = "DatenstandTag"
    #print("byDayTable rows = {}".format(byDayTable.nrows))
    print(byDayTable)

    return byDayTable


def analyzeDailyAltersgruppen(fullTable, byDayTable, filter, Altersgruppen, Geschlechter, postfix):
    #byDayTable = analyzeDaily(fullTable, filter, postfix)
    #print("----- analyzeDailyAltersgruppen:"+postfix)

    for ag in Altersgruppen:
        print("Analyzing Altergruppe "+ ag)
        byDayTableAG = analyzeDaily(fullTable, filter & (dt.f.Altersgruppe == ag), postfix+"-AG-"+ag)
        byDayTable = byDayTable[:,:,dt.join(byDayTableAG)]
        byDayTable.key = "DatenstandTag"
    return byDayTable

def analyzeDailyAltersgruppenGeschlechter(fullTable, filter, Altersgruppen, Geschlechter):
    byDayTable = analyzeDaily(fullTable, filter, "")
    byDayTable = analyzeDailyAltersgruppen(fullTable, byDayTable, filter, Altersgruppen, Geschlechter,"")
    #byDayTable = byDayTable[:, :, dt.join(byDayTableAG)]
    return byDayTable
    print("byDayTable 1", byDayTable.names)
    for g in Geschlechter:
        print("Analyzing Geschlechter "+ g)
        byDayTableG = analyzeDaily(fullTable, filter & (dt.f.Geschlecht == g), "-G-"+g)
        print("byDayTableG", byDayTableG.names)
        byDayTable = byDayTable[:,:,dt.join(byDayTableG)]
        print("byDayTable 2", byDayTable.names)
        byDayTable = analyzeDailyAltersgruppen(fullTable, byDayTable, filter & (dt.f.Geschlecht == g), Altersgruppen, Geschlechter, "-G-"+g)
        #print("byDayTableAG", byDayTableAG.names)
        #byDayTable = byDayTable[:,:,dt.join(byDayTableAG)]
        print("byDayTable 3", byDayTable.names)

    return byDayTable

def filterByDayAndLandkreis(fromDay, toDay, forIdLandkreis):
    return (dt.f.DatenstandTag >= fromDay) & (dt.f.DatenstandTag < toDay) & (dt.f.IdLandkreis == forIdLandkreis)

def filterByDayAndCriteria(fromDay, toDay, criteria):
    return (dt.f.DatenstandTag >= fromDay) & (dt.f.DatenstandTag < toDay) & criteria

def filterByDay(fromDay, toDay):
    return (dt.f.DatenstandTag >= fromDay) & (dt.f.DatenstandTag < toDay)

def timeSeries(fullTable, fromDay, toDay, byCriteria, nameColumn, Altersgruppen, Geschlechter):
    regions = fullTable[:, [dt.first(nameColumn)], dt.by(byCriteria)]
    #regions = regions[:5,:]
    print("Creating time series for regions:")
    print(regions)
    dailysByCriteria = {}
    start = time.perf_counter()
    for i, lk in enumerate(regions[:, byCriteria].to_list()[0]):
        print("Processing Region '{}'".format(regions[i,nameColumn][0,0]))
        start_region = time.perf_counter()

        pmu.printMemoryUsage("pre analyzeDailyAltersgruppenGeschlechter")
        dailysByCriteria[lk] = analyzeDailyAltersgruppenGeschlechter(fullTable,
            filterByDayAndCriteria(fromDay, toDay, (byCriteria == lk)), Altersgruppen, Geschlechter)
        finish = time.perf_counter()
        duration = finish - start
        print("Region took {:.2f} seconds, elapsed {:.2f} minutes, time to completion: {:.2f} minutes".format(finish-start_region, duration/60, duration/(i+1) * (regions.nrows - i)/60))

        pmu.printMemoryUsage("post analyzeDailyAltersgruppenGeschlechter")
        print("Done {} of {}, key = {} name = {}".format(i+1, regions.nrows, lk, regions[i,nameColumn][0,0]))
        #if lk >= 0:
        #    break
    return regions, dailysByCriteria

def makeIncidenceColumns(regionTable, censusTable, Altersgruppen, Geschlechter):
    #ag_Berlin = {'A00-A04': 195952, 'A05-A14': 318242, 'A15-A34': 826771, 'A35-A59': 1489424, 'A60-A79': 754139,
    #             'A80+': 198527, 'unbekannt': 0}

    print(censusTable)
    regionTable = regionTable[:, dt.f[:].extend({"Einwohner": censusTable[0,"Insgesamt-total"]})]
    regionTable = regionTable[:, dt.f[:].extend({"Dichte": censusTable[0,"Insgesamt-total"] / dt.f.Flaeche})]
    regionTable = regionTable[:, dt.f[:].extend({"InzidenzFallNeu": (100000.0 * dt.f.AnzahlFallNeu) / censusTable[0,"Insgesamt-total"]})]

    for ag in Altersgruppen:
        if ag != "unbekannt":
            srccolname = "AnzahlFallNeu-AG-" + ag
            newcolname = "InzidenzFallNeu-" + ag
            censuscolname = ag + "-total"
            AnzahlFallNeu_X = dt.f[srccolname]
            ag_size = censusTable[0,censuscolname]
            #print("srccolname:{} newcolname:{} AnzahlFallNeu_X:{} ag_size:{}".format(srccolname, newcolname, AnzahlFallNeu_X, ag_size))
            regionTable = regionTable[:, dt.f[:].extend({"Einwohner-"+ag: ag_size})]
            regionTable = regionTable[:, dt.f[:].extend({newcolname: (100000.0 * AnzahlFallNeu_X) / ag_size})]
    print(regionTable)
    return regionTable

def add7DayAverages(table):
    print(table.names)
    candidatesColumns = [name for name in  table.names if "Neu" in name]
    print(candidatesColumns)

def loadCensus(fileName="../CensusByRKIAgeGroups.csv"):
    census = dt.fread(fileName)
    sKeys = census[:, "IdLandkreis"].to_list()[0]
    values = census[:, "Landkreis"].to_list()[0]
    valuesDict = dict(zip(sKeys, values))
    #print(valuesDict)
    return valuesDict

def loadFlaechen(fileName="covid-19-germany-landkreise.csv"):
    geodata = dt.fread(fileName)
    sKeys = geodata[:, 'Regional code'].to_list()[0]
    values = geodata[:, 'Cadastral area'].to_list()[0]

    bundeslaenderFlaechen = geodata[:, [dt.sum(dt.f['Cadastral area'])],dt.by(dt.f['Land ID'])]
    sKeys = sKeys + bundeslaenderFlaechen[:, 'Land ID'].to_list()[0]
    values = values + bundeslaenderFlaechen[:, 'Cadastral area'].to_list()[0]

    deutschlandFlaeche = bundeslaenderFlaechen[:,'Cadastral area'].sum()
    sKeys = sKeys + [0]
    values = values + deutschlandFlaeche.to_list()[0]

    valuesDict = dict(zip(sKeys, values))
    #print(valuesDict)
    return valuesDict


# def loadLandkreisFlaeche(fileName="../covid-19-germany-landkreise.csv"):
#     result = {}
#     with open(fileName, newline='') as csvfile:
#         reader = csv.DictReader(csvfile, delimiter=';')
#         for i, row in enumerate(reader):
#             #print(row)
#             LandkreisID = int(row['Regional code'])
#             FlaecheStr = row['Cadastral area']
#             if FlaecheStr != "":
#                 #print(FlaecheStr)
#                 result[LandkreisID] = float(FlaecheStr)
#     #print("Flaeche:", result)
#     return result

def insertDates(table):
    days = table[:,"DatenstandTag"].to_list()[0]
    dates = [cd.dateStrFromDay(day) for day in days]
    result = table[:,dt.f["DatenstandTag"].extend({"Datum": ""})]
    result[:,"Datum"] = dt.Frame(dates)
    result.cbind(table[:,1:])
    return result

def insertRegionInfo(table,IdLandkreis, Landkreis, LandkreisTyp, IdBundesland, Bundesland, Flaeche):
    result = table[:, dt.f[:2].extend({"IdLandkreis": IdLandkreis, "Landkreis": Landkreis, "LandkreisTyp": LandkreisTyp, "IdBundesland": IdBundesland,
                                   "Bundesland": Bundesland, "Flaeche": Flaeche})]
    result.cbind(table[:,2:])
    return result

def landKreisTyp(lk_id, lk_name):
    lk_Typ = lk_name[:2]
    if lk_Typ == "SK" or lk_Typ == "LK":
        return lk_Typ
    else:
        return "LSK"

def analyze(fullTable, args):
    #fullTable = fullTable[dt.f.DatenstandTag > 382 - 20,:]
    print("Analyzing")
    pmu.printMemoryUsage("begin analyze")
    print("Keys:")
    print(fullTable.keys())
    firstDumpDay = cint(fullTable[:,"DatenstandTag"].min())
    lastDumpDay = cint(fullTable[:,"DatenstandTag"].max())
    print("firstDumpDay", firstDumpDay)
    print("lastDumpDay",lastDumpDay)

    #fromDay = lastDumpDay-27
    fromDay = firstDumpDay
    toDay = lastDumpDay+1

    fullTable = fullTable[:, dt.f[:].extend({"MeldeDelay": dt.f.DatenstandTag-dt.f.MeldeTag-1})]
    fullTable = fullTable[:, dt.f[:].extend({"RefDelay": dt.f.DatenstandTag-dt.f.RefTag-1})]
    fullTable.materialize()

    Altersgruppen = []
    if args.agegroups:
        Altersgruppen = dt.unique(fullTable[:,"Altersgruppe"]).to_list()[0]

    print("Altersgruppen", Altersgruppen)

    Geschlechter = dt.unique(fullTable[:,"Geschlecht"]).to_list()[0]
    print("Geschlechter", Geschlechter)

    census = dt.fread("CensusByRKIAgeGroups.csv")
    censusDeutschland = census[dt.f.Name == "Deutschland",:]
    print(censusDeutschland)

    flaechen = loadFlaechen()
    #for id in range(1,16):
    #    censusBL = census[dt.f.Code == id, :]
    #    print(censusBL)

    print("Processing 'Deutschland'")
    pmu.printMemoryUsage("begin Deutschland")
    deutschland = analyzeDailyAltersgruppenGeschlechter(fullTable, filterByDay(fromDay, toDay), Altersgruppen, Geschlechter)
    deutschland = insertDates(deutschland)
    deutschland = insertRegionInfo(deutschland, 0, "Deutschland", "BR", 0, "Deutschland", flaechen[0])

    print(deutschland)
    pmu.printMemoryUsage("pre makeIncidenceColumns")

    deutschland = makeIncidenceColumns(deutschland, censusDeutschland, Altersgruppen, Geschlechter)
    print(deutschland)
    pmu.printMemoryUsage("pre save")
    pmu.saveCsvTable(deutschland, "series-{}-{}.csv".format(0, "Deutschland"), args.outputDir)
    pmu.printMemoryUsage("post save")
    deutschland = None

    #exit(0)

    print("Processing Bundesländer")
    bundeslaender, bundeslaender_numbers = timeSeries(fullTable, fromDay, toDay, dt.f.IdBundesland, dt.f.Bundesland, Altersgruppen, Geschlechter)
    pmu.printMemoryUsage("post Bundesländer timeSeries")
    for i in range(bundeslaender.nrows):
        bl_name=bundeslaender[i,dt.f.Bundesland].to_list()[0][0]
        bl_id=bundeslaender[i,dt.f.IdBundesland].to_list()[0][0]

        if bl_id > 0:
            #bundeslaender_numbers[bl_id] = bundeslaender_numbers[bl_id][:, dt.f[:].extend(
            #    {"IdLandkreis": bl_id, "Landkreis": bl_name, "IdBundesland": bl_id, "Bundesland": bl_name, "Flaeche" : flaechen[bl_id]})]
            bundeslaender_numbers[bl_id] = insertDates(bundeslaender_numbers[bl_id])
            bundeslaender_numbers[bl_id] = insertRegionInfo(bundeslaender_numbers[bl_id], bl_id, bl_name, "BL", bl_id, bl_name, flaechen[0])
            censusBL = census[dt.f.IdLandkreis == bl_id, :]
            print(censusBL)
            bundeslaender_numbers[bl_id] = makeIncidenceColumns(bundeslaender_numbers[bl_id], censusBL, Altersgruppen, Geschlechter)
        pmu.printMemoryUsage("pre save {}".format(bl_name))

        pmu.saveCsvTable(bundeslaender_numbers[bl_id], "series-{}-{}.csv".format(bl_id, bl_name), args.outputDir)
    bundeslaender = None
    bundeslaender_numbers = None

    print("Processing Landkreise'")
    landKreise, landkreise_numbers = timeSeries(fullTable, fromDay, toDay, dt.f.IdLandkreis, dt.f.Landkreis, Altersgruppen, Geschlechter)
    pmu.printMemoryUsage("post Landkreise timeSeries")
    #print(landKreise)
    #print(landkreise_numbers)
    for i in range(landKreise.nrows):
        print(i)
        lk_name = landKreise[i, dt.f.Landkreis].to_list()[0][0]
        lk_id = landKreise[i, dt.f.IdLandkreis].to_list()[0][0]
        if lk_id > 0:
             censusLK = census[dt.f.IdLandkreis == lk_id, :]
             bl_name = censusLK[0,dt.f.Bundesland].to_list()[0][0]
             bl_id = censusLK[0, dt.f.IdBundesland].to_list()[0][0]
             lk_typ = landKreisTyp(lk_id, lk_name)

             landkreise_numbers[lk_id] = insertDates(landkreise_numbers[lk_id])
             landkreise_numbers[lk_id] = insertRegionInfo(landkreise_numbers[lk_id], lk_id, lk_name, lk_typ, bl_id,
                                                             bl_name, flaechen[lk_id])
             #landkreise_numbers[lk_id] = landkreise_numbers[lk_id][:, dt.f[:].extend(
             #   {"IdLandkreis": lk_id, "Landkreis": lk_name, "IdBundesland": bl_id, "Bundesland": bl_name,
             #    "Flaeche": flaechen[lk_id]})]
             print(censusLK)
             landkreise_numbers[lk_id] = makeIncidenceColumns(landkreise_numbers[lk_id], censusLK, Altersgruppen,
                                                                Geschlechter)
        pmu.printMemoryUsage("pre save {}".format(lk_name))
        pmu.saveCsvTable(landkreise_numbers[lk_id], "series-{}-{}.csv".format(lk_id, lk_name), args.outputDir)
    #print(landKreise)

    return fullTable

def main():
    parser = argparse.ArgumentParser(description='Fast queries on all data')
    parser.add_argument('file', metavar='fileName', type=str, nargs='?',
                        help='.Full unified NPGEO COVID19 Germany data as .csv or .jay file',
                        default="archive_v2/all-data.jay")
    parser.add_argument('-d', '--output-dir', dest='outputDir', default="series")
    parser.add_argument("--agegroups", help="also create columns for all seperate age groups", action="store_true")

    args = parser.parse_args()
    #print(args)
    pmu.printMemoryUsage("after start")
    print("Loading " + args.file)
    fullTable = dt.fread(args.file)
    print("Loading done loading table from ‘{}‘, rows: {} cols: {}".format(args.file, fullTable.nrows, fullTable.ncols))
    pmu.printMemoryUsage("after load")

    if False:
        print("Materializing fullTable")
        fullTable.materialize(to_memory=True)
        pmu.printMemoryUsage("after materialize")

    analyze(fullTable, args)

if __name__ == "__main__":
    # execute only if run as a script
    main()