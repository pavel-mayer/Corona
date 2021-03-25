
import argparse
import datatable as dt
import cov_dates as cd
import numpy as np
import pm_util as pmu
import time
import glob


def cval(tableCell):
    return tableCell.to_list()[0][0]

def cstr(tableCell):
    return str(tableCell.to_list()[0][0])

def cint(tableCell):
    return int(tableCell.to_list()[0][0])

def addRunningSumColumn(table, srcColumn, newColumn):
    src = dt.f[srcColumn]
    src_na = dt.math.isna(src)
    table[src_na, src] = 0
    values = table[:, src].to_list()[0]
    #print("values",values)
    cumsums = np.cumsum(values)
    #print("cumsums",cumsums)
    sumsTable = table[:, dt.f[:].extend({newColumn: 0})]
    sumsTable[:, newColumn] = cumsums
    return sumsTable

def analyzeDaily(fullTable, filter, prefix, postfix, byDateColName):

    print("analyzeDaily prefix='{}' postfix='{}' byDateColName='{}'".format(prefix, postfix, byDateColName))
    #print("analyzeDaily filter='{}' '".format(filter))
    byDate = dt.f[byDateColName]
    #print("----- analyzeDaily:"+postfix)
    #dayTable = fullTable[(dt.f.DatenstandTag >= fromDay) & (dt.f.DatenstandTag < toDay) & (dt.f.IdLandkreis == forIdLandkreis),:]

    dayTable = fullTable[filter,:]

    cases_to_count = dayTable[(dt.f.NeuerFall == 0) | (dt.f.NeuerFall == 1),:]
    cases = cases_to_count[:, [dt.sum(dt.f.AnzahlFall)],dt.by(byDate)]
    cases.names = [byDateColName, prefix+"AnzahlFall"+postfix]
    cases.key = byDateColName
    print("cases rows = {}, cases_to_count = {}".format(cases.nrows, cases_to_count.nrows))
    #print(cases)
    byDayTable = cases

    if byDateColName == "DatenstandTag":
        new_cases_to_count = dayTable[(dt.f.NeuerFall == -1) | (dt.f.NeuerFall == 1),:]
        new_cases = new_cases_to_count[:, [dt.sum(dt.f.AnzahlFall)],dt.by(byDate)]
        new_cases.names = [byDateColName, prefix+"AnzahlFallNeu"+postfix]
        new_cases.key = byDateColName
        print("new_cases rows = {}, new_cases_to_count = {}".format(new_cases.nrows, new_cases_to_count.nrows))
        #new_cases_to_count.to_csv("new_cases_to_count.csv")
        byDayTable = byDayTable[:,:,dt.join(new_cases)]
    else:
        # add days by MeldeTag
        byDayTable.names = {prefix+"AnzahlFall"+postfix: prefix+"AnzahlFallNeu"+postfix}
        byDayTable = addRunningSumColumn(byDayTable, prefix+"AnzahlFallNeu"+postfix, prefix+"AnzahlFall"+postfix)

    dead_to_count = dayTable[(dt.f.NeuerTodesfall == 0) | (dt.f.NeuerTodesfall == 1),:]
    dead = dead_to_count[:, [dt.sum(dt.f.AnzahlTodesfall)],dt.by(byDate)]
    dead.names = [byDateColName, prefix+"AnzahlTodesfall"+postfix]
    dead.key = byDateColName
    #print("dead rows = {}".format(dead.nrows))
    byDayTable = byDayTable[:,:,dt.join(dead)]

    if byDateColName == "DatenstandTag":
        new_dead_to_count = dayTable[(dt.f.NeuerTodesfall == -1) | (dt.f.NeuerTodesfall == 1),:]
        new_dead = new_dead_to_count[:, [dt.sum(dt.f.AnzahlTodesfall)],dt.by(byDate)]
        new_dead.names = [byDateColName, prefix+"AnzahlTodesfallNeu"+postfix]
        new_dead.key = byDateColName
        #print("new_dead rows = {}".format(new_dead.nrows))
        byDayTable = byDayTable[:,:,dt.join(new_dead)]
    else:
        # add days by MeldeTag
        byDayTable.names = {prefix+"AnzahlTodesfall"+postfix: prefix+"AnzahlTodesfallNeu"+postfix}
        byDayTable = addRunningSumColumn(byDayTable, prefix+"AnzahlTodesfallNeu"+postfix, prefix+"AnzahlTodesfall"+postfix)

    byDayTable.key = byDateColName

    if postfix == "" and prefix == "" and byDateColName == "DatenstandTag":
        new_cases_to_count_delay = new_cases_to_count[(dt.f.AnzahlFall > 0), :]  # measure delay only for positive cases
        new_cases_to_count_delay.materialize()
        new_cases_delay = new_cases_to_count_delay[:, [dt.min(dt.f.MeldeDelay), dt.max(dt.f.MeldeDelay),
                                                       dt.mean(dt.f.MeldeDelay), dt.median(dt.f.MeldeDelay),
                                                       dt.sd(dt.f.MeldeDelay), dt.sum(dt.f.AnzahlFall),
                                                       dt.max(dt.f.DatenstandTag)], dt.by(byDate)]
        new_cases_delay.names = ["DatenstandTag",
                                 "PublikationsdauerFallNeu_Min" + postfix, "PublikationsdauerFallNeu_Max" + postfix,
                                 "PublikationsdauerFallNeu_Schnitt" + postfix, "PublikationsdauerFallNeu_Median" + postfix,
                                 "PublikationsdauerFallNeu_StdAbw" + postfix, "PublikationsdauerFallNeu_Fallbasis" + postfix,
                                 "DatenstandTag_Max" + postfix]
        new_cases_delay.key = "DatenstandTag"
        print("new_cases_delay rows = {}, new_cases_to_count_delay = {}".format(new_cases_delay.nrows,
                                                                                new_cases_to_count_delay.nrows))

        recovered_to_count = dayTable[(dt.f.NeuGenesen == 0) | (dt.f.NeuGenesen == 1),:]
        recovered = recovered_to_count[:, [dt.sum(dt.f.AnzahlGenesen)],dt.by(byDate)]
        recovered.names = ["DatenstandTag", "AnzahlGenesen"+postfix]
        recovered.key = "DatenstandTag"
        #print("recovered rows = {}".format(recovered.nrows))

        new_recovered_to_count = dayTable[(dt.f.NeuGenesen == -1) | (dt.f.NeuGenesen == 1),:]
        new_recovered = new_recovered_to_count[:, [dt.sum(dt.f.AnzahlGenesen)],dt.by(byDate)]
        new_recovered.names = ["DatenstandTag", "AnzahlGenesenNeu"+postfix]
        new_recovered.key = "DatenstandTag"
        #print("new_recovered rows = {}".format(new_recovered.nrows))

        byDayTable = byDayTable[:, :, dt.join(recovered)][:, :, dt.join(new_recovered)][:, :,dt.join(new_cases_delay)]
        #byDayTable = byDayTable[:,:,dt.join(recovered)][:,:,dt.join(new_recovered)]\
        #    [:,:,dt.join(new_cases_strict)][:,:,dt.join(new_cases_strict_14)][:,:,dt.join(new_cases_delay)]

    byDayTable.key = byDateColName
    #print("byDayTable rows = {}".format(byDayTable.nrows))
    #print(byDayTable)
    return byDayTable

def filterByDayAndLandkreis(fromDay, toDay, forIdLandkreis, byDateColName):
    byDate = dt.f[byDateColName]
    return (byDate >= fromDay) & (byDate < toDay) & (dt.f.IdLandkreis == forIdLandkreis)

def filterByDayAndCriteria(fromDay, toDay, criteria, byDateColName):
    byDate = dt.f[byDateColName]
    return (byDate >= fromDay) & (byDate < toDay) & criteria

def filterByDay(fromDay, toDay, byDateColName):
    byDate = dt.f[byDateColName]
    return (byDate >= fromDay) & (byDate < toDay)

def analyzeDailyAndMeldeTag(fullTable, fromDay, toDay, byCriteria, criteriaValue, filter, postfix):
    # print("fromDay, toDay",fromDay, toDay)
    # print("byCriteria, criteriaValue",byCriteria, criteriaValue)
    # print("filter:", filter)

    fullfilter = filter & filterByDayAndCriteria(fromDay, toDay, (byCriteria == criteriaValue),"DatenstandTag")
    #print("fullfilter:", fullfilter)
    dayTable = analyzeDaily(fullTable, fullfilter, "", postfix, "DatenstandTag")

    maxDatenstandTag = fullTable[:, dt.f.DatenstandTag].max().to_list()[0][0]
    #print("maxDatenstandTag",maxDatenstandTag)

    latestTable = fullTable[dt.f.DatenstandTag == maxDatenstandTag, :]
    olderTable = fullTable[dt.f.DatenstandTag == maxDatenstandTag-7, :]
    #latestTable.materialize()
    #print("latestTable",latestTable)

    #print(latestTable)
    minMeldeTag = latestTable[:, dt.f.MeldeTag].min().to_list()[0][0]
    maxMeldeTag = latestTable[:, dt.f.MeldeTag].max().to_list()[0][0]
    #print("minMeldeTag,maxMeldeTag",minMeldeTag,maxMeldeTag)

    fullfilter = filter & filterByDayAndCriteria(minMeldeTag, maxMeldeTag+1, (byCriteria == criteriaValue), "MeldeTag")
    #print("fullfilter2:", fullfilter)

    meldeTable = analyzeDaily(latestTable,fullfilter,"MeldeTag_", postfix, "MeldeTag")
    meldeTable.names = {"MeldeTag":"DatenstandTag"}
    meldeTable.key = "DatenstandTag"

    meldeTable7TageAlt = analyzeDaily(olderTable,fullfilter,"MeldeTag_Vor7Tagen_", postfix, "MeldeTag")
    meldeTable7TageAlt.names = {"MeldeTag": "DatenstandTag"}
    meldeTable7TageAlt.key = "DatenstandTag"

    dayTable.key = "DatenstandTag"

    meldeDays = set(meldeTable[:,"DatenstandTag"].to_list()[0])
    meldeDays7old = set(meldeTable7TageAlt[:,"DatenstandTag"].to_list()[0])
    dataDays = set(dayTable[:,"DatenstandTag"].to_list()[0])
    allDays = sorted(list(meldeDays.union(dataDays).union(meldeDays7old)))

    allDaysTable = dt.Frame(allDays)
    allDaysTable.names = ["DatenstandTag"]
    allDaysTable.key = "DatenstandTag"

    #dayTable = dayTable[:, :, dt.join(meldeTable)]
    allDaysTable = allDaysTable[:, :, dt.join(meldeTable)][:, :, dt.join(meldeTable7TageAlt)][:, :, dt.join(dayTable)]
    allDaysTable.key = "DatenstandTag"
    return allDaysTable

def agColName(ag):
    ag_col_postfix = ag.replace("-", "_").replace("+", "Plus")
    return ag_col_postfix

#return new column name based on gender and age group
def fullColName(baseColName,g,ag):
    if g != "":
        baseColName = baseColName + "_G_"+g
    if ag != "":
        baseColName = baseColName + "_AG_"+agColName(ag)
    return baseColName


def analyzeDailyAltersgruppen(fullTable, byDayTable, fromDay, toDay, byCriteria, criteriaValue, filter, Altersgruppen, Geschlechter, postfix):
    #byDayTable = analyzeDaily(fullTable, filter, postfix)
    #print("----- analyzeDailyAltersgruppen:"+postfix)

    for ag in Altersgruppen:
        if ag != "unbekannt":
            print("Analyzing Altergruppe "+ ag)

            fullfilter = filter & (dt.f.Altersgruppe == ag)
            byDayTableAG = analyzeDailyAndMeldeTag(fullTable, fromDay, toDay, byCriteria, criteriaValue, fullfilter, postfix+"_AG_"+agColName(ag))
            byDayTable = byDayTable[:,:,dt.join(byDayTableAG)]
            byDayTable.key = "DatenstandTag"
    return byDayTable

def analyzeDailyAltersgruppenGeschlechter(fullTable, fromDay, toDay, byCriteria, criteriaValue, Altersgruppen, Geschlechter):
    byDayTable = analyzeDailyAndMeldeTag(fullTable, fromDay, toDay, byCriteria, criteriaValue, True, "")
    byDayTable = analyzeDailyAltersgruppen(fullTable, byDayTable, fromDay, toDay, byCriteria, criteriaValue, True, Altersgruppen, Geschlechter,"")
    #byDayTable = byDayTable[:, :, dt.join(byDayTableAG)]
    #return byDayTable
    #print("byDayTable 1", byDayTable.names)
    for g in Geschlechter:
        if g != "unbekannt":
            print("Analyzing Geschlechter "+ g)
            byDayTableG = analyzeDailyAndMeldeTag(fullTable, fromDay, toDay, byCriteria, criteriaValue, (dt.f.Geschlecht == g), "_G_"+g)
            #print("byDayTableG", byDayTableG.names)
            byDayTable = byDayTable[:,:,dt.join(byDayTableG)]
            #print("byDayTable 2", byDayTable.names)
            byDayTable = analyzeDailyAltersgruppen(fullTable, byDayTable,
                                                   fromDay, toDay, byCriteria, criteriaValue, (dt.f.Geschlecht == g),
                                                   Altersgruppen, Geschlechter, "_G_"+g)
            #print("byDayTableAG", byDayTableAG.names)
            #byDayTable = byDayTable[:,:,dt.join(byDayTableAG)]
           # print("byDayTable 3", byDayTable.names)

    return byDayTable


def timeSeries(fullTable, fromDay, toDay, byCriteria, nameColumn, Altersgruppen, Geschlechter):
    good_regions = fullTable[byCriteria >= 0, :]
    regions = good_regions[:, [dt.first(nameColumn)], dt.by(byCriteria)]
    #regions = regions[:5,:]
    print("Creating time series for regions:")
    print(regions)
    dailysByCriteria = {}
    dailysMeldeDatumByCriteria = {}
    start = time.perf_counter()
    for i, lk in enumerate(regions[:, byCriteria].to_list()[0]):
        print("Processing Region '{}'".format(regions[i,nameColumn][0,0]))
        start_region = time.perf_counter()

        pmu.printMemoryUsage("pre analyzeDailyAltersgruppenGeschlechter")
        dailysByCriteria[lk] = analyzeDailyAltersgruppenGeschlechter(fullTable, fromDay, toDay, byCriteria, lk, Altersgruppen, Geschlechter)

        finish = time.perf_counter()
        duration = finish - start
        print("Region took {:.2f} seconds, elapsed {:.2f} minutes, time to completion: {:.2f} minutes or {:.2f} hours".format(
            finish-start_region, duration/60, duration/(i+1) * (regions.nrows - i)/60, duration/(i+1) * (regions.nrows - i)/60/60))

        pmu.printMemoryUsage("post analyzeDailyAltersgruppenGeschlechter")
        print("Done {} of {}, key = {} name = {}".format(i+1, regions.nrows, lk, regions[i,nameColumn][0,0]))
        #if lk >= 0:
        #    break
    return regions, dailysByCriteria

# def makeIncidenceColumn(regionTable, censusTable, g, ag):
#     if ag != "unbekannt" and g != "unbekannt":
#
#         srccolname = fullColName("AnzahlFallNeu", g, ag)
#         newcolname = fullColName("InzidenzFallNeu", g, ag)
#
#         if ag == "":
#             censusPrefix = "Insgesamt"
#         else:
#             censusPrefix = ag
#
#         if g == "":
#             censusPostfix = "-total"
#         else:
#             censusPostfix = "-" + g
#
#         censuscolname = censusPrefix + censusPostfix
#         AnzahlFallNeu_X = dt.f[srccolname]
#         g_ag_size = censusTable[0, censuscolname]
#         print("srccolname:{} newcolname:{} AnzahlFallNeu_X:{} g_ag_size:{}".format(srccolname, newcolname, AnzahlFallNeu_X, g_ag_size))
#         regionTable = regionTable[:, dt.f[:].extend({fullColName("Einwohner",g,ag): g_ag_size})]
#         if censuscolname == "Insgesamt-total":
#             regionTable = regionTable[:, dt.f[:].extend({"Dichte": censusTable[0,"Insgesamt-total"] / dt.f.Flaeche})]
#         regionTable = regionTable[:, dt.f[:].extend({newcolname: (100000.0 * AnzahlFallNeu_X) / g_ag_size})]
#     return regionTable

def makeEinwohnerColumn(regionTable, censusTable, g, ag, afterColumnIndex):
    if ag != "unbekannt" and g != "unbekannt":

        if ag == "":
            censusPrefix = "Insgesamt"
        else:
            censusPrefix = ag

        if g == "":
            censusPostfix = "-total"
        else:
            censusPostfix = "-" + g

        censuscolname = censusPrefix + censusPostfix
        g_ag_size = censusTable[0, censuscolname]
        regionTable = regionTable[:, dt.f[:afterColumnIndex].extend({fullColName("Einwohner",g,ag): g_ag_size})]
    return regionTable

# def makeEinwohnerColumns(regionTable, censusTable, Altersgruppen, Geschlechter):
#     for g in [""] + Geschlechter:
#         for ag in [""] + Altersgruppen:
#             if ag != "unbekannt":
#                 regionTable = makeEinwohnerColumn(regionTable, censusTable,g,ag)
#     return regionTable

def insertEinwohnerColumns(regionTable, censusTable, Altersgruppen, Geschlechter, afterColumnName):
    afterFirstColumnIndex = [i for i, n in enumerate(regionTable.names) if n == afterColumnName][0] + 1
    afterColumnIndex = afterFirstColumnIndex
    print("afterColumnIndex", afterColumnIndex)
    resultTable = regionTable
    for g in [""] + Geschlechter:
        for ag in [""] + Altersgruppen:
            if ag != "unbekannt":
                resultTable = makeEinwohnerColumn(resultTable, censusTable,g,ag, afterColumnIndex)
                afterColumnIndex = afterColumnIndex + 1

    resultTable.cbind(regionTable[:, afterFirstColumnIndex:])
    return resultTable

# def makeIncidenceColumns(regionTable, censusTable, Altersgruppen, Geschlechter):
#     for g in [""] + Geschlechter:
#         for ag in [""] + Altersgruppen:
#             if ag != "unbekannt":
#                 regionTable = makeIncidenceColumn(regionTable, censusTable,g,ag)
#     return regionTable


# def makeIncidenceColumns(regionTable, censusTable, Altersgruppen, Geschlechter):
#     #ag_Berlin = {'A00-A04': 195952, 'A05-A14': 318242, 'A15-A34': 826771, 'A35-A59': 1489424, 'A60-A79': 754139,
#     #             'A80+': 198527, 'unbekannt': 0}
#
#     print(censusTable)
#     regionTable = regionTable[:, dt.f[:].extend({"Einwohner": censusTable[0,"Insgesamt-total"]})]
#     regionTable = regionTable[:, dt.f[:].extend({"Dichte": censusTable[0,"Insgesamt-total"] / dt.f.Flaeche})]
#     regionTable = regionTable[:, dt.f[:].extend({"InzidenzFallNeu": (100000.0 * dt.f.AnzahlFallNeu) / censusTable[0,"Insgesamt-total"]})]
#
#     for g in [""]+Geschlechter:
#         for ag in [""]+Altersgruppen:
#             if ag != "unbekannt":
#                 srccolname = "AnzahlFallNeu_AG_" + ag
#                 newcolname = "InzidenzFallNeu_" + ag
#                 censuscolname = ag + "-total"
#                 AnzahlFallNeu_X = dt.f[srccolname]
#                 ag_size = censusTable[0,censuscolname]
#                 #print("srccolname:{} newcolname:{} AnzahlFallNeu_X:{} ag_size:{}".format(srccolname, newcolname, AnzahlFallNeu_X, ag_size))
#                 regionTable = regionTable[:, dt.f[:].extend({"Einwohner_"+ag: ag_size})]
#                 regionTable = regionTable[:, dt.f[:].extend({newcolname: (100000.0 * AnzahlFallNeu_X) / ag_size})]
#     #print(regionTable)
#     return regionTable

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


def moveCols(table, cols, afterColumnName):
    afterFirstColumnIndex = [i for i, n in enumerate(table.names) if n == afterColumnName][0] + 1
    print("move cols {} afterColumnIndex {}".format(cols, afterFirstColumnIndex))
    resultTable = table[:, dt.f[:afterFirstColumnIndex].extend(table[:, cols])]
    del table[:, cols]
    resultTable.cbind(table[:, afterFirstColumnIndex:])
    return resultTable

def updateOldTable(table, withTable):
    #print("table.names     len={}, names={}".format(len(table.names), table.names))
    #print()
    #print("withTable.names len={}, names={}".format(len(withTable.names), withTable.names))

    incidenceCols = [col for col in table.names if "Inzidenz" in col]
    if len(incidenceCols)>0:
        del table[:, incidenceCols]
        del table[:, "Dichte"]
        EinwohnerCols = [col for col in table.names if "Einwohner" in col]
        table = moveCols(table, EinwohnerCols, "Flaeche" )

    #print()
    #print("table.names     len={}, names={}".format(len(table.names), table.names))

    if table.names != withTable.names:
        print("BAD: table# {} !=: {} withTable#".format(len(table.names),len(withTable.names)))

        for i, n in enumerate(table.names):
            if n == withTable.names[i]:
                print("OK: {}: {}".format(i,n))
            else:
                print("BAD: {} : {} != {}".format(i,n, withTable.names[i]))
        #print("table.names", table.names)
        #print("withTable.names", withTable.names)
        print("#ERROR: Can't update, table name mismatch ")
        exit(1)

    # update  with new rows
    rowsAvailable = withTable[~dt.math.isna(dt.f.AnzahlFall),:]
    #print("rowsAvailable",rowsAvailable)
    #table.to_csv("oldTable.csv")
    #withTable.to_csv("withTable.csv")
    #rowsAvailable.to_csv("rowsAvailable.csv")

    rowsAvailableList = rowsAvailable[:,dt.f.DatenstandTag].to_list()[0]
    rowsOldList = table[:,dt.f.DatenstandTag].to_list()[0]
    newRowsList = list(np.setdiff1d(rowsAvailableList,rowsOldList))

    if len(newRowsList) > 0:
        for row in newRowsList:
            rowToAdd = rowsAvailable[dt.f.DatenstandTag == int(row),:]
            table.rbind(rowToAdd)
        table = table.sort("DatenstandTag")
    else:
        print("updateOldTable: No rows to add")
        exit(1)

    # check result
    tableDates = table[:,"DatenstandTag"].to_list()[0]
    withTableDates = withTable[:,"DatenstandTag"].to_list()[0]

    # update MeldeDatum columns
    meldeTagCols = [col for col in table.names if "MeldeTag_" in col]
    #print("meldeTagCols", meldeTagCols)
    # update only rows that are contained in withTable
    meldeRowMask = [day in withTableDates for day in tableDates]
    #print(meldeRowMask)

    table[meldeRowMask,meldeTagCols] = withTable[:,meldeTagCols]
    return table

def analyze(fullTable, args, oldTables):
    #fullTable = fullTable[dt.f.DatenstandTag <= 387,:]

    print("Analyzing")
    pmu.printMemoryUsage("begin analyze")
    print("Keys:")
    print(fullTable.keys())
    print(list(zip(fullTable.names, fullTable.stypes)))

    daysInfullTable = dt.unique(fullTable[:, "DatenstandTag"]).to_list()[0]
    firstDumpDay = min(daysInfullTable)
    lastDumpDay = max(daysInfullTable)
    maxMeldeDay = cint(fullTable[:,"MeldeTag"].max())
    if maxMeldeDay > lastDumpDay:
        print("Future Date in Meldetag ({}), clipping to yesterday, Datenstandtag-1 = {}".format(maxMeldeDay, lastDumpDay))
        fullTable["MeldeTag">=lastDumpDay,"MeldeTag"] = lastDumpDay -1

    print("firstDumpDay", firstDumpDay)
    print("lastDumpDay",lastDumpDay)
    print("maxMeldeDay",maxMeldeDay)

    fromDay = firstDumpDay
    toDay = lastDumpDay+1
    #fromDay = lastDumpDay-1
    if len(oldTables)>0:

        # calculate which rows are needed for the update
        daysInOldTables = dt.unique(oldTables[0][:, "DatenstandTag"]).to_list()[0]
        newDays = sorted(list(set(daysInfullTable).difference(set(daysInOldTables))))
        print("newDays",newDays)
        if len(newDays) == 0:
            print("Nothing to update")
            exit(9)
        minNewDay = min(newDays)
        maxNewDay = max(newDays)
        minNewDay7daysAgo = minNewDay - 7
        maxNewDay7daysAgo = maxNewDay - 7

        fullTable = fullTable[((dt.f.DatenstandTag >= minNewDay) & (dt.f.DatenstandTag <= maxNewDay)) |
                                ((dt.f.DatenstandTag >= minNewDay7daysAgo) & (dt.f.DatenstandTag <= maxNewDay7daysAgo)),:]
        #fullTable.materialize()
        daysInfullTable = dt.unique(fullTable[:, "DatenstandTag"]).to_list()[0]
        print("daysInfullTable",daysInfullTable)

    fullTable = fullTable[:, dt.f[:].extend({"MeldeDelay": dt.f.DatenstandTag-dt.f.MeldeTag-1})]
    fullTable = fullTable[:, dt.f[:].extend({"RefDelay": dt.f.DatenstandTag-dt.f.RefTag-1})]
    #fullTable.materialize()

    Altersgruppen = []
    if args.agegroups:
        Altersgruppen = dt.unique(fullTable[:,"Altersgruppe"]).to_list()[0]

    print("Altersgruppen", Altersgruppen)

    Geschlechter = []
    if args.gender:
        Geschlechter = dt.unique(fullTable[:,"Geschlecht"]).to_list()[0]
    print("Geschlechter", Geschlechter)

    census = dt.fread("CensusByRKIAgeGroups.csv")
    censusDeutschland = census[dt.f.Name == "Deutschland",:]
    print(censusDeutschland)

    flaechen = loadFlaechen()

    print("Processing 'Deutschland'")
    pmu.printMemoryUsage("begin Deutschland")

    deutschland = analyzeDailyAltersgruppenGeschlechter(fullTable, fromDay, toDay, True, True, Altersgruppen, Geschlechter)
    deutschland = insertDates(deutschland)
    deutschland = insertRegionInfo(deutschland, 0, "Deutschland", "BR", 0, "Deutschland", flaechen[0])
    deutschland = insertEinwohnerColumns(deutschland, censusDeutschland, Altersgruppen, Geschlechter, "Flaeche")

    print(deutschland)
    pmu.printMemoryUsage("pre makeIncidenceColumns")

    #deutschland = makeIncidenceColumns(deutschland, censusDeutschland, Altersgruppen, Geschlechter)
    #print(deutschland)
    if len(oldTables) > 0: deutschland = updateOldTable(oldTables[0], deutschland)
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
            bundeslaender_numbers[bl_id] = insertDates(bundeslaender_numbers[bl_id])
            bundeslaender_numbers[bl_id] = insertRegionInfo(bundeslaender_numbers[bl_id], bl_id, bl_name, "BL", bl_id, bl_name, flaechen[0])
            censusBL = census[dt.f.IdLandkreis == bl_id, :]
            bundeslaender_numbers[bl_id] = insertEinwohnerColumns(bundeslaender_numbers[bl_id], censusBL, Altersgruppen, Geschlechter, "Flaeche")
            if len(oldTables) > 0:
                bundeslaender_numbers[bl_id] = updateOldTable(oldTables[bl_id], bundeslaender_numbers[bl_id])
            print(censusBL)

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
        if lk_name == "LK Saarpfalz-Kreis":
            lk_name = "LK Saar-Pfalz-Kreis"

        if lk_id > 0:
            censusLK = census[dt.f.IdLandkreis == lk_id, :]
            bl_name = censusLK[0,dt.f.Bundesland].to_list()[0][0]
            bl_id = censusLK[0, dt.f.IdBundesland].to_list()[0][0]
            lk_typ = landKreisTyp(lk_id, lk_name)

            landkreise_numbers[lk_id] = insertDates(landkreise_numbers[lk_id])
            landkreise_numbers[lk_id] = insertRegionInfo(landkreise_numbers[lk_id], lk_id, lk_name, lk_typ, bl_id,
                                                             bl_name, flaechen[lk_id])
            #print(censusLK)
            landkreise_numbers[lk_id] = insertEinwohnerColumns(landkreise_numbers[lk_id], censusLK, Altersgruppen,
                                                                Geschlechter, "Flaeche")
            if len(oldTables) > 0:
                landkreise_numbers[lk_id] = updateOldTable(oldTables[lk_id], landkreise_numbers[lk_id])

        pmu.printMemoryUsage("pre save {}".format(lk_name))
        pmu.saveCsvTable(landkreise_numbers[lk_id], "series-{}-{}.csv".format(lk_id, lk_name), args.outputDir)
    #print(landKreise)

    return fullTable

def main():
    dt.options.progress.enabled = False

    parser = argparse.ArgumentParser(description='Fast queries on all data')
    parser.add_argument('file', metavar='fileName', type=str, nargs='?',
                        help='.Full unified NPGEO COVID19 Germany data as .csv or .jay file',
                        default="archive_v2/all-data.jay")
    parser.add_argument('-d', '--output-dir', dest='outputDir', default="series")
    parser.add_argument('-i', '--incremental-update-dir', dest='incrementalUpdateDir', default="")
    parser.add_argument("--agegroups", help="also create columns for all seperate age groups", action="store_true")
    parser.add_argument("--gender", help="also create columns for all seperate gender groups", action="store_true")

    args = parser.parse_args()
    #print(args)
    pmu.printMemoryUsage("after start")
    print("Loading " + args.file)
    fullTable = dt.fread(args.file)
    print("Loading done loading table from ‘{}‘, rows: {} cols: {}".format(args.file, fullTable.nrows, fullTable.ncols))
    pmu.printMemoryUsage("after load")

    oldTables = {}
    if args.incrementalUpdateDir != "":
        updateFiles = sorted(glob.glob(args.incrementalUpdateDir+"/*.csv"))
        for f in updateFiles:
            if not f.endswith("--nicht erhoben-.csv"):
                table = dt.fread(f)
                print("Load {}".format(f))
                lkID = table[0,"IdLandkreis"]#.to_list()[0][0]
                print("lkID {}".format(lkID))
                oldTables[lkID] = table

    if False:
        print("Materializing fullTable")
        fullTable.materialize(to_memory=True)
        pmu.printMemoryUsage("after materialize")

    analyze(fullTable, args, oldTables)

if __name__ == "__main__":
    # execute only if run as a script
    main()