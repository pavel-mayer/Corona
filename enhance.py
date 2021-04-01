import argparse
import datatable as dt
import os
import glob

from datatable import dt, f, by, ifelse, update

def add7dSumColumn(table, srcColumn, newColumn):
    src = dt.f[srcColumn]
    #src_na = dt.math.isna(src)
    src_na = dt.math.isna(src)
    table[src_na, src] = 0

    newTable = table[:, dt.f[:].extend({newColumn: src + dt.shift(src, n=1) + dt.shift(src, n=2) + dt.shift(src, n=3)+
                                                   dt.shift(src, n=4)+ dt.shift(src, n=5)+ dt.shift(src, n=6)})]
    #print(newTable)
    return newTable

def add7dAvrgColumn(table, srcColumn, newColumn):
    src = dt.f[srcColumn]
    newTable = table[:, dt.f[:].extend({newColumn: (src + dt.shift(src, n=1) + dt.shift(src, n=2) + dt.shift(src, n=3)+
                                                   dt.shift(src, n=4)+ dt.shift(src, n=5)+ dt.shift(src, n=6))/7})]
    #print(newTable)
    return newTable

def add7dBeforeColumn(table, srcColumn, newColumn):
    src = dt.f[srcColumn]
    newTable = table[:, dt.f[:].extend({newColumn: dt.shift(src, n=7)})]
    #print(newTable)
    return newTable

def add7dTrendColumn(table, srcColumn, newColumn):
    src = dt.f[srcColumn]
    newTable = table[:, dt.f[:].extend({newColumn: (src / dt.shift(src, n=7))})]
    #print(newTable)
    return newTable

def add7dWTrendColumn(table, srcColumn, newColumn):
    src = dt.f[srcColumn]
    newTable = table[:, dt.f[:].extend({newColumn: (dt.math.abs(src)+5) / (dt.shift(dt.math.abs(src), n=7)+5)})]
    return newTable


def add7dRColumn(table, srcColumn, newColumn):
    src = dt.f[srcColumn]
    newTable = table[:, dt.f[:].extend({newColumn: dt.math.pow(src, (4/7))})]
    #print(newTable)
    return newTable

def addPredictionsColumn(table, incidenceColumn, trendColumn, newColumn, weeks):
    incidence = dt.f[incidenceColumn]
    trend = dt.f[trendColumn]
    newTable = table[:, dt.f[:].extend({newColumn: (incidence * dt.math.pow(trend, weeks))})]
    #print(newTable)
    return newTable

def addRiskColumn(table, incidencePrognosisColumn, newColumn, darkFactor):
    incidence = dt.f[incidencePrognosisColumn]
    newTable = table[:, dt.f[:].extend({newColumn: dt.ifelse(incidence <= 0, 99999,100000 / (incidence * darkFactor))})]
    #print(newTable)
    return newTable

def addDifferenceColumn(table, theColumn, minusColumn, newColumn):
    theValue = dt.f[theColumn]
    minusValue = dt.f[minusColumn]
    newTable = table[:, dt.f[:].extend({newColumn: theValue - minusValue})]
    #print(newTable)
    return newTable

def addRatioColumn(table, theColumn, divideByColumn, newColumn, factor=1):
    theValue = dt.f[theColumn]
    divisor = dt.f[divideByColumn]
    newTable = table[:, dt.f[:].extend({newColumn: (theValue * factor) / divisor })]
    #print(newTable)
    return newTable

def addIncidenceColumn(table, srcColumn, einwohnerColumn, newColumn):
    #print(srcColumn, einwohnerColumn, newColumn)
    src = dt.f[srcColumn]
    einwohner = dt.f[einwohnerColumn]
    #print("srcColumn",srcColumn)
    #print(list(zip(table.names, table.stypes)))
    newTable = table[:, dt.f[:].extend({newColumn: src / einwohner * 100000})]
    return newTable

def addMultipliedColumn(table, srcColumn, newColumn, factor):
    src = dt.f[srcColumn]
    #print("srcColumn",srcColumn)
    #print(list(zip(table.names, table.stypes)))
    newTable = table[:, dt.f[:].extend({newColumn: src * factor})]
    return newTable

def addShiftedColumn(table, srcColumn, newColumn, shift_rows):
    src = dt.f[srcColumn]
    newTable = table[:, dt.f[:].extend({newColumn: dt.shift(src, n=shift_rows)})]
    #print(newTable)
    return newTable

# goal = incidence * trend ^ t
# trend ^ t = goal/incidence
# t = log(trend, goal/incidence)
# t = ln(res/incidence)/ln(trend)
def addGoalColumn(table, incidenceColumn, trendColumn, newColumn, goal, scale = 7):
    incidence = dt.f[incidenceColumn]
    trend = dt.f[trendColumn]
        # print(list(zip(table.names, table.stypes)))
    newTable = table[:, dt.f[:].extend({newColumn: dt.math.log(goal / incidence) / dt.math.log(trend) * scale})]
    return newTable

def fillEmptyCellsWithZeroes(table, columns):
    for col in columns:
        src = dt.f[col]
        src_na = dt.math.isna(src)
        table[src_na, src] = 0
    return table

def test():
    table = dt.Frame({"object": [1, 1, 1, 2, 2, 3,4,5,6,7,8,9,10,11,12,13,14],
                      "period": [1, 2, 4, 4, 23,2,5,7,2,7,8,9,5, 18,35,17,15]})
    table = add7dSumColumn(table, "period", "sumPeriod")
    add7dAvrgColumn(table, "period", "avPeriod")

def addMeldeTagShift(table):
    candidatesColumns = [name for name in table.names if ("MeldeTag_Anzahl" in name) ]
    for c in candidatesColumns:
        table = addShiftedColumn(table, c, c+"_Gestern",1)

    candidatesColumns = [name for name in table.names if ("MeldeTag_Vor7Tagen_Anzahl" in name)]
    for c in candidatesColumns:
        table = addShiftedColumn(table, c, c + "_Vor8Tagen", 8)
    return table

def ifcontains(name, stringlist):
    for string in stringlist:
        if string in name:
            return string
    return ""

def einwohnerColName(colName):
    gender = ifcontains(colName,["_G_W","_G_M"])
    age = ifcontains(colName,["_AG_A00_A04","_AG_A05_A14","_AG_A15_A34","_AG_A35_A59","_AG_A60_A79","_AG_A80Plus"])
    result = "Einwohner"+gender+age
    return result

def addIncidences(table):
    #table = addIncidenceColumn(table, "AnzahlTodesfallNeu", "Einwohner", "InzidenzTodesfallNeu")

    candidatesColumns = [name for name in table.names if ("AnzahlFall" in name or "AnzahlTodesfall" in name)]
    #   print("addIncidences candidates:", candidatesColumns)
    for c in candidatesColumns:
        newColName = c.replace("Anzahl","Inzidenz")
        ewc = einwohnerColName(c)
        #print("ewc:"+ewc)
        table = addIncidenceColumn(table, c, ewc, newColName)
        if "AnzahlTodesfall" in c:
            newColName = c.replace("AnzahlTodesfall", "Fallsterblichkeit_Prozent")
            caseColName = c.replace("AnzahlTodesfall", "AnzahlFall")
            table = addRatioColumn(table, c, caseColName, newColName, factor=100)

    return table

def enhanceDatenstandTagMax(table):
    for row in range(table.nrows):
        table[row,"DatenstandTag_Max"] = table[:row+1,"DatenstandTag_Max"].max()
    return table


def addMoreMetrics(table):
    #print(table.names)
    table = enhanceDatenstandTagMax(table)
    table = addDifferenceColumn(table, "DatenstandTag_Max", "DatenstandTag", "DatenstandTag_Diff")

    #table = addIncidenceColumn(table, "MeldeTag_AnzahlFallNeu_Gestern_7TageSumme", "Einwohner", "MeldeTag_InzidenzFallNeu_Gestern_7TageSumme")
    table = addDifferenceColumn(table, "AnzahlFallNeu_7TageSumme", "MeldeTag_AnzahlFallNeu_Gestern_7TageSumme", "AnzahlFallNeu_7TageSumme_Dropped")
    table = addRatioColumn(table, "AnzahlFallNeu_7TageSumme_Dropped", "AnzahlFallNeu_7TageSumme", "ProzentFallNeu_7TageSumme_Dropped", factor=100)

    #table = addIncidenceColumn(table, "MeldeTag_Vor7Tagen_AnzahlFallNeu_Vor8Tagen_7TageSumme", "Einwohner", "MeldeTag_Vor7Tagen_InzidenzFallNeu_Vor8Tagen_7TageSumme")
    table = addRatioColumn(table, "MeldeTag_InzidenzFallNeu_Gestern_7TageSumme", "MeldeTag_Vor7Tagen_InzidenzFallNeu_Vor8Tagen_7TageSumme",
                           "MeldeTag_InzidenzFallNeu_Trend")
    table = add7dRColumn(table, "MeldeTag_InzidenzFallNeu_Trend", "MeldeTag_InzidenzFallNeu_R")
    table = addPredictionsColumn(table, "MeldeTag_InzidenzFallNeu_Gestern_7TageSumme", "MeldeTag_InzidenzFallNeu_Trend",
                                 "MeldeTag_InzidenzFallNeu_Prognose_4_Wochen",4)

    table = addMultipliedColumn(table, "PublikationsdauerFallNeu_Min", "PublikationsdauerFallNeu_Min_Neg", factor=-1)

    return table

def add7DayAverages(table):
    #print(table.names)
    candidatesColumns = [name for name in  table.names if "Neu" in name]
    #print(candidatesColumns)
    for c in candidatesColumns:
        if not ("Publikationsdauer" in c):
            table = add7dSumColumn(table, c, c+"_7TageSumme")
        if not ("MeldeTag" in c or "Publikationsdauer" in c):
            table = add7dTrendColumn(table, c+"_7TageSumme", c+"_7TageSumme_Trend")
        if c in ["AnzahlFallNeu","InzidenzFallNeu","AnzahlTodesfallNeu","InzidenzTodesfallNeu"]:
            table = add7dBeforeColumn(table, c + "_7TageSumme", c + "_7TageSumme_7_Tage_davor")
        if c in ["InzidenzFallNeu", "InzidenzTodesfallNeu"]:
            table = add7dWTrendColumn(table, c+"_7TageSumme", c+"_7TageSumme_Trend_Spezial")
            if c in ["InzidenzFallNeu"]:
                table = add7dRColumn(table, c + "_7TageSumme_Trend", c + "_7TageSumme_R")
                table = addPredictionsColumn(table, c + "_7TageSumme", c + "_7TageSumme_Trend_Spezial", c + "_Prognose_1_Wochen", 1)
                table = addPredictionsColumn(table, c + "_7TageSumme", c + "_7TageSumme_Trend_Spezial", c + "_Prognose_2_Wochen", 2)
                table = addPredictionsColumn(table, c + "_7TageSumme", c + "_7TageSumme_Trend_Spezial", c + "_Prognose_4_Wochen", 4)
                table = addPredictionsColumn(table, c + "_7TageSumme", c + "_7TageSumme_Trend_Spezial", c + "_Prognose_8_Wochen", 8)

                table = addGoalColumn(table, c + "_7TageSumme", c + "_7TageSumme_Trend_Spezial", c + "_Tage_bis_50", 50)
                table = addGoalColumn(table, c + "_7TageSumme", c + "_7TageSumme_Trend_Spezial", c + "_Tage_bis_100", 100)

                table = addRiskColumn(table,"InzidenzFallNeu_Prognose_1_Wochen", "Kontaktrisiko", 3.5)
    return table

def enhance(inputFile, destDir="."):
    #print("Loading {}...".format(inputFile))

    table = dt.fread(inputFile)
    #print(table.names)
    #print(table.stypes)
    #print(list(zip(table.names, table.stypes)))
    #print("----------------------------------------")

    typedict = {}
    for i, name in enumerate(table.names):
        if table.stypes[i] == dt.stype.bool8:
            typedict[name] = dt.stype.int32
    #print(typedict)
    #print("Loading {}... - for real now".format(inputFile))
    table = dt.fread(inputFile, columns=typedict)
    #print(table.names)
    #print(table.stypes)

    numericColumns = [name for name in table.names if "Anzahl" in name or "Inzidenz" in name or "DatenstandTag_Max" in name]
    fillEmptyCellsWithZeroes(table, numericColumns)

    newTable = addMeldeTagShift(table)
    newTable = addIncidences(newTable)
    newTable = add7DayAverages(newTable)
    newTable = addMoreMetrics(newTable)

    path = os.path.normpath(inputFile)
    fileName = path.split(os.sep)[-1]
    newFile = destDir + "/"+"enhanced-"+fileName

    newTable.to_csv(newFile)


    # testCols = ["MeldeTag_InzidenzFall_G_M_AG_A00_A04_Gestern",
    #             "MeldeTag_InzidenzFall_G_M_AG_A05_A14_Gestern",
    #             "MeldeTag_InzidenzFall_G_M_AG_A15_A34_Gestern",
    #             "MeldeTag_InzidenzFall_G_M_AG_A35_A59_Gestern",
    #             "MeldeTag_InzidenzFall_G_M_AG_A60_A79_Gestern",
    #             "MeldeTag_InzidenzFall_G_M_AG_A80Plus_Gestern",
    #             "MeldeTag_InzidenzFall_G_W_AG_A00_A04_Gestern",
    #             "MeldeTag_InzidenzFall_G_W_AG_A05_A14_Gestern",
    #             "MeldeTag_InzidenzFall_G_W_AG_A15_A34_Gestern",
    #             "MeldeTag_InzidenzFall_G_W_AG_A35_A59_Gestern",
    #             "MeldeTag_InzidenzFall_G_W_AG_A60_A79_Gestern",
    #             "MeldeTag_InzidenzFall_G_W_AG_A80Plus_Gestern",]
    # testTable = newTable[dt.f.DatenstandTag == 391, testCols]
    # print(testTable)
    # testTable.to_csv(fileName+"-test-extract.csv")
    #
    # testCols2 = ["Einwohner_AG_A00_A04",
    #             "InzidenzFallNeu_AG_A00_A04",
    #             "Einwohner_AG_A05_A14",
    #             "InzidenzFallNeu_AG_A05_A14",
    #             "Einwohner_AG_A15_A34",
    #             "InzidenzFallNeu_AG_A15_A34",
    #             "Einwohner_AG_A35_A59",
    #             "InzidenzFallNeu_AG_A35_A59",
    #             "Einwohner_AG_A60_A79",
    #             "InzidenzFallNeu_AG_A60_A79",
    #             "Einwohner_AG_A80Plus",]
    # testTable = newTable[dt.f.DatenstandTag == 391, testCols2]
    # print(testTable)

def main():
    #test()
    #exit(0)
    parser = argparse.ArgumentParser(description='Enhance by adding columns with 7-day-averages, changes, predictions and risk')
    parser.add_argument('files', metavar='fileName', type=str, nargs='+',
                        help='.csv-File produced by database.py')
    parser.add_argument('-d', '--output-dir', dest='outputDir', default=".")
    args = parser.parse_args()
    print(args)
    for fa in args.files:
        files = sorted(glob.glob(fa))

        for f in files:
            if not f.endswith("--nicht erhoben-.csv"):
                print("Enhancing {}".format(f))
                enhance(f, args.outputDir)

if __name__ == "__main__":
    # execute only if run as a script
    main()