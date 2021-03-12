import argparse
import datatable as dt
import os
import glob


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
    newTable = table[:, dt.f[:].extend({newColumn: (src+5) / (dt.shift(src, n=7)+5)})]
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
    newTable = table[:, dt.f[:].extend({newColumn: 100000 / (incidence * darkFactor)})]
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

def addIncidenceColumn(table, srcColumn, newColumn):
    src = dt.f[srcColumn]
    #print("srcColumn",srcColumn)
    #print(list(zip(table.names, table.stypes)))
    newTable = table[:, dt.f[:].extend({newColumn: src / dt.f.Einwohner * 100000})]
    return newTable

def addMultipliedColumn(table, srcColumn, newColumn, factor):
    src = dt.f[srcColumn]
    #print("srcColumn",srcColumn)
    #print(list(zip(table.names, table.stypes)))
    newTable = table[:, dt.f[:].extend({newColumn: src * factor})]
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

def addIncidences(table):
    table = addIncidenceColumn(table, "AnzahlTodesfallNeu", "InzidenzTodesfallNeu")

    candidatesColumns = [name for name in table.names if ("AnzahlFall" in name or "AnzahlTodesfall" in name) and not "Neu" in name]
    #print("addIncidences candidates:", candidatesColumns)
    for c in candidatesColumns:
        newColName = c.replace("Anzahl","Inzidenz")
        table = addIncidenceColumn(table, c, newColName)

    return table

def enhanceDatenstandTagMax(table):
    for row in range(table.nrows):
        table[row,"DatenstandTag-Max"] = table[:row+1,"DatenstandTag-Max"].max()
    return table


def addMoreMetrics(table):
    table = enhanceDatenstandTagMax(table)
    table = addDifferenceColumn(table, "DatenstandTag-Max", "DatenstandTag", "DatenstandTag-Diff")
    table = addIncidenceColumn(table, "AnzahlFallNeu-Meldung-letze-7-Tage-7-Tage", "InzidenzFallNeu-Meldung-letze-7-Tage-7-Tage")
    table = addDifferenceColumn(table, "AnzahlFallNeu-7-Tage", "AnzahlFallNeu-Meldung-letze-7-Tage-7-Tage", "AnzahlFallNeu-7-Tage-Dropped")
    table = addRatioColumn(table, "AnzahlFallNeu-7-Tage-Dropped", "AnzahlFallNeu-7-Tage", "ProzentFallNeu-7-Tage-Dropped", factor=100)
    table = addMultipliedColumn(table, "MeldeDauerFallNeu-Min", "MeldeDauerFallNeu-Min-Neg", factor=-1)

    return table

def add7DayAverages(table):
    #print(table.names)
    candidatesColumns = [name for name in  table.names if "Neu" in name]
    #print(candidatesColumns)
    for c in candidatesColumns:
        table = add7dSumColumn(table, c, c+"-7-Tage")
        table = add7dTrendColumn(table, c+"-7-Tage", c+"-7-Tage-Trend")
        if c in ["AnzahlFallNeu","InzidenzFallNeu","AnzahlTodesfallNeu","InzidenzTodesfallNeu"]:
            table = add7dBeforeColumn(table, c + "-7-Tage", c + "-7-Tage-7-Tage-davor")
        if c in ["InzidenzFallNeu", "InzidenzTodesfallNeu"]:
            table = add7dWTrendColumn(table, c+"-7-Tage", c+"-7-Tage-Trend-Spezial")
            if c in ["InzidenzFallNeu"]:
                table = add7dRColumn(table, c + "-7-Tage-Trend", c + "-7-Tage-R")
                table = addPredictionsColumn(table, c + "-7-Tage", c + "-7-Tage-Trend-Spezial", c + "-Prognose-1-Wochen", 1)
                table = addPredictionsColumn(table, c + "-7-Tage", c + "-7-Tage-Trend-Spezial", c + "-Prognose-2-Wochen", 2)
                table = addPredictionsColumn(table, c + "-7-Tage", c + "-7-Tage-Trend-Spezial", c + "-Prognose-4-Wochen", 4)
                table = addPredictionsColumn(table, c + "-7-Tage", c + "-7-Tage-Trend-Spezial", c + "-Prognose-8-Wochen", 8)

                table = addGoalColumn(table, c + "-7-Tage", c + "-7-Tage-Trend-Spezial", c + "-Tage-bis-50", 50)
                table = addGoalColumn(table, c + "-7-Tage", c + "-7-Tage-Trend-Spezial", c + "-Tage-bis-100", 100)

                table = addRiskColumn(table,"InzidenzFallNeu-Prognose-1-Wochen", "Kontaktrisiko", 3.5)
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

    numericColumns = [name for name in table.names if "Anzahl" in name or "Inzidenz" in name or "DatenstandTag-Max" in name]
    fillEmptyCellsWithZeroes(table, numericColumns)

    newTable = addIncidences(table)
    newTable = add7DayAverages(newTable)
    newTable = addMoreMetrics(newTable)

    path = os.path.normpath(inputFile)
    fileName = path.split(os.sep)[-1]
    newFile = destDir + "/"+"enhanced-"+fileName
    newTable.to_csv(newFile)


def main():
    #test()
    #exit(0)
    parser = argparse.ArgumentParser(description='Enahnce by adding columns with 7-day-averages, changes, predictions and risk')
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