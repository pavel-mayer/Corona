import argparse
import datatable as dt
import glob


# def add7dSumColumn(table, srcColumn, newColumn):
#     src = dt.f[srcColumn]
#     newTable = table[:, dt.f[:].extend({newColumn: src + dt.shift(src, n=1) + dt.shift(src, n=2) + dt.shift(src, n=3)+
#                                                    dt.shift(src, n=4)+ dt.shift(src, n=5)+ dt.shift(src, n=6)})]
#     #print(newTable)
#     return newTable
#
# def add7dAvrgColumn(table, srcColumn, newColumn):
#     src = dt.f[srcColumn]
#     newTable = table[:, dt.f[:].extend({newColumn: (src + dt.shift(src, n=1) + dt.shift(src, n=2) + dt.shift(src, n=3)+
#                                                    dt.shift(src, n=4)+ dt.shift(src, n=5)+ dt.shift(src, n=6))/7})]
#     #print(newTable)
#     return newTable
#
# def add7dChangeColumn(table, srcColumn, newColumn):
#     src = dt.f[srcColumn]
#     newTable = table[:, dt.f[:].extend({newColumn: (src / dt.shift(src, n=7))})]
#     #print(newTable)
#     return newTable
#
# def add7dWChangeColumn(table, srcColumn, newColumn):
#     src = dt.f[srcColumn]
#     newTable = table[:, dt.f[:].extend({newColumn: (src+5) / (dt.shift(src, n=7)+5)})]
#     return newTable
#
#
# def add7dRColumn(table, srcColumn, newColumn):
#     src = dt.f[srcColumn]
#     newTable = table[:, dt.f[:].extend({newColumn: dt.math.pow(src, (4/7))})]
#     #print(newTable)
#     return newTable
#
# def addPredictionsColumn(table, incidenceColumn, changeColumn, newColumn, weeks):
#     incidence = dt.f[incidenceColumn]
#     change = dt.f[changeColumn]
#     newTable = table[:, dt.f[:].extend({newColumn: (incidence * dt.math.pow(change, weeks))})]
#     #print(newTable)
#     return newTable
#
# def addRiskColumn(table, incidencePrognosisColumn, newColumn, darkFactor):
#     incidence = dt.f[incidencePrognosisColumn]
#     newTable = table[:, dt.f[:].extend({newColumn: 100000 / (incidence * darkFactor)})]
#     #print(newTable)
#     return newTable
#
# def test():
#     table = dt.Frame({"object": [1, 1, 1, 2, 2, 3,4,5,6,7,8,9,10,11,12,13,14],
#                       "period": [1, 2, 4, 4, 23,2,5,7,2,7,8,9,5, 18,35,17,15]})
#     table = add7dSumColumn(table, "period", "sumPeriod")
#     add7dAvrgColumn(table, "period", "avPeriod")
#
# def add7DayAverages(table):
#     print(table.names)
#     candidatesColumns = [name for name in  table.names if "Neu" in name or "Inzidenz" in name]
#     print(candidatesColumns)
#     for c in candidatesColumns:
#         table = add7dSumColumn(table, c, c+"-7d")
#         table = add7dChangeColumn(table, c+"-7d", c+"-7d-Change")
#         if c in ["InzidenzFallNeu", "InzidenzTodesfallNeu"]:
#             table = add7dWChangeColumn(table, c+"-7d", c+"-7dW")
#             if c in ["InzidenzFallNeu"]:
#                 table = add7dRColumn(table, c + "-7d-Change", c + "-7d-R")
#                 table = addPredictionsColumn(table, c + "-7d", c + "-7dW", c + "-Prog1W", 1)
#                 table = addPredictionsColumn(table, c + "-7d", c + "-7dW", c + "-Prog2W", 2)
#                 table = addPredictionsColumn(table, c + "-7d", c + "-7dW", c + "-Prog4W", 4)
#                 table = addPredictionsColumn(table, c + "-7d", c + "-7dW", c + "-Prog8W", 8)
#                 table = addRiskColumn(table,"InzidenzFallNeu-Prog1W", "Risk", 3.5)
#     return table
#
# def enhance(inputFile, destDir="."):
#     table = dt.fread(inputFile)
#     newTable = add7DayAverages(table)
#     print(newTable)
#
#     path = os.path.normpath(inputFile)
#     fileName = path.split(os.sep)[-1]
#     newFile = destDir + "/"+"enhanced-"+fileName
#     newTable.to_csv(newFile)


def main():
    parser = argparse.ArgumentParser(description='Gather daily tables and save in a single file')
    parser.add_argument('files', metavar='fileName', type=str, nargs='+',
                        help='.csv-File produced by enhance.py')
    parser.add_argument('-o', '--output-file', dest='outputFile', default="all-series.csv")
    args = parser.parse_args()
    print(args)
    result = None
    for fa in args.files:
        files = sorted(glob.glob(fa))
        print(files)
        for f in files:
            if not "nicht erhoben" in f:
                print("Gathering {}".format(f))
                table = dt.fread(f)
                #print(table.names)
                #print(table.stypes)
                print(list(zip(table.names, table.stypes)))
                if result is None:
                    result = table
                else:
                    result.rbind(table)
            else:
                print("Ignoring {}".format(f))
    print(result)
    if result is not None:
        print("Saving {}".format(args.outputFile))
        result.to_csv(args.outputFile)

if __name__ == "__main__":
    # execute only if run as a script
    main()