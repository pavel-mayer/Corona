import argparse
import datatable as dt
import cov_dates as cd
from math import nan
import dateutil.parser
import os
import pm_util as pmu


def add7dSumColumn(table, srcColumn, newColumn):
    src = dt.f[srcColumn]
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

def test():
    table = dt.Frame({"object": [1, 1, 1, 2, 2, 3,4,5,6,7,8,9,10,11,12,13,14],
                      "period": [1, 2, 4, 4, 23,2,5,7,2,7,8,9,5, 18,35,17,15]})
    table = add7dSumColumn(table, "period", "sumPeriod")
    add7dAvrgColumn(table, "period", "avPeriod")

def add7DayAverages(table):
    print(table.names)
    candidatesColumns = [name for name in  table.names if "Neu" in name or "Inzidenz" in name]
    print(candidatesColumns)
    for c in candidatesColumns:
        table = add7dSumColumn(table, c, c+ "-7d")
    return table

def enhance(inputFile, destDir="."):
    table = dt.fread(inputFile)
    newTable = add7DayAverages(table)
    print(newTable)

    path = os.path.normpath(inputFile)
    fileName = path.split(os.sep)[-1]
    newFile = destDir + "/"+"enhanced-"+fileName
    newTable.to_csv(newFile)


def main():
    #test()
    #exit(0)
    parser = argparse.ArgumentParser(description='Add columns with 7-day-averages')
    parser.add_argument('files', metavar='fileName', type=str, nargs='+',
                        help='.csv-File produced by database.py')
    parser.add_argument('-d', '--output-dir', dest='outputDir', default=".")
    args = parser.parse_args()
    print(args)
    for f in args.files:
        enhance(f, args.outputDir)



if __name__ == "__main__":
    # execute only if run as a script
    main()