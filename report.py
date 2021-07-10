import argparse
import os
import glob
import datatable as dt

agegroups = ["_AG_A00_A04","_AG_A05_A14","_AG_A15_A34","_AG_A35_A59","_AG_A60_A79","_AG_A80Plus"]
genders = ["_G_W","_G_M"]

def loadData(dataFilename):
    print("Loading " + dataFilename)

    fullTable = dt.fread(dataFilename)
    print("Loading done loading table from ‘" + dataFilename + "‘, keys:")
    print(fullTable.keys())
    return fullTable


def selectColumnsWithSubstring(names, substrings):
    result = []
    for n in names:
        for s in substrings:
            if s in n:
                result.append(n)
    return result

def equal(col, value):
    return dt.f[col] == value

def lessThan(col, value):
    return dt.f[col] < value

def lessThanEqual(col, value):
    return dt.f[col] <= value

def greaterThan(col, value):
    return dt.f[col] > value

def greaterThanEqual(col, value):
    return dt.f[col] >= value

def inRange(col, begin, end):
    return (dt.f[col] >= begin) & (dt.f[col] < end)

def selectRowsEqual(frame, colName, value):
    return frame[dt.f[colName] == value]

def csv_list(string):
    return string.split(',')

def csv_dict(string):
    result = {}
    assignment = string.split(',')
    for a in assignment:
        parts = a.split(":")
        if len(parts)==2:
            result[parts[0]]=parts[1]
        else:
            print("Malformed map, expected two values seperated by colon like KEY:VALUE")
            return None
    return result


def main():
    parser = argparse.ArgumentParser(description='search files for binary string pattern')
    parser.add_argument('files', metavar='fileName', type=str, nargs='+',
                        help='files to search')
    parser.add_argument('-d', '--datafile', dest='datafile', default="all-series-agegroups-gender.csv")
    parser.add_argument('-v', dest='verbose', action="store_true")
    parser.add_argument('-c', dest='columnsSelector', type=csv_list)
    parser.add_argument('-r', dest='rowSelector')
    parser.add_argument('--eq', '--equals', dest='rowEquals', type=csv_dict)
    parser.add_argument('--gte', '--greater_than', dest='rowSelector', type=csv_dict)
    parser.add_argument('--lte', '--less_than', dest='rowSelector', type=csv_dict)

    args = parser.parse_args()
    print(args)

    if args.verbose:
        print("Searching for pattern {}".format(pattern, bytes))
    for fa in args.files:
        files = sorted(glob.glob(fa))
        for f in files:
            if os.path.isfile(f):
                search(f, pattern, args.lcs, args.verbose)
    print("done")

if __name__ == "__main__":
    # execute only if run as a script
    main()