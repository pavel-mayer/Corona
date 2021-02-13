import bz2
import lzma
import os
import argparse
import datatable as dt
import time
from datetime import timedelta
from datetime import datetime, date
import cov_dates as cd
import json
import ndjson

# cd /Users/pavel/Corona/2020-rki-archive/data/2_parsed
# python ~/Corona/convertARD.py -d ~/Corona/archive_ard/ *.bz2

def datetimeFromARDFilenameBZ2(ds):
    st = time.strptime(ds, "data_%Y-%m-%d-%H-%M.json.bz2")
    stf = time.mktime(st)
    sdt = datetime.fromtimestamp(stf)
    return sdt

def datetimeFromARDFilenameXZ(ds):
    st = time.strptime(ds, "data_%Y-%m-%d-%H-%M.ndjson.xz")
    stf = time.mktime(st)
    sdt = datetime.fromtimestamp(stf)
    return sdt

def datetimeFromARDFilename(ds):
    if ds.endswith("xz"):
        return datetimeFromARDFilenameXZ(ds)
    return datetimeFromARDFilenameBZ2(ds)

def convert(compressedJSONFile, destDir=".", force = False):
    path = os.path.normpath(compressedJSONFile)
    fileName = path.split(os.sep)[-1]
    date = datetimeFromARDFilename(fileName)
    day = cd.dayFromDate(date)
    newFile =  destDir+"/NPGEO-RKI-{}.csv".format(cd.dateStrYMDFromDay(day))

    if force or not os.path.isfile(newFile):
        print("Loading " + compressedJSONFile)
        #with bz2.open(compressedJSONFile, "rb") as f:
        with lzma.open(compressedJSONFile, "rb") as f:
            content = ndjson.load(f)
            frame = dt.Frame(content)
            print("Saving " + newFile)
            frame.to_csv(newFile)
    else:
        print("Skipping '{}' because '{}' already exists".format(compressedJSONFile, newFile))

def main():
    parser = argparse.ArgumentParser(description='Convert ARD-RKI-dumps to .csv')
    parser.add_argument('files', metavar='fileName', type=str, nargs='+',
                        help='Convert ARD-RKI-dumps to .csv')
    parser.add_argument('-d', '--output-dir', dest='outputDir', default=".")
    args = parser.parse_args()
    print(args)
    for f in args.files:
        convert(f, args.outputDir)

if __name__ == "__main__":
    # execute only if run as a script
    main()