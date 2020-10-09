import bz2
import os
import argparse
import datatable as dt
import time
from datetime import timedelta
from datetime import datetime, date
import cov_dates as cd
import json

# cd /Users/pavel/Corona/2020-rki-archive/data/2_parsed
# python ~/Corona/convertARD.py -d ~/Corona/archive_ard/ *.bz2

# Parse date string like "19.05.2020, 00:00 Uhr"
def datetimeFromARDFilename(ds):
    st = time.strptime(ds, "data_%Y-%m-%d-%H-%M.json.bz2")
    stf = time.mktime(st)
    sdt = datetime.fromtimestamp(stf)
    return sdt

def convert(bz2csvFile, destDir=".", force = False):
    path = os.path.normpath(bz2csvFile)
    fileName = path.split(os.sep)[-1]
    date = datetimeFromARDFilename(fileName)
    day = cd.dayFromDate(date)
    newFile =  destDir+"/NPGEO-RKI-{}.csv".format(cd.dateStrYMDFromDay(day))

    if force or not os.path.isfile(newFile):
        print("Loading " + bz2csvFile)
        with bz2.open(bz2csvFile, "rb") as f:
            content = json.load(f)
            frame = dt.Frame(content)
            print("Saving " + newFile)
            frame.to_csv(newFile)
    else:
        print("Skipping {} because {} already exists", bz2csvFile, newFile)

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