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

    # check if previous file exist and make sure the current file is not broken
    previousFile =  destDir+"/NPGEO-RKI-{}.csv".format(cd.dateStrYMDFromDay(day-1))

    yesterDayRows = -1
    if os.path.isfile(previousFile):
        yesterdayFrame = dt.fread(previousFile)
        yesterDayRows = yesterdayFrame.nrows
    else:
        print("No file for previous day {}".format(day-1))

    allowedShrinkageDays = [33,68]
    allowedSameDays = [33]
    allowedJumpDays = [46,66]

    redo = False
    if not force and os.path.isfile(newFile) and yesterDayRows >= 0:
        existingFrame = dt.fread(newFile)
        existingRows = existingFrame.nrows
        if existingRows < yesterDayRows:
            if not day in allowedShrinkageDays:
                print("Existing .csv file for day {} contains less rows ({}) than previous day file ({}), redoing".format(day,existingRows,yesterDayRows))
                redo = True
            else:
                print("On day {} the number of rows was reduced from {} to compared to yesterday's file ({})".format(day,existingRows,yesterDayRows))
        else:
            if existingRows == yesterDayRows:
                if not day in allowedSameDays:
                    print("Existing .csv file for day {} contains same number of rows ({}) than previous day file ({}), redoing".format(day,existingRows,yesterDayRows))
                    redo = True
                else:
                    print( "Existing .csv file for day {} contains same number of rows ({}) than previous day file ({}) but we can't do anything about it".format(
                            day, existingRows, yesterDayRows))
            elif (existingRows > yesterDayRows * 1.1) and (existingRows - yesterDayRows > 5000) and not day in allowedJumpDays:
                print("Existing .csv file for day {} contains much more rows ({}) than previous day file ({}), redoing".format(day,existingRows,yesterDayRows))
                redo = True

            print("Existing .csv file contains {} rows, {} more than yesterday".format(existingRows,existingRows-yesterDayRows))

    if force or redo or not os.path.isfile(newFile):
        print("Loading " + compressedJSONFile)
        #with bz2.open(compressedJSONFile, "rb") as f:
        with lzma.open(compressedJSONFile, "rb") as f:
            content = ndjson.load(f)
            frame = dt.Frame(content)
            if frame.nrows <= yesterDayRows and not day in allowedShrinkageDays:
                print("Rejecting '{}' because it contains less rows than yesterdays file".format(compressedJSONFile))
                return
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
    #print(args)

    dt.options.progress.enabled = False

    for f in sorted(args.files):
        convert(f, args.outputDir)

if __name__ == "__main__":
    # execute only if run as a script
    main()