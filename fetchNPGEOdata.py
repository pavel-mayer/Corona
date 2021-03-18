import os
import datatable as dt
import cov_dates as cd
import json
import urllib.request
import time
import argparse

import pm_util as pmu

UPDATE = True # fetch new data for the day if it not already exist
FORCE_UPDATE = False # fetch new data for the day even if it already exists
REFRESH= False or UPDATE # recreate enriched, consolidated dump

def retrieveRecords(offset, length):
    url = "https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/RKI_COVID19/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json&resultOffset={}&resultRecordCount={}".format(offset, length)
    with urllib.request.urlopen(url) as response:
        response = urllib.request.urlopen(url)
        data = json.loads(response.read())
        #print(data)
        # records = data['fields']
        return data

def getRecordVersionOnServer():
    print("Retrieving version date from server")
    chunk = retrieveRecords(0,1)
    #pmu.pretty(chunk)
    datenStand = chunk['features'][0]['attributes']['Datenstand']
    print("Version on server is: "+datenStand)
    return datenStand

def retrieveAllRecords():
    ready = 0
    offset = 0
    #offset = 340000
    chunksize = 5000
    records = []
    newRecords = None
    retry = 0
    while ready == 0:
        chunk = retrieveRecords(offset, chunksize)
        print("Retrieved chunk from {}, chunk items: {}".format(offset, len(chunk)))
        try:
            newRecords= chunk['features']
        except KeyError:
            print("feature not found in newRecord:")
            #pmu.pretty(newRecords)
            return None
        print("Records = {}".format(len(newRecords)))
        if 'exceededTransferLimit' in chunk:
            records = records + newRecords
            exceededTransferLimit = chunk['exceededTransferLimit']
            ready = not exceededTransferLimit
            offset = offset + chunksize
        else:
            print("exceededTransferLimit flag missing, retry #q{}".format(retry))
            retry = retry = retry + 1
            if retry > 10:
                return None
    print("Done")
    return records


print("day0={} {}".format(cd.day0, cd.day0t))

def archiveFilename(day,dir):
    return dir+"/NPGEO-RKI-{}.json".format(cd.dateStrYMDFromDay(day))

def csvFilename(day,kind,dir):
    return "{}/NPGEO-RKI-{}-{}.csv".format(dir, cd.dateStrYMDFromDay(day),kind)

def main():
    parser = argparse.ArgumentParser(description='Download RKI/NPGEO data and save as json-dump and .csv')
    parser.add_argument('-j', '--json-dump-dir', dest='dumpDir', default="dumps")
    parser.add_argument('-c', '--csv-dir', dest='csvDir', default="archive_csv")
    parser.add_argument("-f","--force", dest='force', help="download even if data for day already exist", action="store_true")
    args = parser.parse_args()
    print(args)

    datenStand = getRecordVersionOnServer()
    datenStandDay = cd.dayFromDatenstand(datenStand)
    afn = archiveFilename(datenStandDay, args.dumpDir)
    cfn = csvFilename(datenStandDay, "fullDaily", args.csvDir)

    if not os.path.isfile(afn) and not os.path.isfile(cfn):
        print("New data available, Stand: {} Tag: {}, downloading...".format(datenStand, datenStandDay))
    else:
        print("Data already locally exists, Stand: {} Tag: {}".format(datenStand, datenStandDay))
        if args.force:
            print("Forcing Download because '--force' is set")

    if (not os.path.isfile(afn) and not os.path.isfile(cfn)) or args.force:
        allRecords = retrieveAllRecords()
        if allRecords is not None:
            dfn = "dumps/dump-rki-" + time.strftime("%Y%m%d-%H%M%S") + "-Stand-" + cd.dateStrYMDFromDay(
                datenStandDay) + ".json"
            pmu.saveJson(dfn, allRecords)

            afn = archiveFilename(datenStandDay,args.dumpDir )
            if not os.path.isfile(afn) or  args.force:
                pmu.saveJson(afn, allRecords)

            fn = csvFilename(datenStandDay, "fullDaily", args.csvDir)
            if not os.path.isfile(fn) or args.force:
                pmu.saveCsv(fn, allRecords)
            exit(0)
        else:
            print("failed to retrieve data")
            exit(1)
    exit(9)


if __name__ == "__main__":
    # execute only if run as a script
    main()