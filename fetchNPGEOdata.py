import os
import datatable as dt
import cov_dates as cd
import json
import time
import argparse
import sys

from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
import socket
import pm_util as pmu

# timeout in seconds
socket.setdefaulttimeout(10)

def retrieveRecords(offset, length):
    url = "https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/RKI_COVID19/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json&resultOffset={}&resultRecordCount={}".format(offset, length)
    with urlopen(url) as response:
        response = urlopen(url)
        data = json.loads(response.read())
        #print(data)
        # records = data['fields']
        return data

#return a repsonse object; errors are also response instances
def requestURL(url):
    req = Request(url)
    try:
        response = urlopen(req)
    except HTTPError as e:
        print('The server couldn\'t fulfill the request.')
        print('Error code: ', e.code)
        return e
    except URLError as e:
        print('We failed to reach a server.')
        print('Reason: ', e.reason)
        return e
    except socket.timeout as e:
        print('Socket timeout')
        print('Exception: ', e)
        return None
    except socket.error as e:
        print('Socket error')
        print('Exception: ', e)
        return None
    except:
        e = sys.exc_info()[0]
        print('Exception: ', e)
    else:
        return response


def retrieveRecords2(offset, length):
    apiurl = "https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/RKI_COVID19/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json&resultOffset={}&resultRecordCount={}".format(offset, length)

    response = requestURL(apiurl)

    if response is None:
        return None

    if hasattr(response,'reason') and response.reason != "OK":
        print("response.reason", response.reason)
        return None
    else:
        if hasattr(response, 'code'):
            if response.code == 200:
                #print("response.code", response.code)
                try:
                    data = json.loads(response.read())
                except:
                    e = sys.exc_info()[0]
                    print('Exception: ', e)
                else:
                    return data
    return None


def getRecordVersionOnServer():
    print("Retrieving version date from server")
    chunk = retrieveRecords(0,1)
    #pmu.pretty(chunk)
    datenStand = chunk['features'][0]['attributes']['Datenstand']
    print("Version on server is: "+datenStand)
    return datenStand

def retrieveAllRecords(args):
    ready = 0
    offset = 0
    #offset = 340000
    chunksize = 5000
    records = []
    if args.resume:
        records = pmu.loadJson("lastReceived.json")

    newRecords = None
    lastReceived = None
    retry = 0
    while ready == 0:
        chunk = retrieveRecords2(offset, chunksize)
        error = False
        if chunk is None:
            print("Retrieve returned without chunk")
            time.sleep(retry * 3)
            retry = retry + 1
            print("Starting retry #{}".format(retry))
        else:
            print("Retrieved chunk from {}, chunk items: {}".format(offset, len(chunk)))
            try:
                newRecords = chunk['features']
            except KeyError:
                print("feature not found in newRecord, retry #{}".format(retry))
                time.sleep(retry*3)
                retry = retry + 1
                print("Starting retry #{}".format(retry))
                error = True
                if retry > args.maxRetries:
                    pmu.saveJson("lastReceived.json", newRecords)
                    pmu.saveJson("allReceived-noFeature.json", records)
                    #pmu.pretty(newRecords)
                    return None

            if newRecords != None:
                print("Records = {}".format(len(newRecords)))
            else:
                print("No Records")
            if not error:
                if 'exceededTransferLimit' in chunk:
                    records = records + newRecords
                    exceededTransferLimit = chunk['exceededTransferLimit']
                    ready = not exceededTransferLimit
                    offset = offset + chunksize
                    retry = 0
                else:
                    if retry > 0 and lastReceived == newRecords:# and len(newRecords)>0:
                        print("exceededTransferLimit flag still missing, but we got the same data twice, so it should be ok")
                        records = records + newRecords
                        break

                    print("exceededTransferLimit flag missing, retry #{}".format(retry))
                    #pmu.pretty(newRecords)
                    time.sleep(retry*3)
                    retry = retry + 1
                    print("Starting retry #{}".format(retry))
                    if retry > args.maxRetries:
                        pmu.saveJson("lastReceived.json", newRecords)
                        pmu.saveJson("allReceived-noLimitFlag.json", records)
                        return None
                    lastReceived = newRecords
    print("Done")
    return records


print("day0={} {}".format(cd.day0, cd.day0t))

def archiveFilename(day,dir):
    return dir+"/NPGEO-RKI-{}.json".format(cd.dateStrYMDFromDay(day))

def csvFilename(day,kind,dir):
    return "{}/NPGEO-RKI-{}-{}.csv".format(dir, cd.dateStrYMDFromDay(day),kind)


def retrieveLatestCsvDate(args):
    metaDataUrl = 'https://www.arcgis.com/sharing/rest/content/items/f10774f1c63e40168479a1feb6c7ca74?f=json'

    response = requestURL(metaDataUrl)
    if hasattr(response,'reason') and response.reason != "OK":
        print("response.reason", response.reason)
        return None
    else:
        if hasattr(response, 'code'):
            if response.code == 200:
                print("response.code", response.code)
                metaData = json.loads(response.read())
                pmu.pretty(metaData)
                lastModDateStr = metaData["modified"]
                lastModeDate = cd.datetimeFromStampStr(lastModDateStr)
                print(lastModDateStr, lastModeDate)
                return lastModeDate
    return None


def downloadCsv(args, toFile):
    dataUrl = 'https://www.arcgis.com/sharing/rest/content/items/f10774f1c63e40168479a1feb6c7ca74/data'
    response = requestURL(dataUrl)
    if hasattr(response, 'reason') and response.reason != "OK":
        print("response.reason", response.reason)
    else:
        if hasattr(response, 'code'):
            if response.code == 200:
                # print("response.code", response.code)
                with open(toFile, mode='wb') as localfile:
                    localfile.write(response.read())
                    return True
    return False

def main():
    parser = argparse.ArgumentParser(description='Download RKI/NPGEO data and save as json-dump and .csv')
    parser.add_argument('-j', '--json-dump-dir', dest='dumpDir', default="dumps")
    parser.add_argument('-c', '--csv-dir', dest='csvDir', default="archive_csv")
    parser.add_argument("-f","--force", dest='force', help="download even if data for day already exist", action="store_true")
    parser.add_argument("-R","--resume", dest='resume', help="download even if data for day already exist (API download only). Barely tested)", action="store_true")
    parser.add_argument('-r','--retry', dest='maxRetries',type=int, default=10, help='Number of retries before giving up on a single request; each retry waits 3 second longer')
    parser.add_argument("-F","--fetchcsv", dest='fetchcsv', help="fall back to directly download as .csv file, not using the api", action="store_true")
    args = parser.parse_args()
    print(args)

    if args.fetchcsv:
        datenStand = retrieveLatestCsvDate(args)
        datenStandDay = cd.dayFromDate(datenStand)
    else:
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
        dfn = "dumps/dump-rki-" + time.strftime("%Y%m%d-%H%M%S") + "-Stand-" + cd.dateStrYMDFromDay(
            datenStandDay) + ".json"

        if not args.fetchcsv:
            allRecords = retrieveAllRecords(args)
            if allRecords is not None:
                #pmu.saveJson(dfn, allRecords)
                if not os.path.isfile(afn) or  args.force:
                    pmu.saveJson(afn, allRecords)
                if not os.path.isfile(cfn) or args.force:
                    pmu.saveCsv(cfn, allRecords)
                exit(0)
            else:
                print("failed to retrieve data")
                exit(1)
        else:
            # download the .csv
            if downloadCsv(args, cfn):
                print("Successfully downloaded .csv")
                dataDict = pmu.loadCsv(cfn)
                pmu.saveJson(afn, dataDict)

    exit(9)


if __name__ == "__main__":
    # execute only if run as a script
    main()