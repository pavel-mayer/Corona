import csv
import json
import datatable as dt
import sys
import os
import psutil
import glob
import time
import re

csv.field_size_limit(sys.maxsize)

def loadJson(fileName):
    print("loadJson: Loading "+fileName)
    with open(fileName, 'r') as openfile:
        json_object = json.load(openfile)
        return json_object

def saveJson(fileName, objectToSave):
    print("saveJson: Saving "+fileName)
    with open(fileName, 'w') as outfile:
        json.dump(objectToSave, outfile)

def pretty(jsonmap):
    print(json.dumps(jsonmap, sort_keys=False, indent=4, separators=(',', ': ')))

def loadCsv(filename):
    print("loadCsv: Loading "+filename)
    records = []
    with open(filename, 'r') as file:
        csv_file = csv.DictReader(file)
        for row in csv_file:
            attributes = {"attributes" : dict(row)}
            #print(dict(row))
            records.append(attributes)
    return records

def saveCsv(filename, records):
    print("saveCsv: Saving "+filename)
    csv_columns = list(records[0]["attributes"].keys())
    for data in records:
        record = data["attributes"]
        for attr in record:
            if not attr in csv_columns:
                csv_columns.append(attr)
    #print(csv_columns)

    try:
        with open(filename, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for data in records:
                record = data["attributes"]
                writer.writerow(record)
    except IOError:
        print("I/O error")

# def saveCsvTable(table, fileName, destDir="."):
#     newFile =  destDir+"/"+fileName
#     if os.path.isfile(newFile):
#         bakFile = newFile + ".bak"
#         if os.path.isfile(bakFile):
#             os.remove(bakFile)
#         os.rename(newFile, bakFile)
#
#     print("Saving "+newFile)
#     table.to_csv(newFile)

# def saveJayTable(table, fileName, destDir="."):
#     newFile =  destDir+"/"+fileName
#     if os.path.isfile(newFile):
#         bakFile = newFile+".bak"
#         if os.path.isfile(bakFile):
#             os.remove(bakFile)
#         os.rename(newFile, bakFile)
#
#     print("Saving "+newFile)
#     table.to_jay(newFile)
#     print("Saving done "+newFile)

# will remove the backup file before saving
# and write to a temp file that will be renamed
# after successful write, and the old file
# is renamed to a .bak ending or deleted
# when backup is not set.
def saveJayTable(table, fileName, destDir=".", backup = False, unsafe=False):
    newFile = destDir + "/" + fileName
    print("Saving " + newFile)
    newTmpFile = newFile+".saving"
    if os.path.isfile(newTmpFile):
        os.remove(newTmpFile)
    bakFile = newFile + ".bak"
    if os.path.isfile(bakFile):
        os.remove(bakFile)

    if unsafe:
        table.to_jay(newFile)
    else:
        table.to_jay(newTmpFile)
        if backup:
            os.rename(newFile, bakFile)
        elif os.path.isfile(newFile):
            os.remove(newFile)
        os.rename(newTmpFile, newFile)
    print("Saving done " + newFile)

def saveCsvTable(table, fileName, destDir=".", backup = False, unsafe=False):
    newFile = destDir + "/" + fileName
    print("Saving " + newFile)
    newTmpFile = newFile+".saving"
    if os.path.isfile(newTmpFile):
        os.remove(newTmpFile)
    bakFile = newFile + ".bak"
    if os.path.isfile(bakFile):
        os.remove(bakFile)

    if unsafe:
        table.to_csv(newFile)
    else:
        table.to_csv(newTmpFile)
        if backup:
            os.rename(newFile, bakFile)
        elif os.path.isfile(newFile):
            os.remove(newFile)
        os.rename(newTmpFile, newFile)
    print("Saving done " + newFile)

def ascify(name):
    replacements = {"Ä" : "Ae",
                    "Ö" : "Oe",
                    "Ü" : "Ue",
                    "ä" : "ae",
                    "ö" : "oe",
                    "ü" : "ue",
                    "ß": "ss",
                    " ": "_",
                    }
    for key, value in replacements.items():
        name = name.replace(key,value)
    name = re.sub("[^0-9a-zA-Z_\(\)\.\-]+", "#", name)
    return name

def seriesFileName(id, name):
    return "series-{:05d}-{}.csv".format(id, ascify(name))


def printMemoryUsage(where):
    process = psutil.Process(os.getpid())
    print("Memory Usage @ {}: {:.3f} GB".format(where, process.memory_info().rss/1024/1024/1024))  # in bytes

def makePartitionFileName(partitionNumber, fileName):
    return "partition-{:04d}-{}".format(partitionNumber, fileName)

def lastJayTablePartition(fileName, destDir=".", partitionSize = 10000000,
                          tempDir=None, memoryLimit=None, verbose=False):

    # count partition files
    partitionNumber = 0
    last = False
    while True:
        partitionFileName = makePartitionFileName(partitionNumber, fileName)
        if not os.path.isfile(partitionFileName):
            print("Last partition is {}".format(partitionFileName))
            break
        else:
            partitionNumber = partitionNumber + 1

    # check all partitions foe proper size
    for p in range(partitionNumber):
        partitionFileName = makePartitionFileName(p, fileName)
        try:
            partition = dt.fread(partitionFileName, tempdir=tempDir, memory_limit=memoryLimit, verbose=verbose)
        except:
            e = sys.exc_info()[0]
            print("Could not read file {}, error= {}".format(partitionFileName, e))
            return None
        else:
            if partitionSize != partition.nrows:
                print("Partition file {} has wrong partition size, is {}, must be {}".format(partitionFileName, partition.nrows, partitionSize))
                return None

    partitionFileName = makePartitionFileName(partitionNumber, fileName)
    partitionTable = dt.fread(partitionFileName, tempdir=tempDir, memory_limit=memoryLimit, verbose=verbose)
    return partitionTable


def appendToJayTablePartioned(table, fileName, destDir=".", partitionSize = 10000000,
                          tempDir=None, memoryLimit=None, verbose=False):
    partitionNumber = 0
    last = False
    while True:
        partitionFileName = makePartitionFileName(partitionNumber, fileName)
        if not os.path.isfile(partitionFileName):
            break
        partitionNumber = partitionNumber + 1


def saveJayTablePartioned(table, fileName, destDir=".", partitionSize = 10000000, onlyWhenChanged=False, destructive=False,
                          tempDir=None, memoryLimit=None, verbose=False, lastPartitionOnly = False, noBackup = True):
    partitions = int(table.nrows / partitionSize) + 1
    print("partitions",partitions)
    for r in range(partitions):
        printMemoryUsage("saveJayTablePartioned {}".format(r))
        partitionFileName = "partition-{:04d}-{}".format(r, fileName)
        if destructive:
            start = 0
            end = min(partitionSize, table.nrows)
            partition = table[0:end,:]
        else:
            start = r * partitionSize
            end = min(start+partitionSize, table.nrows)
            partition = table[start:end,:]
        newFile = os.path.join(destDir,partitionFileName)
        if os.path.isfile(newFile):
            if onlyWhenChanged:
                printMemoryUsage("saveJayTablePartioned - reading existing {}".format(r))
                try:
                    oldTable = dt.fread(newFile, tempdir=tempDir, memory_limit=memoryLimit, verbose=verbose)
                except:
                    e = sys.exc_info()[0]
                    print("Could not read file {}, error= {}".format(newFile, e))
                else:
                    if oldTable.nrows == partition.nrows:
                        if oldTable.names == partition.names:
                            oldCases = oldTable[(dt.f.NeuerFall == 0) | (dt.f.NeuerFall == 1), 'AnzahlFall'].sum()[0, 0]
                            partitionCases = partition[(dt.f.NeuerFall == 0) | (dt.f.NeuerFall == 1), 'AnzahlFall'].sum()[0, 0]
                            if oldCases == partitionCases:
                                printMemoryUsage("saveJayTablePartioned - same as old, not saving {}".format(r))
                                continue

                printMemoryUsage("saveJayTablePartioned - old table differs, will save {}".format(r))
            else:
                bakFile = newFile + ".bak"
                if os.path.isfile(bakFile):
                    os.remove(bakFile)
                if noBackup:
                    os.remove(newFile)
                else:
                    os.rename(newFile, bakFile)

        print("Saving " + newFile)
        start = time.perf_counter()
        partition.to_jay(newFile)
        elapsed = time.perf_counter() - start
        bytes = os.stat(newFile).st_size
        print("Saved {} bytes, {:0f} bytes per sec, {:3f} MB, {:3f} MB per sec".format(bytes, bytes/elapsed, bytes/1024/1024, bytes/1024/1024/elapsed))
        if destructive:
            partition = None
            del table[0:end,:]
    print("Saved all partitions of " + fileName)

def getJayTablePartitions(pathName):
    path = os.path.normpath(pathName)
    dir, fileName = os.path.split(path)
    globstr = dir+"/partition-????-"+fileName
    files = sorted(glob.glob(globstr))
    return files

def loadJayTablePartioned(fileName, tempdir=None, memory_limit=None, verbose=False):
    files = getJayTablePartitions(fileName)
    fullTable = None
    for f in files:
        printMemoryUsage("before loading partition from '{}'".format(f))
        try:
            newTable = dt.fread(f, tempdir=tempdir, memory_limit=memory_limit, verbose=verbose)
        except:
            e = sys.exc_info()[0]
            print("Could not read file {}, error= {}".format(f,e))
            return fullTable
        else:
            if fullTable is None:
                fullTable = newTable
            else:
                fullTable.rbind(newTable)
            printMemoryUsage("after loading partition from '{}'".format(f))
            newTable = None
            #printMemoryUsage("after clearing partition from '{}'".format(f))

    print("Read {} rows from {} partitions".format(fullTable.nrows, len(files)))
    return fullTable

def is_int(o):
    try:
        i=int(o)
        return True
    except ValueError:
        return False

