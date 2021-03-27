import csv
import json
import datatable as dt
import sys
import os
import psutil
import glob
import time

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

def saveCsvTable(table, fileName, destDir="."):
    newFile =  destDir+"/"+fileName
    if os.path.isfile(newFile):
        bakFile = newFile + ".bak"
        if os.path.isfile(bakFile):
            os.remove(bakFile)
        os.rename(newFile, bakFile)

    print("Saving "+newFile)
    table.to_csv(newFile)

def saveJayTable(table, fileName, destDir="."):
    newFile =  destDir+"/"+fileName
    if os.path.isfile(newFile):
        bakFile = newFile+".bak"
        if os.path.isfile(bakFile):
            os.remove(bakFile)
        os.rename(newFile, bakFile)

    print("Saving "+newFile)
    table.to_jay(newFile)
    print("Saving done "+newFile)

def printMemoryUsage(where):
    process = psutil.Process(os.getpid())
    print("Memory Usage @ {}: {:.3f} GB".format(where, process.memory_info().rss/1024/1024/1024))  # in bytes

def saveJayTablePartioned(table, fileName, destDir=".", partitionSize = 10000000, onlyWhenChanged=False, destructive=False,
                          tempDir=None, memoryLimit=None, verbose=False):
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

def loadJayTablePartioned(fileName, tempDir=None, memoryLimit=None, verbose=False):
    files = getJayTablePartitions(fileName)
    fullTable = None
    for f in files:
        printMemoryUsage("before loading partition from '{}'".format(f))
        try:
            newTable = dt.fread(f, tempdir=tempDir, memory_limit=memoryLimit, verbose=verbose)
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

