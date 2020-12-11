import csv
import json
import datatable as dt
import sys
import os
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

def is_int(o):
    try:
        i=int(o)
        return True
    except ValueError:
        return False