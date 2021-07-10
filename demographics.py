
import csv
import datatable as dt
import pm_util as pmu

# creates a new table by joining all columns from smaller table according to same values of keyFieldName
# largerTable must contain all keys present in smallertable but not vice versa
def join(largerTable, smallerTable, keyFieldName, overwriteSame=False):
    sKeys = smallerTable[:, keyFieldName].to_list()[0]
    extTable = largerTable.copy()
    for ci, colName in enumerate(smallerTable.names):
        if colName != keyFieldName and (not colName in largerTable.names or overwriteSame):
            values = smallerTable[:, colName].to_list()[0]
            valuesDict = dict(zip(sKeys, values))

            if smallerTable.stypes[ci] == dt.str32:
                extTable = extTable[:, dt.f[:].extend({colName: ""})]
            else:
                extTable = extTable[:, dt.f[:].extend({colName: 0})]

            for i, lk in enumerate(extTable[:,keyFieldName].to_list()[0]):
                if lk in valuesDict:
                    extTable[i,colName] = valuesDict[lk]
    return extTable

def convertCensus(inFilename, outFilename):
    result = {}
    with open(inFilename, newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        currentCode = None
        currentName = None
        for i, row in enumerate(reader):
            print(i,row)
            code = row["Code"]
            name = row["Name"].lstrip(' ')
            if len(code) > 0 and len(name) > 0:
                currentCode = str(code)
                currentName = name
                if currentCode not in result:
                    result[currentCode] = {}
                    result[currentCode]["Code"] = code
                    result[currentCode]["Name"] = name

            group = row["Group"]
            result[currentCode][group+"-total"] = row["Insgesamt"]
            result[currentCode][group+"-M"] = row["männlich"]
            result[currentCode][group+"-W"] = row["weiblich"]
    print(result)
    csv_columns = list(result['11'].keys())
    print(csv_columns)
    try:
        with open(outFilename, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            #writer = csv.DictWriter(csvfile)
            writer.writeheader()
            for key, value in result.items():
                #print("value",value)
                writer.writerow(value)
    except IOError:
        print("I/O error")

def makeNames(fromAge, includeAge, postfix):
    names = []
    age = fromAge
    while age <= includeAge:
        if age == 0:
            name = "unter 1 Jahr-{}".format(postfix)
            names.append(name)
            age = age + 1
        elif age < 75:
            name = "{} bis unter {} Jahre-{}".format(age, age+1, postfix)
            names.append(name)
            age = age + 1
        elif age < 90:
            name = "{} bis unter {} Jahre-{}".format(age, age+5, postfix)
            names.append(name)
            age = age + 5
        else:
            name = "90 Jahre und mehr-{}".format(postfix)
            names.append(name)
            age = includeAge+1
    return names

def makeBerlinNames(fromAge, includeAge, postfix):
    names = []
    age = fromAge
    while age <= includeAge:
        if age == 0:
            name = "unter 1-{}".format(postfix)
            names.append(name)
            age = age + 1
        elif age < 95:
            name = "{} bis unter {}-{}".format(age, age+1, postfix)
            names.append(name)
            age = age + 1
        else:
            name = "95 und älter-{}".format(postfix)
            names.append(name)
            age = includeAge+1
    return names

def makeRKIAgeGroups(outputFile):
    Altersgruppen = ['A00-A04', 'A05-A14', 'A15-A34', 'A35-A59', 'A60-A79', 'A80+', 'unbekannt']
    AltersgruppenRange = [(0,4), (5,14), (15, 34), (35,59), (60,79), (80,150)]
    fullTable = dt.fread("Census.csv")
    RKITable = fullTable[:,['Code',"Name","Insgesamt-total", "Insgesamt-M", "Insgesamt-W"]]

    print(RKITable)
    for i, (fromAge, toAge) in enumerate(AltersgruppenRange):
        for postfix in ["total", "M", "W"]:
            print(Altersgruppen[i], fromAge, toAge)
            names = makeNames(fromAge, toAge, postfix)
            #print(names)
            tTable = fullTable[:, names]
            #print(tTable)
            print(tTable[0])
            newColName = Altersgruppen[i]+"-"+postfix
            sums = tTable[:, {newColName: dt.rowsum(dt.f[:])}]
            RKITable[newColName] = sums
            print(sums)
            print(RKITable)

    BerlinTable = dt.fread("Census-Berlin.csv")
    RKIBerlinTable = BerlinTable[:,['Code',"Name","Insgesamt-total", "Insgesamt-M", "Insgesamt-W"]]

    print(RKIBerlinTable)
    for i, (fromAge, toAge) in enumerate(AltersgruppenRange):
        for postfix in ["total", "M", "W"]:
            print(Altersgruppen[i], fromAge, toAge)
            names = makeBerlinNames(fromAge, toAge, postfix)
            print(names)
            tTable = BerlinTable[:, names]
            print(tTable)
            print(tTable[0])
            newColName = Altersgruppen[i]+"-"+postfix
            sums = tTable[:, {newColName: dt.rowsum(dt.f[:])}]
            RKIBerlinTable[newColName] = sums
            print(sums)
            print(RKIBerlinTable)

    # adjust berlin Number to Match nuw numbers
    oldBerlinRow = RKIBerlinTable[0,2:]
    newBerlinRow = RKITable[dt.f.Name == "Berlin",2:]
    print(oldBerlinRow)
    print(newBerlinRow)

    factors = newBerlinRow.to_numpy() / oldBerlinRow.to_numpy()
    print(factors)

    adjustedBerlin = (RKIBerlinTable[:,2:].to_numpy() * factors).astype(int)
    print(adjustedBerlin)
    RKIBerlinTable[:, 2:] = adjustedBerlin
    print(RKIBerlinTable)

    RKITable.rbind(RKIBerlinTable[1:,:])
    #print(RKITable)
    #RKITable.to_csv("raw.csv")

    # add Hamburg also as Landkreis
    Hamburg = RKITable[dt.f.Code == 2000,:]
    if Hamburg.nrows != 1:
        Hamburg = RKITable[dt.f.Code == 2, :]
        if Hamburg.nrows != 1:
            print("Hamburg not found in Census")
            exit(1)
        RKITable.rbind(Hamburg)
        RKITable[RKITable.nrows-1, "Code"] = 2000

    ## check for consistency
    latest = dt.fread("data.csv")
    latestList = latest[:,["IdLandkreis","Landkreis","IdBundesland","Bundesland"]]

    RKITable.names = {"Code" : "IdLandkreis"}
    #print(RKITable)

    check = join(RKITable, latestList, "IdLandkreis", overwriteSame=False)
    #print(check)
    #check.to_csv("check.csv")
    print("Trash:\n", check[(dt.f.IdBundesland == 0) & (dt.f.Landkreis != "Deutschland"),:])
    del check[(dt.f.IdBundesland == 0) & (dt.f.Landkreis != "Deutschland"),:]
    print(check)
    check.to_csv(outputFile)

convertCensus("demographie/Census-ALL-clean-7.csv", "Census.csv")
convertCensus("demographie/Berlin-Bezirke-3.csv", "Census-Berlin.csv")

makeRKIAgeGroups("CensusByRKIAgeGroups.csv")