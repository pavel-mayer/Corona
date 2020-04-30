import csv
import json
import urllib.request
import time
import copy
from datetime import timedelta
from datetime import datetime, date
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.animation import FuncAnimation
from matplotlib.animation import FFMpegWriter
from matplotlib.animation import ImageMagickWriter


def bar_plot(ax, data, colors=None, total_width=0.8, single_width=1, legend=True):
    """Draws a bar plot with multiple bars per data point.

    Parameters
    ----------
    ax : matplotlib.pyplot.axis
        The axis we want to draw our plot on.

    data: dictionary
        A dictionary containing the data we want to plot. Keys are the names of the
        data, the items is a list of the values.

        Example:
        data = {
            "x":[1,2,3],
            "y":[1,2,3],
            "z":[1,2,3],
        }

    colors : array-like, optional
        A list of colors which are used for the bars. If None, the colors
        will be the standard matplotlib color cyle. (default: None)

    total_width : float, optional, default: 0.8
        The width of a bar group. 0.8 means that 80% of the x-axis is covered
        by bars and 20% will be spaces between the bars.

    single_width: float, optional, default: 1
        The relative width of a single bar within a group. 1 means the bars
        will touch eachother within a group, values less than 1 will make
        these bars thinner.

    legend: bool, optional, default: True
        If this is set to true, a legend will be added to the axis.
    """

    # Check if colors where provided, otherwhise use the default color cycle
    if colors is None:
        colors = plt.rcParams['axes.prop_cycle'].by_key()['color']

    # Number of bars per group
    n_bars = len(data)

    # The width of a single bar
    bar_width = total_width / n_bars

    # List containing handles for the drawn bars, used for the legend
    bargroups = []
    barsR = []

    # Iterate over all data
    for i, (name, values) in enumerate(data.items()):
        # The offset in x direction of that bar
        x_offset = (i - n_bars / 2) * bar_width + bar_width / 2

        bars = []
        #print(name)
        #print(values[0])
        #print(values[1])
        for j, day in enumerate(values[0]):
            #print(j,day)

            bar = ax.bar(day + x_offset, values[1][j], width=bar_width * single_width, color=colors[i % len(colors)])
            bars.append(bar)
        bargroups.append(bars)
        # Draw a bar for every value of that type
        #for x, y in enumerate(values):
        #    bar = ax.bar(x + x_offset, y, width=bar_width * single_width, color=colors[i % len(colors)])
        # Add a handle to the last drawn bar, which we'll need for the legend
        barsR.append(bar[0])

    # Draw legend if we need
    if legend:
        ax.legend(barsR, data.keys())
    return bargroups


# if __name__ == "__main__":
#     # Usage example:
#     data = {
#         "a": [1, 2, 3, 2, 1],
#         "b": [2, 3, 4, 3, 1],
#         "c": [3, 2, 1, 4, 2],
#         "d": [5, 9, 2, 1, 8],
#         "e": [1, 3, 2, 2, 3],
#         "f": [4, 3, 1, 1, 4],
#     }
#
#     fig, ax = plt.subplots()
#     bar_plot(ax, data, total_width=.8, single_width=.9)
#     plt.show()


def pretty(jsonmap):
    print(json.dumps(jsonmap, sort_keys=False, indent=4, separators=(',', ': ')))

def retrieveRecords(offset, length):
    url = "https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/RKI_COVID19/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json&resultOffset={}&resultRecordCount={}".format(offset, length)
    with urllib.request.urlopen(url) as response:
        response = urllib.request.urlopen(url)
        data = json.loads(response.read())
        #print(data)
        # records = data['fields']
        return data

def retrieveAllRecords():
    ready = 0
    offset = 0
    chunksize = 3000
    records = []
    while ready == 0:
        chunk = retrieveRecords(offset, chunksize)
        offset = offset + chunksize
        newRecords= chunk['features']
        print("Retrieved chunk from {} count {}".format(offset, len(newRecords)))
        records = records + newRecords
        if 'exceededTransferLimit' in chunk:
            exceededTransferLimit = chunk['exceededTransferLimit']
            ready = not exceededTransferLimit
        else:
            ready = True
    print("Done")
    return records

def loadJson(fileName):
    with open(fileName, 'r') as openfile:
        json_object = json.load(openfile)
        return json_object

def saveJson(fileName, objectToSave):
    with open(fileName, 'w') as outfile:
        json.dump(objectToSave, outfile)

day0 = time.strptime("22.2.2020", "%d.%m.%Y")
#day0 = time.strptime("22.4.2020", "%d.%m.%Y")
day0t = time.mktime(day0)
day0d = datetime.fromtimestamp(day0t)

def dateFromClock(c):
    t = time.gmtime(int(c)/1000)
    return "{}.{}.{}".format(t.tm_mday, t.tm_mon,t.tm_year)

def dateFromDay(day):
    result = day0d + timedelta(day)
    #t = time.gmtime(int(c)/1000)
    return "{}.{}.{}".format(result.day, result.month, result.year)
    #t.tm_mday, t.tm_mon,t.tm_year)

def dayFromStampStr(s):
    d = datetime.fromtimestamp(int(s) / 1000)
    delta = d - day0d
    return delta.days

def sumField(records,fieldName):
    result = 0
    for r in records:
        result = result + int(r['attributes'][fieldName])
    return result

def sumFieldIf(records,fieldToSum, ifField, ifFieldIs):
    result = 0
    for r in records:
        attrs = r['attributes']
        if attrs[ifField] == ifFieldIs:
            result = result + int(attrs[fieldToSum])
    return result

def sumFieldIfDateBefore(records,fieldToSum, dateField, beforeDay):
    result = 0
    for r in records:
        attrs = r['attributes']
        if dayFromStampStr(attrs[dateField]) < beforeDay:
            result = result + int(attrs[fieldToSum])
    return result

def delaysList(records,beforeDay):
    result = []
    for r in records:
        attrs = r['attributes']
        meldedatum = dayFromStampStr(attrs["Meldedatum"])
        if meldedatum < beforeDay:
            erkdatum=dayFromStampStr(attrs["Refdatum"])
            result.append(meldedatum-erkdatum)
    return result

def includeAll(day,attrs):
    return True

def includePos(day,attrs):
    return day >= 0

def includePosErkbeginn(day,attrs):
    return day >= 0 and int(attrs['IstErkrankungsbeginn']) == 1

def includePosNoErkbeginn(day,attrs):
    return day >= 0 and int(attrs['IstErkrankungsbeginn']) == 0

def byDate(records, whichDate, filterFunc):
    result = {}
    for r in records:
        attrs = r['attributes']
        dateStamp = attrs[whichDate]
        day = dayFromStampStr(dateStamp)
        if filterFunc(day,attrs):
            if day in result:
                result[day].append(r)
            else:
                result[day] = [r]
    return result


print("day0={} {}".format(day0, day0t))

allRecords = []

update = False
fileName = "latest-rki.json"
if update:
    allRecords = retrieveAllRecords()
    saveJson(fileName, allRecords)
else:
    allRecords = loadJson(fileName)

print("Loaded {} records".format(len(allRecords)))

dead = sumField(allRecords, "AnzahlTodesfall")
cases = sumField(allRecords, "AnzahlFall")

femaleCases = sumFieldIf(allRecords,"AnzahlFall","Geschlecht","W")
maleCases = sumFieldIf(allRecords,"AnzahlFall","Geschlecht","M")

print("Cases {} male {} female {} sum {}, dead {}".format(cases, maleCases, femaleCases, maleCases+femaleCases,dead))

pretty(allRecords[0:100])

def extractLists(records):
    dayList = []
    deadList = []
    caseList =[]

    for d in sorted (records.keys()):
        dayList.append(d)
        cases = sumField(records[d], "AnzahlFall")
        caseList.append(cases)
        deaths = sumField(records[d], "AnzahlTodesfall")
        deadList.append(deaths)
        #print("Day {} {} cases {} dead {}".format(d, dateFromDay(d),cases, deaths))
    return dayList, deadList, caseList

def extractListsPartial(records, fromDay):
    dayList = []
    deadList = []
    caseList =[]

    for d in sorted (records.keys()):
        dayList.append(d)
        cases = sumFieldIfDateBefore(records[d], "AnzahlFall","Meldedatum",fromDay)
        caseList.append(cases)
        deaths = sumFieldIfDateBefore(records[d], "AnzahlTodesfall","Meldedatum",fromDay)
        deadList.append(deaths)
        #print("Day {} {} cases {} dead {}".format(d, dateFromDay(d),cases, deaths))
    return dayList, deadList, caseList

def extractDelays(records, beforeDay):
    delayList = []

    for d in sorted (records.keys()):
        delays = delaysList(records[d],beforeDay)
        delayList.append(delays)
    return delayList

def makeIndex(days, values):
    result = {}
    for i, day in enumerate(days):
        result[day] = values[i]
    return result

def unmakeIndex(daysAndValues):
    days = sorted(daysAndValues.keys())
    values = []
    for d in days:
        values.append(daysAndValues[d])
    return days, values

def redistributed(ohneErkBeg, mitErkBeg, backDistribution):
    result = copy.deepcopy(mitErkBeg)
    for d in sorted (ohneErkBeg.keys()):
        for back, dist in enumerate(backDistribution):
            destDay = d - back
            if destDay in result:
                result[destDay] = result[destDay] + ohneErkBeg[d] * dist
    return result

byMeldedatum = byDate(allRecords,'Meldedatum',includePos)
byRefdatum = byDate(allRecords,'Refdatum',includePosNoErkbeginn)
byErkdatum = byDate(allRecords,'Refdatum',includePosErkbeginn)

dayList, deadList, caseList = extractLists(byMeldedatum)
dayListR, deadListR, caseListR = extractLists(byRefdatum)
dayListE, deadListE, caseListE = extractLists(byErkdatum)

ohneErkBeg = makeIndex(dayListR, caseListR)
mitErkBeg = makeIndex(dayListE, caseListE)

delayList = extractDelays(byErkdatum, 100)
allDelays = [item for sublist in delayList for item in sublist]

num_bins = 24
allBins = np.histogram(allDelays, bins=num_bins, range=(0, num_bins), density=1)
erkrankungen = redistributed(ohneErkBeg, mitErkBeg, allBins[0])
compErkDays, compErkValues = unmakeIndex(erkrankungen)

totalCases = np.sum(caseList)
totalCompErk = np.sum(compErkValues)

print("Meldungen {}, Errechnete Erkranungen {}".format(totalCases, totalCompErk))

######################################################################
fig = plt.figure(figsize=(16, 10))

######################################################################
ax = plt.subplot(311)

plt.ylim(0,7000)

data = {
    "Gemeldete Infektionen":[dayList,caseList],
    "Ohne Erkrankungsdatum":[dayListR,caseListR],
    "Erkrankt am":[dayListE, caseListE],
}

colors = ['royalblue', 'firebrick', 'darkorange']
ax.xaxis.set_major_locator(ticker.MultipleLocator(7))

ax_bargroups = bar_plot(ax,data,colors=colors)

######################################################################
axb = plt.subplot(312)

plt.ylim(0,7000)

data = {
#    "Gemeldete Infektionen":[dayList,caseList],
#    "Ohne Erkrankungsdatum":[dayListR,caseListR],
    "Erkrankt am":[dayListE, caseListE],
    "Berechnet Erkrankt am": [compErkDays, compErkValues],
}

colors = ['darkorange','darkseagreen']
axb.xaxis.set_major_locator(ticker.MultipleLocator(7))

bargroups = bar_plot(axb,data,colors=colors)

######################################################################
# second histogram plot
axh = plt.subplot(313)

plt.ylim(0,0.3)
n, bins, patches = axh.hist([allDelays,allDelays,allDelays], bins=num_bins,range=(0,num_bins),density=1)
######################################################################

def animate(frame):
    print("Updating frame {}".format(frame))
    dayList, deadList, caseList = extractListsPartial(byMeldedatum,frame)
    dayListR, deadListR, caseListR = extractListsPartial(byRefdatum,frame)
    dayListE, deadListE, caseListE = extractListsPartial(byErkdatum,frame)

    for i, b in enumerate(ax_bargroups[0]):
        b[0].set_height(caseList[i])
    for i, b in enumerate(ax_bargroups[1]):
        b[0].set_height(caseListR[i])
    for i, b in enumerate(ax_bargroups[2]):
        b[0].set_height(caseListE[i])

    # for i, b in enumerate(bargroups[0]):
    #     b[0].set_height(caseList[i])
    # for i, b in enumerate(bargroups[1]):
    #     b[0].set_height(caseListR[i])
    # for i, b in enumerate(bargroups[2]):
    #     b[0].set_height(caseListE[i])


    delays7days = [item for sublist in delayList[frame-7:frame] for item in sublist]
    curbins=np.histogram(delays7days,bins=num_bins, range=(0,num_bins),density=1)

    for i, p in enumerate(patches[2]):
        p.set_height(curbins[0][i])

    delays24days = [item for sublist in delayList[frame-24:frame] for item in sublist]
    curbins24=np.histogram(delays24days,bins=num_bins, range=(0,num_bins),density=1)

    for i, p in enumerate(patches[1]):
        p.set_height(curbins24[0][i])

anim=FuncAnimation(fig,animate,repeat=False,blit=False,frames=range(10,dayList[-1]+2), interval=1)
#anim.save('rki-data-inflow.gif', writer=ImageMagickWriter(fps=5))
#anim.save('rki-data-inflow.mp4',writer=FFMpegWriter(fps=5))
plt.show()

def loadcsv():

    with open('RKI_COVID19-29.4..csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        deaths = 0
        infected = 0
        total = 0
        for row in reader:
    #        print(row)
            newInf = int(row['AnzahlFall'])
            newDead = int(row['AnzahlTodesfall'])
            deaths=deaths+newDead
            infected = infected+newInf
            total = total + 1
        print("infected:"+str(infected)+". deaths:"+str(deaths)+", total rows="+str(total))
