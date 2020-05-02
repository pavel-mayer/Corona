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


def bar_plot(ax, data, colors=None, total_width=0.8, single_width=1, legend=True, legend_loc='upper left'):
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
        ax.legend(barsR, data.keys(),loc=legend_loc)

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
    chunksize = 5000
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

day0 = time.strptime("22.2.2020", "%d.%m.%Y") # struct_time
day0t = time.mktime(day0) # float timestamp since epoch
day0d = datetime.fromtimestamp(day0t) # datetime.datetime

def dateFromClock(c) -> time.struct_time:
    t = time.gmtime(int(c)/1000)
    return "{}.{}.{}".format(t.tm_mday, t.tm_mon,t.tm_year)

def dateFromDay(day) -> datetime:
    return day0d + timedelta(day)

def dateStrFromDay(day) -> str:
    result = dateFromDay(day)
    return "{}.{}.{}".format(result.day, result.month, result.year)

def dayFromDate(date) -> int:
    delta = date - day0d
    return delta.days

def dayFromTime(t) -> int:
    return dayFromDate(datetime.fromtimestamp(time.mktime(t)))

def dayFromStampStr(s) -> int:
    d = datetime.fromtimestamp(int(s) / 1000)
    delta = d - day0d
    return delta.days

FeiertagDates = ["10.4.2020", "13.4.2020", "1.5.2020"]
Feiertage = [dayFromTime(time.strptime(f, "%d.%m.%Y")) for f in FeiertagDates]

print("Feiertage",Feiertage)

def daysWorkedOrNot(fromDay, toDay) -> ([bool], [int]):
    workDays = []
    consecutiveDays = []

    for day in range(fromDay,toDay):
        date = dateFromDay(day)
        dayOfWeek = date.weekday() # 0=Mo, 6= So
        isFeiertag = day in Feiertage
        isAWorkDay = dayOfWeek>=0 and dayOfWeek <=4 and not isFeiertag
        workDays.append(isAWorkDay)

    if workDays[0]:
        consecutiveDay = dateFromDay(0).weekday()
    else:
        consecutiveDay = dateFromDay(0).weekday() - 5

    wasWorkDay = workDays[0]
    consecutiveDays.append(consecutiveDay)
    for day in range(fromDay+1,toDay):
        if workDays[day] == wasWorkDay:
            consecutiveDay = consecutiveDay + 1
        else:
            wasWorkDay = workDays[day]
            consecutiveDay = 0
        consecutiveDays.append(consecutiveDay)

    return (workDays, consecutiveDays)

def kindOfDayIndex(day, workDays, consecutiveDays):
    if workDays[day]:
        return consecutiveDays[day]
    else:
        return consecutiveDays[day]+5

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

def includePosErkbeginnDiffers(day,attrs):
    mDay = dayFromStampStr(attrs['Meldedatum'])
    return day >= 0 and int(attrs['IstErkrankungsbeginn']) == 1 and mDay != day

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

datenStand = allRecords[0]["attributes"]["Datenstand"]

print("Datenstand {}".format(datenStand))
print("Cases {} male {} female {} sum {}, dead {}".format(cases, maleCases, femaleCases, maleCases+femaleCases,dead))

#pretty(allRecords[0:100])

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

# returns an array of length 9 containing lists of delays for each day category
def extractDelaysBinned(records, beforeDay, kindOfDay):
    delayBins = {}

    for d in sorted(records.keys()):
        for r in records[d]:
            attrs = r['attributes']
            meldedatum = dayFromStampStr(attrs["Meldedatum"])
            if meldedatum < beforeDay:
                erkdatum = dayFromStampStr(attrs["Refdatum"])
                bod = kindOfDay[meldedatum]
                delay = meldedatum - erkdatum
                if bod in delayBins:
                    delayBins[bod].append(delay)
                else:
                    delayBins[bod] = [delay]
    return delayBins

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

def redistributed(ohneErkBeg, mitErkBeg, backDistribution, cutOffDay=None):
    #result = copy.deepcopy(mitErkBeg)
    result = {}

    # copy all Erkrankungen with date into result
    for d in sorted (mitErkBeg.keys()):
        if cutOffDay != None:
            if d >= cutOffDay:
                break
        result[d]=mitErkBeg[d]

    for d in sorted (ohneErkBeg.keys()):
        if cutOffDay != None:
            if d >= cutOffDay:
                return result
        for back, dist in enumerate(backDistribution):
            destDay = d - back
            if destDay in result:
                result[destDay] = result[destDay] + ohneErkBeg[d] * dist
    return result

def redistributedByDayKind(ohneErkBeg, mitErkBeg, backDistributions, kindOfDay, cutOffDay=None):
    result = {}

    print("backDistributions",backDistributions)
    # copy all Erkrankungen with date into result
    for d in sorted (mitErkBeg.keys()):
        if cutOffDay != None:
            if d >= cutOffDay:
                break
        result[d]=mitErkBeg[d]

    for d in sorted (ohneErkBeg.keys()):
        if cutOffDay != None:
            if d >= cutOffDay:
                return result
        backDistribution = backDistributions[kindOfDay[d]]
        for back, dist in enumerate(backDistribution):
            destDay = d - back
            if destDay in result:
                result[destDay] = result[destDay] + ohneErkBeg[d] * dist
    return result

def adjustForFuture(Erkrankte, backDistribution, lastDay):
    result = copy.deepcopy(Erkrankte)
    #cumBins = np.cumsum(backDistribution)
    #print(cumBins)

    for back, dist in enumerate(backDistribution):
        destDay = lastDay - back
        if destDay in result:
            #print(dist)
            result[destDay] = result[destDay] / dist
    return result

byMeldedatum = byDate(allRecords,'Meldedatum',includePos)
lastDay = sorted(byMeldedatum.keys())[-1]
byRefdatum = byDate(allRecords,'Refdatum',includePosNoErkbeginn)
byErkdatum = byDate(allRecords,'Refdatum',includePosErkbeginn)
#byErkdatum = byDate(allRecords,'Refdatum',includePosErkbeginnDiffers)

dayList, deadList, caseList = extractLists(byMeldedatum)
dayListR, deadListR, caseListR = extractLists(byRefdatum)
dayListE, deadListE, caseListE = extractLists(byErkdatum)

########################################################
def normalized(a, axis=-1, order=2):
    l2 = np.atleast_1d(np.linalg.norm(a, order, axis))
    l2[l2==0] = 1
    return a / np.expand_dims(l2, axis)

num_bins = 24

workDays, consecutiveDays = daysWorkedOrNot(0, lastDay+1)

print(tuple(zip(workDays, consecutiveDays)))

kindOfDay = [kindOfDayIndex(day, workDays, consecutiveDays) for day in range(len(workDays))]
print(kindOfDay)

dayCategoryNames = ["w0","w1","w2","w3","w4","h0","h1","h2","h3"]

binnedDelaysDict = extractDelaysBinned(byErkdatum, 1000, kindOfDay)
delaysByDayKind=[]
delaysByDayKindHist=[]
for d in range(9):
    bd = binnedDelaysDict[d]
    bdh = np.histogram(bd, bins=num_bins, range=(0, num_bins), density=1)
    delaysByDayKind.append(bd)
    delaysByDayKindHist.append(bdh[0])
    print("{}: avrg={} count={}".format(dayCategoryNames[d],np.average(bd), len(bd)))

########################################################
# redistribute cases with unknown erk-date
delayList = extractDelays(byErkdatum, 1000)
allDelays = [item for sublist in delayList for item in sublist]

ohneErkBeg = makeIndex(dayListR, caseListR)
mitErkBeg = makeIndex(dayListE, caseListE)

allBins = np.histogram(allDelays, bins=num_bins, range=(0, num_bins), density=1)

delays24days = [item for sublist in delayList[lastDay - 24:lastDay] for item in sublist]
last24bins = np.histogram(delays24days, bins=num_bins, range=(0, num_bins), density=1)

erkrankungen = redistributed(ohneErkBeg, mitErkBeg, allBins[0])
compErkDays, compErkValues = unmakeIndex(erkrankungen)

erkrankungen2 = redistributedByDayKind(ohneErkBeg, mitErkBeg, delaysByDayKindHist, kindOfDay)
compErkDays2, compErkValues2 = unmakeIndex(erkrankungen2)

futureErk = adjustForFuture(erkrankungen, last24bins[0], lastDay)
futErkDays, futErkValues = unmakeIndex(futureErk)

print("erkrankungen")
print(makeIndex(compErkDays[-num_bins:], compErkValues[-num_bins:]))
print("futureErk")
print(makeIndex(futErkDays[-num_bins:], futErkValues[-num_bins:]))

print("last24bins[0]")
print(last24bins[0])

totalCases = np.sum(caseList)
totalCompErk = np.sum(compErkValues)

print("Meldungen {}, Errechnete Erkranungen {}".format(totalCases, totalCompErk))

######################################################################
scale='log'
scale='linear'

title_pos_y = 1
title_loc = "left"

fig = plt.figure(figsize=(16, 10))
gs = fig.add_gridspec(4, 2)
fig.suptitle('Visualisierung der Meldeverzögerung von COVID-19 Daten in Deutschland (Stand {})'.format(datenStand),
             fontsize=16, horizontalalignment="left", x=0.05)
######################################################################
#ax = plt.subplot(311)
ax = fig.add_subplot(gs[0, :])

plt.title("Meldungseingänge ({} Fälle)".format(cases), y=title_pos_y, loc=title_loc)
plt.ylim(1,7000)
plt.yscale(scale)

ax_data = {
    "Gemeldete Infektionen":[dayList,caseList],
    "Ohne Erkrankungsdatum":[dayListR,caseListR],
    "Erkrankt":[dayListE, caseListE],
}

ax_colors = ['royalblue', 'firebrick', 'darkorange']
ax.xaxis.set_major_locator(ticker.MultipleLocator(7))

ax_bargroups = bar_plot(ax,ax_data,colors=ax_colors)

dateText = ax.text(1, 1, 'Tag {} ({})'.format(0, dateStrFromDay(0)),
                   verticalalignment='top', horizontalalignment='right',
                   transform=ax.transAxes, fontsize=12, bbox=dict(boxstyle='square,pad=1', fc='yellow', ec='none'))

######################################################################
axb = fig.add_subplot(gs[1, :])

plt.title("Erkrankungen (Fälle ohne Erkrankungsdatum umverteilt nach Verspätungswahrscheinlichkeit)", y=title_pos_y, loc=title_loc)
plt.ylim(1,7000)
plt.yscale(scale)

axb_data = {
#    "Gemeldete Infektionen":[dayList,caseList],
#    "Ohne Erkrankungsdatum":[dayListR,caseListR],
    "Berechnete Erkrankte (Stand heute)":[compErkDays, compErkValues],
    "Berechnete Erkrankte 2 (Stand heute)":[compErkDays2, compErkValues2],
    "Berechnete Erkrankte": [compErkDays, [0]*len(compErkValues)],
    "Erwartete Erkrankte (Hochrechnung)":[futErkDays, [0]*len(futErkValues)],
}
print("compErkValues2",compErkValues2)

axb_colors = ['tomato','green','cornflowerblue','yellow']
axb.xaxis.set_major_locator(ticker.MultipleLocator(7))

bargroups = bar_plot(axb,axb_data,colors=axb_colors)

######################################################################
#  histogram plot
axh = fig.add_subplot(gs[2, 0])

plt.title("Dauer Erkrankung bis Meldung (Verspätungswahrscheinlichkeit in Tagen)", y=title_pos_y, loc=title_loc)
plt.ylim(0,0.3)
plt.yscale('linear')
axh.xaxis.set_major_locator(ticker.MultipleLocator(1))


n, bins, patches = axh.hist([allDelays,allDelays,allDelays], bins=num_bins,range=(0,num_bins),density=1)
axh.legend(["im Gesamtzeitraum","in letzten 24 Tagen","in letzte 7 Tagen"], loc='upper right')
######################################################################
#  histogram plot 2
axbd = fig.add_subplot(gs[3, :])

plt.title("Dauer Erkrankung bis Meldung (Verspätungswahrscheinlichkeit in Tagen) nach Tagesart", y=title_pos_y, loc=title_loc)
plt.ylim(0,0.15)
plt.yscale('linear')
axbd.xaxis.set_major_locator(ticker.MultipleLocator(1))


nbd, binsbd, patchesbd = axbd.hist(delaysByDayKind, bins=num_bins, range=(0, num_bins), density=1)
axbd.legend(dayCategoryNames, loc='upper right')

######################################################################
axd = fig.add_subplot(gs[2, 1:])

plt.title("Vollständigkeit Erkrankungszahlen nach Tagen", y=title_pos_y, loc=title_loc)
plt.ylim(0,1.1)
plt.yscale('linear')

num_bars = 48
axd_data = {
    "Aktuelle Vollständigkeit nach Tagen":[range(num_bars),[0]*num_bars],
    "Gleitender Durchscnhitt (bis 24 Tage vor Ende)":[range(num_bars),[0]*num_bars],
}
axd_colors = ['plum','lime']

axd_bargroups = bar_plot(axd,axd_data,colors=axd_colors,legend_loc='lower right')
######################################################################
plt.subplots_adjust(left = 0.05, # the left side of the subplots of the figure
                    right = 0.95,  # the right side of the subplots of the figure
                    bottom = 0.05, # the bottom of the subplots of the figure
                    top = 0.925,    # the top of the subplots of the figure
                    wspace = 0.1, # the amount of width reserved for space between subplots,
                                  # expressed as a fraction of the average axis width
                    hspace = 0.3  # the amount of height reserved for space between subplots,
                                  # expressed as a fraction of the average axis height)
                    )
######################################################################

def setBarValues(bargroups, valueLists):
    for vi, values in enumerate(valueLists):
        for i, b in enumerate(bargroups[vi]):
            if i < len(values):
                b[0].set_height(values[i])
            else:
                b[0].set_height(0)

ratiosOfFinalList = {}

ratiosOfFinalAvrg = []

def initAnimation():
    global ratiosOfFinalAvrg
    ratiosOfFinalAvrg = []

def animate(frame):
    global ratiosOfFinalAvrg

    print("Updating frame {}".format(frame))

    dateText.set_text('Tag {} ({})'.format(frame, dateStrFromDay(frame)), )
    #################
    # Meldungseingänge
    dayList, deadList, caseList = extractListsPartial(byMeldedatum,frame)
    dayListR, deadListR, caseListR = extractListsPartial(byRefdatum,frame)
    dayListE, deadListE, caseListE = extractListsPartial(byErkdatum,frame)

    setBarValues(ax_bargroups, [caseList, caseListR,caseListE])

    #################
    # Verzögerungshistogramme

    delays7days = [item for sublist in delayList[frame-7:frame] for item in sublist]
    curbins=np.histogram(delays7days,bins=num_bins, range=(0,num_bins),density=1)

    for i, p in enumerate(patches[2]):
        p.set_height(curbins[0][i])

    delays24days = [item for sublist in delayList[frame-24:frame] for item in sublist]
    curbins24=np.histogram(delays24days,bins=num_bins, range=(0,num_bins),density=1)

    for i, p in enumerate(patches[1]):
        p.set_height(curbins24[0][i])

    #################
    # Erkrankungen

    erkrankungenC = redistributed(ohneErkBeg, mitErkBeg, allBins[0],cutOffDay=frame)
    compErkDaysC, compErkValuesC = unmakeIndex(erkrankungenC)

    futureErkC = adjustForFuture(erkrankungenC, ratiosOfFinalAvrg, frame)
    #futureErkC = adjustForFuture(erkrankungenC, curbins24[0], frame)
    futErkDaysC, futErkValuesC = unmakeIndex(futureErkC)
    print("futErkValuesC", futErkValuesC[-7:])

    setBarValues(bargroups, [compErkValues, compErkValues2, compErkValuesC,futErkValuesC])

    #################
    # Meldungsvollständigkeit

    ratiosOfFinal = np.array(compErkValuesC) / compErkValues[:len(compErkValuesC)]
    ratiosOfFinal = np.flip(ratiosOfFinal)

    # print("ratiosOfFinal")
    # print(ratiosOfFinal)
    # print("ratiosOfFinalAvrg")
    # print(ratiosOfFinalAvrg)

    ratiosOfFinalList[frame]=ratiosOfFinal

    if frame<lastDay-24:
        filt = 0.8
        if len(ratiosOfFinalAvrg):
            overLen = len(ratiosOfFinal) - len(ratiosOfFinalAvrg)
            if overLen>0:
                ratiosOfFinalAvrg = np.concatenate((ratiosOfFinalAvrg, ratiosOfFinal[-overLen:]))
            ratiosOfFinalAvrg = ratiosOfFinalAvrg * filt + ratiosOfFinal*(1-filt)
        else:
            ratiosOfFinalAvrg = ratiosOfFinal

    setBarValues(axd_bargroups, [ratiosOfFinal, ratiosOfFinalAvrg])


anim=FuncAnimation(fig,animate,repeat=False,blit=False,frames=range(1,dayList[-1]+2), interval=1, init_func=initAnimation)
#anim=FuncAnimation(fig,animate,repeat=False,blit=False,frames=range(10,dayList[-1]+2), interval=1)
#anim=FuncAnimation(fig,animate,repeat=False,blit=False,frames=range(0,1), interval=1)

#anim.save('rki-data-inflow-'+scale+'.gif', writer=ImageMagickWriter(fps=1))
#anim.save('rki-data-inflow.mp4',writer=FFMpegWriter(fps=1))
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
