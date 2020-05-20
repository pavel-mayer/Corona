#!/usr/bin/env python3.6

# Quick hack to browse RKI-NPGEO-Data, Pavel Mayer 2020,
# License: Use freely at your own risk.

# pip install Click==7.0 Flask==1.1.1 itsdangerous==1.1.0 Jinja2==2.10.3 MarkupSafe==1.1.1 uWSGI==2.0.18 Werkzeug==0.16.0 dash=1.11.0
# pip install: dash pandas datatable feather-format

import locale

print("Locale:"+str(locale.getlocale()))
locale.setlocale(locale.LC_ALL, 'de_DE')
print("Locale set to:"+str(locale.getlocale()))

import cov_dates as cd
import pm_util as pmu

import os
import flask
from flask import render_template

import dash
import dash_table
from dash_table.Format import Format, Scheme, Sign, Symbol
import dash_core_components as dcc
import dash_html_components as html

import pandas as pd
import numpy as np
import datatable as dt
import json
import dash_table.FormatTemplate as FormatTemplate
#import markdown
import socket

print("Running on host '{}'".format(socket.gethostname()))

def pretty(jsonmap):
    print(json.dumps(jsonmap, sort_keys=False, indent=4, separators=(',', ': ')))

# creates a new table by joining all columns from smaller table according to same values of keyFieldName
# largerTable must contain all keys present in smallertable but not vice versa
def join(largerTable, smallerTable, keyFieldName):
    keys = smallerTable[:, keyFieldName].to_list()[0]
    extTable = largerTable.copy()
    for colName in smallerTable.names:
        if colName != keyFieldName:
            values = smallerTable[:, colName].to_list()[0]
            valuesDict = dict(zip(keys, values))

            extTable = extTable[:, dt.f[:].extend({colName: 0.0})]

            for i, lk in enumerate(extTable[:,keyFieldName].to_list()[0]):
                if lk in valuesDict:
                    extTable[i,colName] = valuesDict[lk]
    return extTable

# creates a new table by replacing all values in larger_table with values in matching columns in smaller_table according
# to same values of keyFieldName
# largerTable must contain all keys (key values) present in smallertable but not vice versa
def merge(largerTable, smallerTable, keyFieldName):
    keys = smallerTable[:, keyFieldName].to_list()[0]
    extTable = largerTable.copy()
    for colName in smallerTable.names:
        if colName != keyFieldName:
            values = smallerTable[:, colName].to_list()[0]
            valuesDict = dict(zip(keys, values))

            #extTable = extTable[:, dt.f[:].extend({colName: 0.0})]

            for i, lk in enumerate(extTable[:,keyFieldName].to_list()[0]):
                if lk in valuesDict:
                    extTable[i,colName] = valuesDict[lk]
    return extTable

FormatFixed1 = Format(
    precision=1,
    scheme=Scheme.fixed,
    symbol=Symbol.no,
    decimal_delimiter=',',
)
FormatFixed2 = Format(
    precision=2,
    scheme=Scheme.fixed,
    symbol=Symbol.no,
    decimal_delimiter=',',
)

FormatInt_EN = Format(
                precision=0,
                scheme=Scheme.fixed,
                symbol=Symbol.no,
#                symbol_suffix=u'˚F'
            )
FormatInt = Format(
                precision=0,
                decimal_delimiter=',',
                group=FormatTemplate.Group.yes,
                group_delimiter='.',
                scheme=Scheme.fixed,
                symbol=Symbol.no,
#                symbol_suffix=u'˚F'
            )
FormatIntBracketed = Format(
                nully='',
                precision=0,
                scheme=Scheme.fixed,
                symbol=Symbol.yes,
                symbol_suffix=')',
                symbol_prefix='(',
)
FormatIntPlus = Format(
                nully='',
                precision=0,
                scheme=Scheme.fixed,
                sign=Sign.positive,
                symbol=Symbol.no,
            )
FormatIntRatio = Format(
                nully='',
                precision=0,
                scheme=Scheme.fixed,
                symbol=Symbol.yes,
                symbol_prefix='1/',
                group=FormatTemplate.Group.yes,
                group_delimiter='.',
)

def loadData(dataFilename):
    print("Loading " + dataFilename)

    fullTable = dt.fread(dataFilename)
    print("Loading done loading table from ‘" + dataFilename + "‘, keys:")
    print(fullTable.keys())
    cases = fullTable[:, 'AnzahlFall'].sum()[0, 0]
    dead = fullTable[:, 'AnzahlTodesfall'].sum()[0, 0]
    lastDay=fullTable[:,'MeldeDay'].max()[0,0]
    lastnewCaseOnDay=fullTable[:,'newCaseOnDay'].max()[0,0]
    print("File stats: lastDay {} lastnewCaseOnDay {} cases {} dead {}".format(lastDay, lastnewCaseOnDay, cases, dead))
    newTable=fullTable[:,dt.f[:].extend({"erkMeldeDelay": dt.f.MeldeDay-dt.f.RefDay})]
    return newTable, lastDay

def processData(fullCurrentTable, forDay):

    fullTable = fullCurrentTable[(dt.f.newCaseOnDay <= forDay) | (dt.f.newCaseBeforeDay < forDay),:]

    lastDay = fullTable[:, 'MeldeDay'].max()[0, 0]
    lastnewCaseOnDay = fullTable[:, 'newCaseOnDay'].max()[0, 0]

    #print(newTable.keys())

    #dt.by(dt.f.Bundesland)]
    alldays=fullTable[:,
              [dt.sum(dt.f.AnzahlFall),
               dt.sum(dt.f.FaellePro100k),
               dt.sum(dt.f.AnzahlTodesfall),
               dt.sum(dt.f.TodesfaellePro100k),
               dt.first(dt.f.Bevoelkerung),
               dt.max(dt.f.MeldeDay),
               dt.first(dt.f.LandkreisTyp),
               dt.first(dt.f.Bundesland)],
    dt.by(dt.f.Landkreis)]

    '''
    bevoelkerung = alldays[:, [dt.sum(dt.f.Bevoelkerung), dt.sum(dt.f.AnzahlFall), dt.sum(dt.f.AnzahlTodesfall),], dt.by(dt.f.Bundesland)]
    bevoelkerung=bevoelkerung[:,dt.f[:].extend({"FaellePro100k": dt.f.AnzahlFall * 100000 / dt.f.Bevoelkerung})]
    bevoelkerung=bevoelkerung[:,dt.f[:].extend({"TodesfaellePro100k": dt.f.AnzahlTodesfall * 100000 / dt.f.Bevoelkerung})]

    alldaysBundeslaender = fullTable[:,
              [dt.sum(dt.f.AnzahlFall),
               dt.sum(dt.f.FaellePro100k),
               dt.sum(dt.f.AnzahlTodesfall),
               dt.sum(dt.f.TodesfaellePro100k),
               dt.first(dt.f.Bevoelkerung),
               dt.max(dt.f.MeldeDay),
               dt.first(dt.f.LandkreisTyp),
               dt.first(dt.f.Bundesland)],
            dt.by(dt.f.Bundesland)]

    alldaysBundeslaender[:, "Bevoelkerung"] = bevoelkerung[:, "Bevoelkerung"]
    alldaysBundeslaender[:, "Landkreis"] = bevoelkerung[:, "Bundesland"]
    alldaysBundeslaender[:, "FaellePro100k"] = bevoelkerung[:, "FaellePro100k"]
    alldaysBundeslaender[:, "TodesfaellePro100k"] = bevoelkerung[:, "TodesfaellePro100k"]
    alldaysBundeslaender[:, "LandkreisTyp"] = "BL"
    alldays.rbind(alldaysBundeslaender, force=True)


    # specialDump = fullTable[(dt.f.newCaseOnDay > lastDay - 7) & (dt.f.Landkreis == "LK Main-Tauber-Kreis"), :]
    # specialDump.to_csv("special.csv")
    #
    # specialDump2 = fullTable[(dt.f.newCaseOnDay > lastDay - 14) & (dt.f.newCaseOnDay < lastDay-7) & (dt.f.Landkreis == "LK Main-Tauber-Kreis"), :]
    # specialDump2.to_csv("special2.csv")
    ##############################################################################
    '''

    last7daysRecs = fullTable[((dt.f.newCaseOnDay > lastDay - 7) & (dt.f.MeldeDay > lastDay - 14)), :]
    #last7daysRecs.to_csv("last7daysRecs.csv")
    last7days = last7daysRecs[:,
                [dt.sum(dt.f.AnzahlFall),
               dt.sum(dt.f.FaellePro100k),
               dt.sum(dt.f.AnzahlTodesfall),
               dt.sum(dt.f.TodesfaellePro100k)],
    dt.by(dt.f.Landkreis)]
    last7days.names=["Landkreis","AnzahlFallLetzte7Tage","FaellePro100kLetzte7Tage","AnzahlTodesfallLetzte7Tage",
                     "TodesfaellePro100kLetzte7Tage"]
    '''
    last7daysBL = last7daysRecs[:,
                [dt.sum(dt.f.AnzahlFall),
                 dt.sum(dt.f.AnzahlTodesfall),
                 dt.sum(dt.f.Bevoelkerung)],
                dt.by(dt.f.Bundesland)]
    last7daysBL.names = ["Landkreis", "AnzahlFallLetzte7Tage", "AnzahlTodesfallLetzte7Tage","Bevoelkerung"]
    last7daysBL = last7daysBL[:, dt.f[:].extend({"FaellePro100kLetzte7Tage": dt.f.AnzahlFallLetzte7Tage * 100000 / dt.f.Bevoelkerung})]
    last7daysBL = last7daysBL[:, dt.f[:].extend({"TodesfaellePro100kLetzte7Tage": dt.f.AnzahlTodesfallLetzte7Tage * 100000 / dt.f.Bevoelkerung})]
    print("last7daysBL")
    print(last7daysBL)
    last7days.rbind(last7daysBL, force=True)
    '''
    # clip case to zero
    last7days[dt.f.AnzahlFallLetzte7Tage <0, "AnzahlFallLetzte7Tage"] = 0
    last7days[dt.f.FaellePro100kLetzte7Tage <0, "FaellePro100kLetzte7Tage"] = 0
    last7days[dt.f.AnzahlTodesfallLetzte7Tage <0, "AnzahlTodesfallLetzte7Tage"] = 0
    last7days[dt.f.TodesfaellePro100kLetzte7Tage <0, "TodesfaellePro100kLetzte7Tage"] = 0
    ##############################################################################

    lastWeek7daysRecs = fullTable[(dt.f.newCaseOnDay > lastDay-14) & (dt.f.newCaseOnDay <= lastDay-7)
                                  & (dt.f.MeldeDay > lastDay - 21)& (dt.f.MeldeDay <= lastDay - 7), :]
    lastWeek7days=lastWeek7daysRecs[:,
                    [dt.sum(dt.f.AnzahlFall),
               dt.sum(dt.f.FaellePro100k),
               dt.sum(dt.f.AnzahlTodesfall),
               dt.sum(dt.f.TodesfaellePro100k)],
       dt.by(dt.f.Landkreis)]
    #lastWeek7days[dt.f[1:] < 0, dt.f[1:]] = 0
    lastWeek7days.names=["Landkreis","AnzahlFallLetzte7TageDavor","FaellePro100kLetzte7TageDavor",
                         "AnzahlTodesfallLetzte7TageDavor","TodesfaellePro100kLetzte7TageDavor"]
    '''
    lastWeek7daysBL = lastWeek7daysRecs[:,
                  [dt.sum(dt.f.AnzahlFall),
                   dt.sum(dt.f.AnzahlTodesfall),
                   dt.sum(dt.f.Bevoelkerung)],
                  dt.by(dt.f.Bundesland)]
    lastWeek7daysBL.names = ["Landkreis", "AnzahlFallLetzte7TageDavor", "AnzahlTodesfallLetzte7TageDavor", "Bevoelkerung"]
    lastWeek7daysBL = lastWeek7daysBL[:, dt.f[:].extend({"FaellePro100kLetzte7TageDavor": dt.f.AnzahlFallLetzte7TageDavor * 100000 / dt.f.Bevoelkerung})]
    lastWeek7daysBL = lastWeek7daysBL[:, dt.f[:].extend({"TodesfaellePro100kLetzte7TageDavor": dt.f.AnzahlTodesfallLetzte7TageDavor * 100000 / dt.f.Bevoelkerung})]
    print("lastWeek7daysBL")
    print(lastWeek7daysBL)
    lastWeek7days.rbind(lastWeek7daysBL, force=True)
    '''

    lastWeek7days[dt.f.AnzahlFallLetzte7TageDavor <0, "AnzahlFallLetzte7TageDavor"] = 0
    lastWeek7days[dt.f.FaellePro100kLetzte7TageDavor <0, "FaellePro100kLetzte7TageDavor"] = 0
    lastWeek7days[dt.f.AnzahlTodesfallLetzte7TageDavor <0, "AnzahlTodesfallLetzte7TageDavor"] = 0
    lastWeek7days[dt.f.TodesfaellePro100kLetzte7TageDavor <0, "TodesfaellePro100kLetzte7TageDavor"] = 0

    allDaysExt0 = join(alldays, last7days, "Landkreis")
    allDaysExt1 = join(allDaysExt0, lastWeek7days, "Landkreis")

    Rw = (dt.f.AnzahlFallLetzte7Tage+5)/(dt.f.AnzahlFallLetzte7TageDavor + 5)

    allDaysExt2=allDaysExt1[:,dt.f[:].extend({"AnzahlFallTrend":  Rw})]
    #allDaysExt2[dt.f.AnzahlFallLetzte7TageDavor == 0, "AnzahlFallTrend"] = 1
    #allDaysExt2[dt.f.AnzahlFallLetzte7TageDavor == 0 & dt.f.AnzahlFallLetzte7Tage >0 , "AnzahlFallTrend"] = 1
    #allDaysExt2[dt.f.AnzahlFallLetzte7TageDavor == 0 & dt.f.AnzahlFallLetzte7Tage == 0 , "AnzahlFallTrend"] = 1

    allDaysExt3=allDaysExt2[:,dt.f[:].extend({"FaellePro100kTrend": dt.f.FaellePro100kLetzte7Tage-dt.f.FaellePro100kLetzte7TageDavor})]
    allDaysExt4=allDaysExt3[:,dt.f[:].extend({"TodesfaellePro100kTrend": dt.f.TodesfaellePro100kLetzte7Tage-dt.f.TodesfaellePro100kLetzte7TageDavor})]

    allDaysExt5=allDaysExt4[:,dt.f[:].extend({"Kontaktrisiko": dt.f.Bevoelkerung/6.25/((dt.f.AnzahlFallLetzte7Tage+dt.f.AnzahlFallLetzte7TageDavor)*Rw)})]
    allDaysExt6 = allDaysExt5[:, dt.f[:].extend({"LetzteMeldung": lastDay - dt.f.MeldeDay})]
    allDaysExt6b = allDaysExt6[:, dt.f[:].extend({"LetzteMeldungNeg": dt.f.MeldeDay - lastDay})]

    allDaysExt6b[dt.f.Kontaktrisiko * 2 == dt.f.Kontaktrisiko, "Kontaktrisiko"] = 99999

    sortedByRisk = allDaysExt6b.sort(["Kontaktrisiko","LetzteMeldung","FaellePro100k"])
    #print(sortedByRisk)
    allDaysExt=sortedByRisk[:,dt.f[:].extend({"Rang": 0})]
    allDaysExt[:,"Rang"]=np.arange(1,allDaysExt.nrows+1)
    #print(allDaysExt)
    allDaysExt.materialize()
    return allDaysExt

def loadAndProcessData(fileName):
    currentFullTable, lastDay = loadData(fileName)
    todayTable = processData(currentFullTable, lastDay).sort("Landkreis")
    yesterdayTable = processData(currentFullTable, lastDay-1).sort("Landkreis")

    print(currentFullTable)
    print(todayTable)
    print(yesterdayTable)

    resultTable=todayTable[:,dt.f[:].extend({"RangChange": 0})]
    rangChange = np.subtract(yesterdayTable[:,"Rang"],todayTable[:,"Rang"])
    resultTable[:,"RangChange"] = rangChange

    resultTable=resultTable[:,dt.f[:].extend({"RangYesterday": 0})]
    resultTable[:,"RangYesterday"] = yesterdayTable[:,"Rang"]

    resultTable=resultTable[:,dt.f[:].extend({"RangChangeStr": "-"})]

    rangChangeStrs = np.full(len(rangChange), "*")
    for i, rc in enumerate(rangChange):
        #print(i, rc)

        rangChangeStr = ""
        if rc > 0:
            rangChangeStr = "▲"
        if rc < 0:
            rangChangeStr = "▼"

        resultTable[i, "RangChangeStr"] = rangChangeStr
        rangChangeStrs[i] = rangChangeStr

    #print(rangChangeStrs)

    print("Column names frame order:", list(enumerate(resultTable.names)))
    resultTable2 = resultTable.sort("Rang")
    #print(resultTable2)
    data = resultTable2.to_pandas()
    print(data)

    return data

defaultColWidth=70
#charWidth=7

def colWidth(pixels):
    return pixels

def colWidthStr(pixels):
    #print("colWidth pixels=", pixels)
    return "{}px".format(colWidth(pixels))

def makeColumns():
    desiredOrder = [
        ('Rang', ['Rang', 'Rang'], 'numeric', FormatInt, colWidth(40)),
        ('RangChangeStr', ['Rang', ''], 'text', Format(), colWidth(20)),
        ('RangChange', ['Rang', '+/-'], 'numeric', FormatIntPlus, colWidth(34)),
        ('RangYesterday', ['Rang', 'Gestern'], 'numeric', FormatIntBracketed, colWidth(defaultColWidth)),
        ('Kontaktrisiko', ['Risiko', '1/N'], 'numeric', FormatIntRatio, colWidth(71)),
        ('Landkreis', ['Kreis', 'Name'], 'text', Format(), colWidth(298)),
        ('Bundesland', ['Kreis', 'Bundesland'], 'text', Format(), colWidth(190)),
        ('LandkreisTyp', ['Kreis', 'Art'], 'text', Format(), colWidth(30)),
        ('Bevoelkerung', ['Kreis', 'Einwohner'], 'numeric', FormatInt, colWidth(90)),
        ('LetzteMeldungNeg', ['Kreis', 'Letzte Meldung'], 'numeric', FormatInt, colWidth(70)),
        ('AnzahlFallTrend', ['Fälle', 'RwK'], 'numeric', FormatFixed2, colWidth(70)),
        ('AnzahlFallLetzte7Tage', ['Fälle', 'letzte 7 Tage'], 'numeric', FormatInt, colWidth(defaultColWidth)),
        ('AnzahlFallLetzte7TageDavor', ['Fälle', 'vorl. 7 Tage'], 'numeric', FormatInt, colWidth(defaultColWidth)),
        ('AnzahlFall', ['Fälle', 'total'], 'numeric', FormatInt, colWidth(60)),
        ('FaellePro100kLetzte7Tage', ['Fälle je 100000', 'letzte 7 Tage'], 'numeric', FormatFixed1, colWidth(defaultColWidth)),
        ('FaellePro100kLetzte7TageDavor', ['Fälle je 100000', 'vorl. 7 Tage'], 'numeric', FormatFixed1, colWidth(defaultColWidth)),
        ('FaellePro100kTrend', ['Fälle je 100000', 'Diff.'], 'numeric', FormatFixed1, colWidth(defaultColWidth)),
        ('FaellePro100k', ['Fälle je 100000', 'total'], 'numeric', FormatFixed1, colWidth(60)),
        ('AnzahlTodesfallLetzte7Tage', ['Todesfälle', 'letzte 7 Tage'], 'numeric', FormatInt, colWidth(defaultColWidth)),
        ('AnzahlTodesfallLetzte7TageDavor', ['Todesfälle', 'vorl. 7 Tage'], 'numeric', FormatInt, colWidth(defaultColWidth)),
        ('AnzahlTodesfall', ['Todesfälle', 'total'], 'numeric', FormatInt, colWidth(defaultColWidth)),
        ('TodesfaellePro100kLetzte7Tage', ['Todesfälle je 100000', 'letzte 7 Tage'], 'numeric', FormatFixed2, colWidth(defaultColWidth)),
        ('TodesfaellePro100kLetzte7TageDavor', ['Todesfälle je 100000', 'vorl. 7 Tage'], 'numeric', FormatFixed2, colWidth(defaultColWidth)),
        ('TodesfaellePro100kTrend', ['Todesfälle je 100000', 'Diff.'], 'numeric', FormatFixed2, colWidth(defaultColWidth)),
        ('TodesfaellePro100k', ['Todesfälle je 100000', 'total'], 'numeric', FormatFixed2, colWidth(60)),
    ]

    orderedCols, orderedNames, orderedTypes, orderFormats, orderWidths = zip(*desiredOrder)
    #orderedIndices = np.array(orderedIndices)+1
    #print(orderedIndices)

    columns = [{'name': L1, 'id': L2, 'type':L3, 'format':L4} for (L1,L2,L3,L4) in zip(orderedNames,orderedCols,orderedTypes,orderFormats)]
    widths = {}
    totalWidth = 0
    minWidth = 0
    maxWidth = 0
    #print(orderedCols)
    #print(orderWidths)
    for i, n in enumerate(orderedCols):
        #print(i,n)
        widths[n] = str(orderWidths[i])+"px"
        thisColWidth = orderWidths[i]
        totalWidth = totalWidth + thisColWidth # + 1
        if thisColWidth < minWidth or minWidth == 0:
            minWidth = thisColWidth
        if thisColWidth > maxWidth:
            maxWidth = thisColWidth

    #print("columns=",columns)
    return columns, widths, totalWidth+1, minWidth, maxWidth


server = flask.Flask(__name__)

#@server.route('/covid/Landkreise/about')
@server.route('/covid/risks/about')
def index():
    return 'Nothing to see here!'

app = dash.Dash(
    __name__,
    server=server,
    routes_pathname_prefix='/covid/risks/',
    # routes_pathname_prefix='/covid/Landkreise/',
    #    assets_external_path = 'http://covid/Landkreise/assets'
)
#app.css.append_css({'external_url': 'assets/reset.css'})

fullTableFilename = "full-latest.csv"
cacheFilename = "data-cached.feather"

FORCE_REFRESH_CACHE = True

if FORCE_REFRESH_CACHE or not os.path.isfile(cacheFilename) or os.path.getmtime(fullTableFilename) > os.path.getmtime(cacheFilename) :
    dframe = loadAndProcessData(fullTableFilename)
    dframe.to_feather(cacheFilename)
else:
    print("Loading data cache from ‘"+cacheFilename+"‘")
    dframe = pd.read_feather(cacheFilename)

maxDay = float(dframe["MeldeDay"].max())
#print(maxDay)
dataVersionDate = cd.dateStrWDMYFromDay(maxDay+1)
print("Loading done, max Day {} date {}".format(maxDay, dataVersionDate))

print("Creating Datatable")
data = dframe.to_dict("records")
columns, colWidths, totalWidth, minWidth, maxWidth = makeColumns()
totalWidthStr = colWidthStr(totalWidth)
minWidthStr = colWidthStr(minWidth)
maxWidthStr = colWidthStr(maxWidth)

print("colWidths", colWidths)
print("totalWidthStr",totalWidthStr)
print("minWidthStr",minWidthStr)
print("maxWidthStr",maxWidthStr)

colors = {
    'background': 'rgb(50, 50, 50)',
    'text': 'white'
}

conditionDanger="conditionDanger"
conditionTooHigh="conditionTooHigh"
conditionSerious="conditionSerious"
conditionGood="conditionGood"
conditionSafe="conditionSafe"

conditions = [conditionDanger, conditionTooHigh, conditionSerious, conditionGood, conditionSafe]

conditionStyleDict = {
    conditionDanger : {'backgroundColor': 'firebrick', 'color': 'white'},
    conditionTooHigh: {'color': 'tomato'},
    conditionSerious: {'color': 'yellow'},
    conditionGood: {'color': 'lightgreen'},
    conditionSafe: {'backgroundColor': 'green', 'color': 'white'}
}

def conditionalStyle(condition, filterExprs, columns):
    #print("condition", condition, "filterExprc", filterExprs)
    template = {
        'if': {
            'filter_query': filterExprs[condition],
            'column_id': columns
        },
    }
    for k in conditionStyleDict[condition].keys():
        template = {**template, **conditionStyleDict[condition]}
    #print ("returning", pretty(template))
    return template

def Not(condition):
    return "!("+condition+")"

def braced(condition):
    return "("+condition+")"

KontaktrisikoClass = {
    conditionDanger : '{Kontaktrisiko} > 0 && {Kontaktrisiko} < 100',
    conditionTooHigh: '{Kontaktrisiko} >= 100 && {Kontaktrisiko} < 1000',
    conditionSerious: '{Kontaktrisiko} >= 1000 && {Kontaktrisiko} < 2500',
    conditionGood: '{Kontaktrisiko} >= 2500 && {Kontaktrisiko} < 10000',
    conditionSafe: '{Kontaktrisiko} > 10000'
}

FaellePro100kLetzte7TageClass = {
    conditionDanger : '{FaellePro100kLetzte7Tage} > 50',
    conditionTooHigh: '{FaellePro100kLetzte7Tage} > 20 && {FaellePro100kLetzte7Tage} < 50',
    conditionSerious: '{FaellePro100kLetzte7Tage} > 5 && {FaellePro100kLetzte7Tage} < 20',
    conditionGood: '{FaellePro100kLetzte7Tage} >= 1 && {FaellePro100kLetzte7Tage} < 5',
    conditionSafe: '{FaellePro100kLetzte7Tage} < 1'
}

AnzahlFallTrendClass = {
    conditionDanger : '{AnzahlFallTrend} > 3',
    conditionTooHigh: '{AnzahlFallTrend} > 1 && {AnzahlFallTrend} <= 3',
    conditionSerious: '{AnzahlFallTrend} > 0.9 && {AnzahlFallTrend} <= 1',
    conditionGood: '{AnzahlFallTrend} > 0.3 && {AnzahlFallTrend} <= 0.9',
    conditionSafe: '{AnzahlFallTrend} < 0.3'
}

LandkreisClass = {
    conditionDanger : KontaktrisikoClass[conditionDanger] +" || "+ braced(FaellePro100kLetzte7TageClass[conditionDanger])+" || "+braced(AnzahlFallTrendClass[conditionDanger]),
    conditionTooHigh: KontaktrisikoClass[conditionTooHigh]+" && "+ Not(FaellePro100kLetzte7TageClass[conditionDanger])+" && "+ Not(AnzahlFallTrendClass[conditionDanger]),
    conditionSerious: KontaktrisikoClass[conditionSerious]+" && "+ Not(FaellePro100kLetzte7TageClass[conditionDanger])+" && "+ Not(AnzahlFallTrendClass[conditionDanger]),
    conditionGood: KontaktrisikoClass[conditionGood]+" && "+ Not(FaellePro100kLetzte7TageClass[conditionDanger])+" && "+ Not(AnzahlFallTrendClass[conditionDanger]),
    conditionSafe: KontaktrisikoClass[conditionSafe]+" && "+ Not(FaellePro100kLetzte7TageClass[conditionDanger])+" && "+ Not(AnzahlFallTrendClass[conditionDanger]),
}

xClass = {
    conditionDanger : '',
    conditionTooHigh: '',
    conditionSerious: '',
    conditionGood: '',
    conditionSafe: ''
}

def conditionalStyles(conditionClass, columns):
    #print("conditionalStyles conditionClass", conditionClass, "columns", columns)
    result = []
    for c in conditions:
        #print("condition", c)
        style = conditionalStyle(c, conditionClass, columns)
        #print("appending Style", pretty(style))
        result.append(style)
    return result

def makeNonDefaultColWidthCondStyles(colWidths, default):
    result = []
    for col in colWidths.keys():
        if colWidths[col] != colWidthStr(default):
            csd = {
                'if': {'column_id': col},
                'width': colWidths[col],
                'maxWidth': colWidths[col],
                'minWidth': colWidths[col],
            }
            result.append(csd)
    return result

nonDefaultColWidthStyles = makeNonDefaultColWidthCondStyles(colWidths, defaultColWidth)
#print("nonDefaultColWidthStyles",pretty(nonDefaultColWidthStyles))

# def make_width_style_conditional():
#     result = [
#         {
#             'if': {'column_id': 'Landkreis'},
#             'width': colWidths['Landkreis'],
#             'maxWidth': colWidths['Landkreis'],
#             'minWidth': colWidths['Landkreis'],
#
#         },
#         {
#             'if': {'column_id': 'Bundesland'},
#             'width': colWidths['Bundesland'],
#             'maxWidth': colWidths['Bundesland'],
#             'minWidth': colWidths['Bundesland'],
#         },
#         {
#             'if': {'column_id': 'Kontaktrisiko'},
#             'width': colWidths['Kontaktrisiko'],
#             'maxWidth': colWidths['Kontaktrisiko'],
#             'minWidth': colWidths['Kontaktrisiko'],
#         },
#     ]
#     return result

#width_style_conditional = make_width_style_conditional()
width_style_conditional = nonDefaultColWidthStyles

def make_style_data_conditional():
    result = [
        # {
        #     'if': {'column_id': 'Kontaktrisiko'},
        #     'border-left': '3px solid blue',
        #     'border-right': '3px solid blue',
        # },
        {
            'if': {'row_index': 'odd'},
            'backgroundColor': 'rgb(70, 70, 70)'
        },
        {
            'if': {
                'filter_query': '{FaellePro100kTrend} > 0',
                'column_id': 'FaellePro100kTrend'
            },
            'color': 'tomato'
        },
        {
            'if': {
                'filter_query': '{FaellePro100kTrend} <= 0',
                'column_id': 'FaellePro100kTrend'
            },
            'color': 'lightgreen'
        },
        ################################################################
        {
            'if': {
                'filter_query': '{RangChange} > 0',
                'column_id': 'RangChangeStr'
            },
            'color': 'tomato'
        },
        {
            'if': {
                'filter_query': '{RangChange} < 0',
                'column_id': 'RangChangeStr'
            },
            'color': 'lightgreen'
        },
        ################################################################
        {
            'if': {'column_id': 'RangYesterday'},
            'border-left': 'none',
            #'border-right': '3px solid blue',
        },
        {
            'if': {'column_id': 'RangChange'},
            'border-left': 'none',
            # 'border-right': '3px solid blue',
        },
        ################################################################
        {
            'if': {
                'filter_query': '{RangChange} > 0',
                'column_id': 'RangChange'
            },
            'color': 'tomato'
        },
        {
            'if': {
                'filter_query': '{RangChange} < 0',
                'column_id': 'RangChange'
            },
            'color': 'lightgreen'
        },
        {
            'if': {
                'filter_query': '{RangChange} = 0',
                'column_id': 'RangChange'
            },
            'color': colors["background"]
        },
        ################################################################

    ]
#    result = result + nonDefaultColWidthStyles
    result = result + width_style_conditional
    # result = result + conditionalStyles(FaellePro100kLetzte7TageClass, ['FaellePro100kLetzte7Tage'])
    # result = result + conditionalStyles(AnzahlFallTrendClass, ['AnzahlFallTrend'])
    # result = result + conditionalStyles(KontaktrisikoClass, ['Kontaktrisiko'])
    # result = result + conditionalStyles(LandkreisClass, ['Landkreis'])
    result = result + conditionalStyles(FaellePro100kLetzte7TageClass, 'FaellePro100kLetzte7Tage')
    result = result + conditionalStyles(AnzahlFallTrendClass, 'AnzahlFallTrend')
    result = result + conditionalStyles(KontaktrisikoClass, 'Kontaktrisiko')
    result = result + conditionalStyles(LandkreisClass, 'Landkreis')

    return result




cs_data = make_style_data_conditional()
#print("cs_data", cs_data)
#pretty(cs_data)

tableHeight='90vh'

#tableWidth= '2400px'
tableWidth = totalWidthStr

h_table = dash_table.DataTable(
    id='table',
    columns=columns,
    data=data,
    sort_action='native',
    filter_action='native',
    page_size= 500,
    style_table = {
            'minWidth': tableWidth, 'width': tableWidth, 'maxWidth': tableWidth,
            'minHeight': tableHeight, 'height': tableHeight, 'maxHeight': tableHeight,
            'overflow-y': 'scroll',
#            'tableLayout': 'auto'
            'table-layout': 'fixed'
    },
    sort_by = [{"column_id": "Kontaktrisiko", "direction": "asc"}],
    fixed_rows={ 'headers': True, 'data': 0 },
    style_cell={'textAlign': 'right',
                'padding': '5px',
                'backgroundColor': colors['background'],
                'color': 'white',
                },
    style_data={
        'border-bottom': '1px solid '+colors['background'],
        'border-left': '1px solid rgb(128, 128, 128)',
        'border-right': '1px solid rgb(128, 128, 128)',
#        'textOverflow': 'ellipsis',
         'width': colWidthStr(defaultColWidth),
         'maxWidth': maxWidthStr,
         'minWidth': minWidthStr
    },
    # style_as_list_view = True,
    style_header={
        'backgroundColor': colors['background'],
        'color': 'white',
        #'fontWeight': 'bold',
        #        'height': '100px',
        'whiteSpace': 'normal',
        'height': 'auto',
        'overflow-wrap': 'normal',
        'textAlign': 'center',
    },
    merge_duplicate_headers=True,
    style_cell_conditional=[
        {
            'if': {'column_id': 'Landkreis'},
            'textAlign': 'left'
        },
        {
            'if': {'column_id': 'Bundesland'},
            'textAlign': 'left'
        },
        {
            'if': {'column_id': 'RangYesterday'},
            'textAlign': 'center'
        },
        {
            'if': {'column_id': 'Rang'},
            'textAlign': 'center'
        },
        {
            'if': {'column_id': 'Kontaktrisiko'},
            'textAlign': 'center'
        },

    ],
    style_data_conditional = cs_data,
#    style_header_conditional= width_style_conditional

)

def readExplanation():
    with open('explainer.md', 'r') as file:
        data = file.read()
    return data

appDate = os.path.getmtime("app.py")
print(appDate)
appDateStr=cd.dateTimeStrFromTime(appDate)
print(appDateStr)

introClass="intro"
bodyClass="bodyText"
bodyLink="bodyLink"

h_header = html.Header(
    style={
        'backgroundColor': colors['background'],
    },
    children=[
        html.H1(className="app-header", children="COVID Risiko Deutschland nach Landkreisen", style={'color': colors['text'], 'text-decoration': 'none'}, id="title_header"),
        html.H4(className="app-header-date",
                children="Datenstand: {} 00:00 Uhr (wird täglich aktualisiert)".format(dataVersionDate),
                style={'color': colors['text']}),
        html.H4(className="app-header-date",
                children="Softwarestand: {} (UTC), Version 0.9.8".format(appDateStr),
                style={'color': colors['text']}),
        html.H3(html.A(html.Span("Zur Tabelle springen ⬇", className=introClass), href="#tabletop")),
    ]
)
# h_explanation = dcc.Markdown(
#     readExplanation(),
#     style={
#         'backgroundColor': colors['background'],
#         'color': 'white',
#         'line-height:': '1.8',
#     },
#     dangerously_allow_html=True,
# )



h_Hinweis=html.P([
    html.Span("Hinweis:", className=introClass),
    html.Span(" Dies ist eine privat betriebene Seite im Testbetrieb, für die Richtigkeit der Berechnung und der Ergebnisse "
              "gibt es keine Gewähr. Im Zweifel mit den ", className=bodyClass),
    html.A("offiziellen Zahlen des RKI abgleichen.",
           href="https://experience.arcgis.com/experience/478220a4c454480e823b17327b2bf1d4/page/page_1/",
           className=bodyLink
           ),
    html.P("Das System ist relativ neu, kann unentdeckte, subtile Fehler machen, und auch die Berechnung des Rangs kann sich durch Updates der Software derzeit noch verändern.", className=bodyClass),
    html.Span(" Generell gilt: Die Zahlen sind mit Vorsicht zu genießen. Auf Landkreisebene sind die Zahlen niedrig, eine Handvoll neue Fälle kann viel ausmachen. Auch können hohe Zahlen Folge eines"
              " Ausbruch in einer Klinik, einem Heim oder einer Massenunterkunft sein und nicht repräsentativ für die"
              " Verteilung in der breiten Bevölkerung. Andererseits ist da noch die Dunkelziffer, die hier mit 6,25"
              " angenommen wird. (siehe Risiko 1/N) Es laufen also viel mehr meist symptomlose Infizierte umher als Fälle registriert"
              " sind. Und fast immer gilt: Steigen die Zahlen, ist es nicht unter Kontrolle.", className=bodyClass),
    html.P(
        " Und ja, Ranglisten sind immer fragwürdige Vereinfachungen, aber nachdem die Zahlen kleiner werden, ist der Blick in die Regionen umso"
        " aufschlussreicher und hilft vielleicht, die Aufmerksamkeit auf die Orte zu richten, wo die Situation besonders"
        " schlimm und gefährlich ist. Es lohnt sich aber auch, bis ganz nach unten zu scrollen und auch beruhigend viel grün"
        " zu sehen.", className=bodyClass),
])

h_Erlauterung=html.P([
    html.Span("Erläuterung:", className=introClass),
    html.Span(" Diese Seite bereitet ", className=bodyClass),
    html.A("die RKI COVID19 Daten aus dem NPGEO-Corona-Hub",
              href="https://npgeo-corona-npgeo-de.hub.arcgis.com/datasets/dd4580c810204019a7b8eb3e0b329dd6_0",
              className=bodyLink),
    html.Span(" in tabellarischer Form auf und berechnet u.a. Trends sowie einen Ansteckungsrisikowert für jeden"
            " Landkreis. Anfänglich sind die Landkreise in einer Rangliste von gefährlich bis idyllisch sortiert."
            " Die Daten können aber nach jeder Spalte sortiert und gefiltert werden, siehe 'Benutzung'."
            " Anhand der Ampelfarbgebung läßt sich durch Scrollen schnell ein Überblick über viele"
            " Landkreise und die Unterschiede zwischen ihnen verschaffen.",
            className=bodyClass),
])

h_News=html.P([
    html.Span("News:", className=introClass),
    html.P(" Version 0.9.8: Veränderung der Platzierung gegenüber gestern wird angezeigt"
           "", className=bodyClass),
    html.P(" Version 0.9.7: Sie Spaltenheader bleiben stehen beim scrollen."
              "", className=bodyClass),
    html.P(" Version 0.9.6: Bei den Berechnungen für einzelne Landkreise gab es in der Version eine bedeutende Änderung. Die Definition"
           " wurde geändert, was die Zählung der Fälle pro Woche betrifft. Als Fall in den letzten 7 Tagen gilt nun,"
           " wenn der Fall in den letzten 7 Tagen beim RKI eingegangen ist und in den letzten 14 Tagen beim Gesundheitsamt"
           " gemeldet wurde. Hintergrund ist, dass es bei einzelnen Landkreisen immer wieder zu Nachmeldungen von zig Fällen"
           " ans RKI kommt, die teilweise Wochen oder Monate zurückliegen. In Einzelfällen führte das zu einem zu hohen Risikoranking"
           " und einem verfälschten Bild der Entwicklung."
           "", className=bodyClass),
    html.P(" Während das RKI nur als Fälle der letzten 7 Tage diejenigen ausweist, die sich auch in den letzten 7 Tagen"
            " beim beim Gesundheitsamt gemeldet haben, fallen in der RKI-Landkreis-7-Tage-Rechnung alle Fälle unter den Tisch"
            " die nicht auch in den letzten 7 Tagen eingegangen sind. Dadurch sind die RKI-Zahlen für die letzten 7 Tage"
            " meist zu niedrig, je nach Meldeverzögerung, während sie hier weniger zu niedrig oder zu hoch sind, weil hier"
            " verspätet eingegange Fälle der vorletzten 7 Tage den letzten Tagen zugeschlagen werden, wenn sie erst in den"
            " letzten 7 Tagen eingangen sind. Es wäre interessant zu wissen, warum Wochen zurückliegende Fälle in nennenswerter Zahl, manchmal"
            " in der Grössenordung von zig bis über hundert Fällen in einem Landkreis alle an einem Tag nachgemeldet werden."
            "", className=bodyClass),
])

h_About=html.P([
    html.Span("Der Autor über sich:", className=introClass),
    html.Span(" Bin weder Webentwickler noch Virologe noch Statistiker, aber habe mich in den letzten Wochen sehr"
              " intensiv mit vielen Aspekten rund um den neuen Corona-Virus auseinander gesetzt, fast täglich"
              , className=bodyClass),
    html.A(" auf Twitter",
              href="https://twitter.com/pavel23",
              className=bodyLink),
    html.Span(" und rede einmal in der Woche mit Tim Pritlove", className=bodyClass),
    html.A("  im Corona Weekly Podcast",
           href="http://ukw.fm/category/corona-krise/corona-weekly/",
           className=bodyLink),
    html.Span(" über Zahlen und Apps und News über das Virus. Wir versuchen, das Geschehen zu verstehen, einzuordnen,"
              " zu erklären und zu bewerten. Diese Webseite ist ein Teil meiner Bemühungen, zu verstehen,"
              " was hier gerade passiert und andere daran teilhaben zu lassen."
              " Vielleicht trägt sie ja dazu bei, dass der Eine beruhigter oder der Andere vorsichtiger in den Tag geht."
              " Und es würde mich freuen, wenn ich bessere Daten hätte, liebes Robert-Koch Institut."
              , className=bodyClass),

])

h_Benutzung = html.P([
    html.P(html.H4(html.A(html.Span("An den Seitenanfang springen ⬆", className=introClass), href="#top",id="Benutzung"))),
    html.A(html.Span("Benutzung:", className=introClass)),
    html.P(
        "Durch Klicken auf die kleinen Pfeilsymbole im Kopf einer Spalte kann die Tabelle auf- oder absteigend sortiert werden.",
        className=bodyClass),

    html.P("Interessant ist es, nach Gesamtanzahl von Fällen  zu sortieren und dann zu sehen,"
           "wo sich ehemals stark betroffene Gebiete so auf die Liste verteilen.", className=bodyClass),

    html.P("Im leeren Feld über der Spalte kann ein Filter eingeben werden, z.B. Teil des Namens des Kreises. "
           "Mit Audrücken wie <10 oder >50 können Datensätzte ausgefiltert werden, die bestimmte Werte in der Spalte "
           "über- oder unterschreiten. Einfach Wert eingeben und <Return> drücken. Eingabe löschen, um Filter zu entfernen."
           , className=bodyClass),
    html.H4(html.A(html.Span("Ans Seitenende springen ⬇", className=introClass), href="#bottom")),
    html.P(html.H4(html.A(html.Span("Zu Absatz 'Benutzung' springen ⬆", className=introClass), href="#Benutzung")),
           style={'padding': '0px',
                  'backgroundColor': colors['background']}, id="tabletop"),
])

h_BedeutungSpaltenHead= html.Span("Bedeutung der Spalten:", className=introClass)

h_BedeutungSpaltenIntro=html.Span("Am aussagekräftigsten sind Werte je 100.000 Einwohner, und da sind derzeit praktisch"
" nur die letzten beiden Wochen von Interesse. Wie es sich davor zugetragen hat, lassen aber die Gesamtzahlen erahnen."
' Wo es null Infektion gab, erlaubt ein Blick auf den Zeitpunkt der letzten Meldung, wie lange der Kreis "COVID-frei" ist.'
" Hier die Bedeutung der Spalten, die nicht offensichtlich ihrem Titel zu entnehmen ist:",
                   className=bodyClass)

h_BedeutungSpalten = html.P([h_BedeutungSpaltenHead,h_BedeutungSpaltenIntro])

def makeDefinition(value, definition):
    return html.Div([
        html.Span(children=value, className=introClass),
        html.Span(children=definition, className=bodyClass),
    ])

h_Rw = html.Span(["R", html.Sub("w")])
h_RwK = html.Span(["R", html.Sub("w"),"K"])

h_RwDef = makeDefinition(h_RwK,
'''
 ist ein wöchentlicher Reproduktionsfaktor. Er ist das Verhältnis aller Fälle der letzten 7 Tage gegenüber den 7
 Tagen davor. Diese Zahl "schlägt" stärker aus als der "normale" Reproduktionsfaktor, aber über 1.0 heißt
 auch hier Ausbreitung und unter 1.0 Rückgang. Ein Wert von 2 bedeutet, dass in der letzten Woche ungefähr doppelt so viele
 so viele Fälle gemeldet wurden wie in der vorletzten Woche, ein Wert von 0,5 bedeutet ungefähr nur halb so viele neue Fälle.
 Warum ungefähr? Der Rw, der zuvor verwendet wurde, kann nicht gut mit 0 Fällen in einer Woche umgehen. Steigen die
 Infizerten von 0 auf 1, ist der Faktor unendlich. Es gibt keine perfekte Lösung für das Problem, aber wenn Physiker
 es mit Divisionen durch Null zu tun haben, addieren sie überall was auf, und das macht der RwK auch:
 RwK = ((Fälle in letzten 7 Tage)+5) / ((Fälle in 7 Tagen davor)+5) Warum 5? Sie liefert brauchbare Ergebnisse bei diesem
 Indektionsgeschen. Man kann sich auch vorstellen, dass bei Dunkelzifferquote 6,25 es für jeden bekannten Infizierten
 5 Unentdeckte gibt, von denen jetzt einer gefunden wurde, Werden die Zahlen grösser, fällt die zusätzliche 5 immer weniger ins Gewicht, RwK nähert sich dem Rw.
 Dieser wöchentlicher Reproduktionsfaktor vermeidet auch wochentagsbedingte Schwankungen und kommt ohne Annahmen wie
 Dauer des seriellen Intervalls aus und ist leicht nachvollziehbar. Er modelliert aber nicht den Reproduktionswert eines
 konkreten Virus, sondern ist einfach nur ein Verhältnis, aber bei den kleinen Zahlen kann man schlichtweg keinen sinnvollen
 R-Wert mit seriellem Intervall berechnen, und der RwK liefert keine allzu absurden oder unbrauchbaren Ergebnisse.           
''')

h_Risiko=makeDefinition("Rang, Risiko 1/N",
"""
Je kleiner diese Zahlen sind, umso grösser die Infektionsgefahr. Die Zahl N kann so interpretiert werden, dass jeweils
eine von N Personen ansteckend sein kann. Ist N etwa 100, kann sich im Durchschnitt in jedem Bus oder Waggon ein 
Ansteckender befinden. Rang 1 geht an den Landkreis mit der kleinsten Zahl N, also dem höchsten Risiko. Bei Gleichstand,
der praktisch nur am Tabellenende vorkommt, gewinnt der, wo der letzte Fall länger zurückliegt, bei Gleichstand auch da
gewinnt der, wo bisher pro 100.000 die wenigsten Fälle gemeldet wurden.  
N berechnet sich wie folgt:  
""")

h_LetzteMeldung=makeDefinition("Letzte Meldung",
"""
 gibt an, vor wie viel Tagen die letzte Meldung erfolgt. 0 heisst, dass der Kreis am (Vor-)Tag des Datenstands Fälle 
gemeldet hat, -5 bedeutet, dass seit 5 Tagen keine Meldungen eingegangen sind.
""")


h_RisikoList=html.Ul([
    html.Li(["N = Bevölkerung / Dunkelzifferfaktor / ([Anzahl der Fälle in den letzten 2 Wochen] *",h_RwK,")"]),
    html.Li("Als Faktor für die Dunkelziffer wurde 6,25 gewählt, also auf 1 gemeldeten Infizierten werden 5,25 weitere vermutet"),
    html.Li("Als grobe Annäherung an die Zahl der Ansteckenden wurde die Summe der Fälle der letzten zwei Wochen gewählt"),
    html.Li("Die Zahl der aktuell Ansteckenden wird zudem für die Risikoberechnung hochgerechnet,"
            "indem die Entwicklung von der vorletzten Woche zur letzten Woche prozentual unverändert fortgeschrieben wird "
            "und damit eher dem Stand am heutigen Tag entspricht."),
    html.Li("Die Dunkelziffer kann bis 2-fach höher sein, die Zahl der Ansteckenden aber nur halb so hoch,"
            "so dass der Risikowert als nicht allzu übertriebene Obergrenze für den Anteil der Ansteckenden zu sehen ist. Your mileage may vary."),
])

h_BedeutungFarbenHead = html.Span(children="Bedeutung der Farben:", className=introClass)
h_BedeutungFarbenIntro = html.Span(
    "Was die Farben bedeuten und wie die Schwellwerte gesetzt sind, ist eher subjektiv "
    "und kann sich ändern. Hier die beabsichtigte Bedeutung der Farben:",
    className=bodyClass)

h_BedeutungFarben=html.P([h_BedeutungFarbenHead, h_BedeutungFarbenIntro])

def makeColorSpan(text, className):
    return html.Span(text, className=className)

LiStyle = {#'padding': '10px',
           'margin-bottom': '0.5em',
         #'height': 'auto',
         }
h_TextFarbenList=html.Ul([
    html.Li([makeColorSpan("Rot: ",conditionTooHigh), "Zahl ist zu hoch und es müssen dringend Massnahmen getroffen werden, um sie zu senken"], style=LiStyle),
    html.Li([makeColorSpan("Gelb: ",conditionSerious), "Zahl ist zu hoch, um zur Tagesordnung zurückzukehren"], style=LiStyle),
    html.Li([makeColorSpan("Grün: ",conditionGood), "Zahl ist in Ordnung"], style=LiStyle),
])

h_TextFarben=html.Div(["Im Text", h_TextFarbenList])

h_BgFarbenList = html.Ul(
    [
        html.Li([makeColorSpan("Rot: ", conditionDanger),
                 "Lasset alle Hoffnung fahren. Die Situation ist praktisch ausser Kontrolle."
                 "Wer kürzlich da war und ungeschützte Kontakte hatte, ist mit einer Wahrscheinlichkeit von 1/N infiziert."
                 "Empfehlung: Möglichst zu Hause bleiben und außer Haus bestmögliche Schutzmaßnahmen ergreifen. Gegend weiträumig meiden."
                ],
                style=LiStyle
        ),
        html.Li([makeColorSpan("Grün: ",conditionSafe),
                 "Einheimische da könnten völlig entspannt sein, wenn sie keinen "
                 "ungeschützten Kontakt mit Fremden aus anderen Kreisen hätten. Empfehlung: Da bleiben und niemanden rein lassen."
                ],
                style=LiStyle
        ),
    ],
    style = {#'padding': '10px',
             #'line-height': '2.5em',
             'height': 'auto',
             }
)

h_BgtFarben=html.Div(["Feld mit Farbe hinterlegt", h_BgFarbenList])

# h_BedeutungSpaltenDef = html.Ul([
#     html.Li(h_RwDef),
#     html.Li([h_Risiko, h_RisikoList]),
#     ],
#     style={"padding-left": "16px", "padding-right": "64px"},
# )

# h_BedeutungFarbenDef = html.Ul([
#     html.Li(h_TextFarben),
#     html.Li([h_BgtFarben]),
#     ],
#     style={"padding-left": "16px"},
#
# )

cellStyle = {"border-left": "1px solid #ffffff", "padding-left": "16px"}
cellStyleFarben = {"border-left": "1px solid #ffffff", "padding-left": "16px", 'min-width':'30%','width':'30%'}

h_Bedeutungen = html.Table([
        html.Tr([html.Th(h_BedeutungSpaltenHead), html.Td(h_BedeutungFarbenHead,style=cellStyleFarben)]),
        html.Tr([html.Td(h_BedeutungSpaltenIntro), html.Td(h_BedeutungFarbenIntro,style=cellStyle)]),
        html.Tr([html.Td(h_RwDef), html.Td(h_TextFarben,style=cellStyleFarben)]),
        html.Tr([html.Td([h_Risiko, h_RisikoList]), html.Td(h_BgtFarben,style=cellStyleFarben)]),
        html.Tr([html.Td(h_LetzteMeldung)]),
],
#    style={'padding': '0 0'}
)

# h_BedeutungenVert = html.Div([
#     h_BedeutungSpalten,
#     html.Ul([html.Li(h_RwDef),
#              html.Li([h_Risiko, h_RisikoList]),
#              ]),
#     h_BedeutungFarben,
#     html.Ul([html.Li(h_TextFarben),
#              html.Li([h_BgtFarben]),
#              ])
# ])

betterExplanation = html.Div([
    h_Erlauterung,
    h_News,
    h_About,
    h_Hinweis,
    h_Bedeutungen,
    h_Benutzung
    ],
    style={'padding': '5px',
           'backgroundColor': colors['background'],
           'color': 'white',
           'text-align': 'left',
           'line-height':'1.5em',
           'height': 'auto',
    }
)

app.title = "COVID Risiko Deutschland nach Landkreisen"


app.layout = html.Div([
    h_header,
    betterExplanation,
    h_table,

    #html.P(html.H3(
    #html.A(html.Span("An den Seitenanfang springen ⬆", className=introClass), href="#top", id="Benutzung"))),

    #html.P(html.H3(
    html.P(html.H4(html.A(html.Span("Zu Absatz 'Benutzung' springen ⬆", className=introClass), href="#Benutzung")),style={'padding': '0px',
           'backgroundColor': colors['background']}, id="bottom"),

])

if __name__ == '__main__':
    debugFlag = socket.gethostname() == 'pavlator.local'
    app.run_server(host='0.0.0.0', port=1024,debug=debugFlag)
