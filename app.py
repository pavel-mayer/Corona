#!/usr/bin/env python3

# Quick hack to browse RKI-NPGEO-Data, Pavel Mayer 2020,
# License: Use freely at your own risk.

# pip install Click==7.0 Flask==1.1.1 itsdangerous==1.1.0 Jinja2==2.10.3 MarkupSafe==1.1.1 uWSGI==2.0.18 Werkzeug==0.16.0 dash=1.11.0
# pip install Click Flask itsdangerous Jinja2 MarkupSafe uWSGI Werkzeug matplotlib dash pandas datatable feather-format dash=1.11.0
# pip install: matplotlib dash pandas datatable feather-format psutil

import locale

print("Locale:"+str(locale.getlocale()))
locale.setlocale(locale.LC_ALL, 'de_DE')
print("Locale set to:"+str(locale.getlocale()))

import cov_dates as cd
import pm_util as pmu

import os
import flask
from flask import render_template
from flask import Response

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
import time

versionStr="1.0.0.2"

# = socket.gethostname().startswith('pavlator')
debugFlag = False
print("Running on host '{}', debug={}".format(socket.gethostname(), debugFlag))

def pretty(jsonmap):
    print(json.dumps(jsonmap, sort_keys=False, indent=4, separators=(',', ': ')))

# creates a new table by joining all columns from smaller table according to same values of keyFieldName
# largerTable must contain all keys present in smallertable but not vice versa
def join(largerTable, smallerTable, keyFieldName, overwriteSame=False):
    sKeys = smallerTable[:, keyFieldName].to_list()[0]
    extTable = largerTable.copy()
    for colName in smallerTable.names:
        if colName != keyFieldName and (not colName in largerTable.names or overwriteSame):
            values = smallerTable[:, colName].to_list()[0]
            valuesDict = dict(zip(sKeys, values))

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
    return fullTable
    #
    # lastDay=fullTable[:,'MeldeDay'].max()[0,0]
    # cases = fullTable[:, 'AnzahlFall'].sum()[0, 0]
    # dead = fullTable[:, 'AnzahlTodesfall'].sum()[0, 0]
    # lastDay=fullTable[:,'MeldeDay'].max()[0,0]
    # lastnewCaseOnDay=fullTable[:,'newCaseOnDay'].max()[0,0]
    # print("File stats: lastDay {} lastnewCaseOnDay {} cases {} dead {}".format(lastDay, lastnewCaseOnDay, cases, dead))
    # newTable=fullTable[:,dt.f[:].extend({"erkMeldeDelay": dt.f.MeldeDay-dt.f.RefDay})]
    # newTable=newTable[:,dt.f[:].extend({"MeldeDelay": dt.f.newCaseOnDay - dt.f.MeldeDay})]
    # return newTable, lastDay

def getRankedTable(fullTable, day, sortColumns):
    result = fullTable[dt.f.DatenstandTag == day,:].sort(sortColumns)
    result=result[:,dt.f[:].extend({"Rang": 0})]
    result[:,"Rang"]=np.arange(1,result.nrows+1)
    return result

def clip(table, colName, maxValue):
    table[dt.f[colName] > maxValue, colName] = maxValue
    return table

def getTableForDay(fullTable, day):
    #sortColumns = ["Risk","LetzteMeldung","InzidenzFallNeu_7_Tage"]
    sortColumns = ["Kontaktrisiko","InzidenzFallNeu_7TageSumme"]
    todayTable = getRankedTable(fullTable, day, sortColumns)
    yesterdayTable = getRankedTable(fullTable, day-1, sortColumns)
    #print(todayTable)
    #print(yesterdayTable)
    todayTableById = todayTable.sort("IdLandkreis")
    yesterdayTableById = yesterdayTable.sort("IdLandkreis")
    #print(todayTableById.nrows)
    #print(yesterdayTableById.nrows)

    # check if all entries today and yesterday do match
    for i in range(yesterdayTableById.nrows):
        l_id = todayTableById[i, dt.f.IdLandkreis].to_list()[0][0]
        l_name = todayTableById[i, dt.f.Landkreis].to_list()[0][0]
        l_new_id = yesterdayTableById[i, dt.f.IdLandkreis].to_list()[0][0]
        l_new_name = yesterdayTableById[i, dt.f.Landkreis].to_list()[0][0]
        #print("{}: ? {} {} != {} {}".format(i, l_id, l_name, l_new_id, l_new_name))
        if l_id != l_new_id:
            print("missing id {} ({}) in today, was {} ({}) yesterday".format(l_id, l_name, l_new_id, l_new_name))
            #print("{}: BAD: {} {} != {} {}".format(i, l_id, l_name, l_new_id, l_new_name))
            exit(1)
        #else:
        #    print("{}: ok : {} {} == {} {}".format(i, l_id, l_name, l_new_id, l_new_name))

    todayTableById=todayTableById[:,dt.f[:].extend({"RangChange": 0})]
    rangChange = np.subtract(yesterdayTableById[:,"Rang"],todayTableById[:,"Rang"])
    todayTableById[:,"RangChange"] = rangChange

    todayTableById=todayTableById[:,dt.f[:].extend({"RangYesterday": 0})]
    todayTableById[:,"RangYesterday"] = yesterdayTableById[:,"Rang"]

    todayTableById=todayTableById[:,dt.f[:].extend({"RangChangeStr": "-"})]

    rangChangeStrs = np.full(len(rangChange), "*")
    for i, rc in enumerate(rangChange):
        #print(i, rc)

        rangChangeStr = ""
        if rc > 0:
            rangChangeStr = "▲"
        if rc < 0:
            rangChangeStr = "▼"

        todayTableById[i, "RangChangeStr"] = rangChangeStr
        rangChangeStrs[i] = rangChangeStr

    #print(rangChangeStrs)

    #print("Column names frame order:", list(enumerate(resultTable.names)))
    todayTableById = todayTableById.sort("Rang")

    todayTableById[dt.f.Kontaktrisiko * 2 == dt.f.Kontaktrisiko, "Kontaktrisiko"] = 99999
    clip(todayTableById, "InzidenzFallNeu_Tage_bis_50", 9999.9)
    clip(todayTableById, "InzidenzFallNeu_Tage_bis_100", 9999.9)
    clip(todayTableById, "InzidenzFallNeu_Prognose_4_Wochen", 9999.9)
    clip(todayTableById, "InzidenzFallNeu_Prognose_8_Wochen", 9999.9)

    return todayTableById


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
        ('Landkreis', ['Region', 'Name'], 'text', Format(), colWidth(298)),
        ('Bundesland', ['Region', 'Land'], 'text', Format(), colWidth(190)),
        ('LandkreisTyp', ['Region', 'Art'], 'text', Format(), colWidth(30)),
        ('Einwohner', ['Region', 'Einwohner'], 'numeric', FormatInt, colWidth(90)),
        ('InzidenzFallNeu_7TageSumme_Trend_Spezial', ['Publizierte Fälle', 'RwK'], 'numeric', FormatFixed2, colWidth(70)),
        ('InzidenzFallNeu_7TageSumme_R', ['Publizierte Fälle', 'R7'], 'numeric', FormatFixed2, colWidth(70)),
        ('AnzahlFallNeu_7TageSumme', ['Publizierte Fälle', 'letzte 7 Tage'], 'numeric', FormatInt, colWidth(defaultColWidth)),
        ('AnzahlFallNeu_7TageSumme_7_Tage_davor', ['Publizierte Fälle', 'vorl. 7 Tage'], 'numeric', FormatInt, colWidth(defaultColWidth)),
        ('AnzahlFall', ['Publizierte Fälle', 'total'], 'numeric', FormatInt, colWidth(90)),
        ('AnzahlFallNeu', ['Publizierte Fälle', 'neu'], 'numeric', FormatInt, colWidth(defaultColWidth)),

        ('InzidenzFallNeu_7TageSumme', ['Publizierte Fälle je 100.000', 'letzte 7 Tage'], 'numeric', FormatFixed1, colWidth(defaultColWidth)),
        ('InzidenzFallNeu_7TageSumme_7_Tage_davor', ['Publizierte Fälle je 100.000', 'vorl. 7 Tage'], 'numeric', FormatFixed1, colWidth(defaultColWidth)),
        ('InzidenzFall', ['Publizierte Fälle je 100.000', 'total'], 'numeric', FormatFixed1, colWidth(60)),
        ('InzidenzFallNeu_Prognose_4_Wochen', ['Publizierte Fälle je 100.000', 'in 4 Wochen'], 'numeric', FormatFixed1, colWidth(60)),
        ('InzidenzFallNeu_Prognose_8_Wochen', ['Publizierte Fälle je 100.000', 'in 8 Wochen'], 'numeric', FormatFixed1, colWidth(60)),
        ('InzidenzFallNeu_Tage_bis_50', ['Publizierte Fälle je 100.000', 'Tage bis 50'], 'numeric', FormatInt, colWidth(60)),
        ('InzidenzFallNeu_Tage_bis_100', ['Publizierte Fälle je 100.000', 'Tage bis 100'], 'numeric', FormatInt, colWidth(60)),

        ('MeldeTag_AnzahlFallNeu_Gestern_7TageSumme', ['Fälle nach Meldedatum (RKI-Zählung)', 'absolut'], 'numeric', FormatInt, colWidth(defaultColWidth)),
        ('MeldeTag_InzidenzFallNeu_Gestern_7TageSumme', ['Fälle nach Meldedatum (RKI-Zählung)', '7 Tage Inzidenz'], 'numeric', FormatFixed1, colWidth(defaultColWidth)),
        ('AnzahlFallNeu_7TageSumme_Dropped', ['Fälle nach Meldedatum (RKI-Zählung)', 'Diff. zu publ. Fällen'], 'numeric', FormatInt, colWidth(defaultColWidth)),
        ('ProzentFallNeu_7TageSumme_Dropped', ['Fälle nach Meldedatum (RKI-Zählung)', '% zu publ. Fällen'], 'numeric', FormatFixed1, colWidth(defaultColWidth)),

        ('DatenstandTag_Diff', ['Meldung', 'Letzte Zählung'], 'numeric', FormatInt, colWidth(70)),
        ('PublikationsdauerFallNeu_Min_Neg', ['Meldung', 'Letzte Meldung'], 'numeric', FormatInt, colWidth(70)),
        ('PublikationsdauerFallNeu_Max', ['Meldeverzögerung (Tage)', 'Max.'], 'numeric', FormatFixed1, colWidth(62)),
        ('PublikationsdauerFallNeu_Schnitt', ['Meldeverzögerung (Tage)', 'Mittel x̅'], 'numeric', FormatFixed1, colWidth(62)),
        ('PublikationsdauerFallNeu_Median', ['Meldeverzögerung (Tage)', 'Median x̃'], 'numeric', FormatInt, colWidth(62)),
        ('PublikationsdauerFallNeu_StdAbw', ['Meldeverzögerung (Tage)', 'Stdabw. σx'], 'numeric', FormatFixed1, colWidth(62)),
        ('PublikationsdauerFallNeu_Fallbasis', ['Meldeverzögerung (Tage)', 'Anzahl Fälle'], 'numeric', FormatInt, colWidth(62)),
        ('AnzahlTodesfallNeu_7TageSumme', ['Todesfälle', 'letzte 7 Tage'], 'numeric', FormatInt, colWidth(defaultColWidth)),
        ('AnzahlTodesfallNeu_7TageSumme_7_Tage_davor', ['Todesfälle', 'vorl. 7 Tage'], 'numeric', FormatInt, colWidth(defaultColWidth)),
        ('AnzahlTodesfall', ['Todesfälle', 'total'], 'numeric', FormatInt, colWidth(defaultColWidth)),
        ('Fallsterblichkeit_Prozent', ['Todesfälle', 'CFR in %'], 'numeric', FormatFixed2, colWidth(62)),
        ('InzidenzTodesfallNeu_7TageSumme', ['Todesfälle je 100.000', 'letzte 7 Tage'], 'numeric', FormatFixed2, colWidth(defaultColWidth)),
        ('InzidenzTodesfallNeu_7TageSumme_7_Tage_davor', ['Todesfälle je 100.000', 'vorl. 7 Tage'], 'numeric', FormatFixed2, colWidth(defaultColWidth)),
        ('InzidenzTodesfallNeu_7TageSumme_Trend_Spezial', ['Todesfälle je 100.000', 'Trend'], 'numeric', FormatFixed2, colWidth(defaultColWidth)),
        ('InzidenzTodesfall', ['Todesfälle je 100.000', 'total'], 'numeric', FormatFixed2, colWidth(62)),
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

# fullTableFilename = "full-latest.csv"
# cacheFilename = "data-cached.feather"
dataFilename = "data.csv"
#
# FORCE_REFRESH_CACHE = debugFlag
# #FORCE_REFRESH_CACHE = True
#
# if FORCE_REFRESH_CACHE or not os.path.isfile(cacheFilename) or os.path.getmtime(fullTableFilename) > os.path.getmtime(cacheFilename) :
#     dframe = loadAndProcessData(fullTableFilename)
#     dframe.to_feather(cacheFilename)
#     dframe.to_csv(dataFilename)
# else:
#     print("Loading data cache from ‘"+cacheFilename+"‘")
#     dframe = pd.read_feather(cacheFilename)
#

csvData = open(dataFilename,"rb").read().decode('utf-8')

fullTableFilename="all-series.csv"
csvFullData = open(fullTableFilename,"rb").read().decode('utf-8')

dataURL = '/covid/risks/data.csv'

def csvResponse(data):
    r = Response(response=data, status=200, mimetype="text/csv")
    r.headers["Content-Type"] = "text/csv; charset=utf-8"
    return r

@server.route(dataURL)
def csv_data():
     return csvResponse(csvData)

fullDataURL = '/covid/risks/all-series.csv'
@server.route(fullDataURL)
def csv_fulldata():
    return csvResponse(csvFullData)

###########################################################################################
# Start of table initialization
columns, colWidths, totalWidth, minWidth, maxWidth = makeColumns()
totalWidthStr = colWidthStr(totalWidth)
minWidthStr = colWidthStr(minWidth)
maxWidthStr = colWidthStr(maxWidth)

# load table
fullTable = loadData(fullTableFilename)

#maxDay = float(dframe["MeldeDay"].max())
maxDay = fullTable[:,"DatenstandTag"].max().to_list()[0][0]
print(maxDay)
dataVersionDate = cd.dateStrWDMYFromDay(maxDay)
print("Loading done, max Day {} date {}".format(maxDay, dataVersionDate))

print("Creating Datatable")

table = getTableForDay(fullTable, maxDay)
table.to_csv("data.csv")
data = table.to_pandas().to_dict("records")

# print("colWidths", colWidths)
# print("totalWidthStr",totalWidthStr)
# print("minWidthStr",minWidthStr)
# print("maxWidthStr",maxWidthStr)

colors = {
    'background': 'rgb(50, 50, 50)',
    'text': 'white'
}

conditionUltra="conditionUltra"
conditionDanger="conditionDanger"
conditionTooHigh="conditionTooHigh"
conditionSerious="conditionSerious"
conditionGood="conditionGood"
conditionSafe="conditionSafe"

conditions = [conditionUltra, conditionDanger, conditionTooHigh, conditionSerious, conditionGood, conditionSafe]

conditionStyleDict = {
    conditionUltra : {'backgroundColor': 'blueviolet', 'color': 'white'},
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
    conditionUltra : '{Kontaktrisiko} > 0 && {Kontaktrisiko} < 50',
    conditionDanger : '{Kontaktrisiko} >= 50 && {Kontaktrisiko} < 100',
    conditionTooHigh: '{Kontaktrisiko} >= 100 && {Kontaktrisiko} < 1000',
    conditionSerious: '{Kontaktrisiko} >= 1000 && {Kontaktrisiko} < 2500',
    conditionGood: '{Kontaktrisiko} >= 2500 && {Kontaktrisiko} < 10000',
    conditionSafe: '{Kontaktrisiko} >= 10000'
}

def makeExpression(name, low=None, high=None):
    nameVar = "{"+name+"}"
    if low is None:
        return "{} >= {}".format(nameVar, high)
    if high is None:
        return "{} < {}".format(nameVar, low)
    return "{} >= {} && {} < {}".format(nameVar, low, nameVar, high)


def makeConditionClass(name, ultra, danger, tooHigh, serious, good):
    c=  {
        conditionUltra : makeExpression(name, None, ultra),
        conditionDanger : makeExpression(name, danger, ultra),
        conditionTooHigh: makeExpression(name, tooHigh, danger),
        conditionSerious: makeExpression(name, serious, tooHigh),
        conditionGood: makeExpression(name, good, serious),
        conditionSafe: makeExpression(name, good),
    }
    #print(c)
    return c


FaellePro100kLetzte7TageClass = makeConditionClass("InzidenzFallNeu_7TageSumme",100,50,20,5,1)
FaellePro100kPrognoseClass = makeConditionClass("InzidenzFallNeu_Prognose_4_Wochen",100,50,20,5,1)
FaellePro100kPrognose2Class = makeConditionClass("InzidenzFallNeu_Prognose_8_Wochen",100,50,20,5,1)
InzidenzRKIClass = makeConditionClass("MeldeTag_InzidenzFallNeu_Gestern_7TageSumme",100,50,20,5,1)


AnzahlFallTrendClass = makeConditionClass("InzidenzFallNeu_7TageSumme_Trend_Spezial",3,2,1,0.9,0.3)

LandkreisClass = {
    conditionUltra : KontaktrisikoClass[conditionUltra] +" || "+ braced(FaellePro100kLetzte7TageClass[conditionUltra])+" || "+braced(AnzahlFallTrendClass[conditionUltra]),
    conditionDanger : "("+KontaktrisikoClass[conditionDanger]+" || "+ braced(FaellePro100kLetzte7TageClass[conditionDanger])+ ") && "+ Not(FaellePro100kLetzte7TageClass[conditionUltra])+" && "+ Not(AnzahlFallTrendClass[conditionUltra]),
    conditionTooHigh: KontaktrisikoClass[conditionTooHigh]+" && "+ Not(FaellePro100kLetzte7TageClass[conditionUltra])+" && "+ Not(AnzahlFallTrendClass[conditionUltra])+" && "+ Not(FaellePro100kLetzte7TageClass[conditionDanger])+" && "+ Not(AnzahlFallTrendClass[conditionDanger]),
    conditionSerious: KontaktrisikoClass[conditionSerious]+" && "+ Not(FaellePro100kLetzte7TageClass[conditionUltra])+" && "+ Not(AnzahlFallTrendClass[conditionUltra])+" && "+ Not(FaellePro100kLetzte7TageClass[conditionDanger])+" && "+ Not(AnzahlFallTrendClass[conditionDanger]),
    conditionGood: KontaktrisikoClass[conditionGood]+" && "+ Not(FaellePro100kLetzte7TageClass[conditionUltra])+" && "+ Not(AnzahlFallTrendClass[conditionUltra])+" && "+ Not(FaellePro100kLetzte7TageClass[conditionDanger])+" && "+ Not(AnzahlFallTrendClass[conditionDanger]),
    conditionSafe: KontaktrisikoClass[conditionSafe]+" && "+ Not(FaellePro100kLetzte7TageClass[conditionUltra])+" && "+ Not(AnzahlFallTrendClass[conditionUltra])+" && "+ Not(FaellePro100kLetzte7TageClass[conditionDanger])+" && "+ Not(AnzahlFallTrendClass[conditionDanger]),
}

#print(LandkreisClass)

#    conditionDanger : KontaktrisikoClass[conditionDanger]+" || "+ braced(FaellePro100kLetzte7TageClass[conditionDanger])+ "&& "+ Not(FaellePro100kLetzte7TageClass[conditionUltra])+" && "+ Not(AnzahlFallTrendClass[conditionUltra]),

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
    result = result + conditionalStyles(FaellePro100kLetzte7TageClass, 'InzidenzFallNeu_7TageSumme')
    result = result + conditionalStyles(AnzahlFallTrendClass, 'InzidenzFallNeu_7TageSumme_Trend_Spezial')
    result = result + conditionalStyles(KontaktrisikoClass, 'Kontaktrisiko')
    result = result + conditionalStyles(LandkreisClass, 'Landkreis')
    result = result + conditionalStyles(FaellePro100kPrognoseClass, 'InzidenzFallNeu_Prognose_4_Wochen')
    result = result + conditionalStyles(FaellePro100kPrognose2Class, 'InzidenzFallNeu_Prognose_8_Wochen')
    result = result + conditionalStyles(InzidenzRKIClass, 'MeldeTag_InzidenzFallNeu_Gestern_7TageSumme')

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
    #filter_query='{Bundesland} = Berlin',
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
#print("appDate",appDate)
appDateStr=cd.dateTimeStrFromTime(appDate)
print("App last modified: ",appDateStr)

introClass="intro"
bodyClass="bodyText"
bodyLink="bodyLink"

appTitle = "COVID Risiko Deutschland nach Ländern und Kreisen"

h_header = html.Header(
    style={
        'backgroundColor': colors['background'],
    },
    children=[
        html.H1(className="app-header", children=appTitle, style={'color': colors['text'], 'text-decoration': 'none'}, id="title_header"),
        html.H2(className="app-header", children="Die Zahlen in der Tabelle sind wieder benutzbar.", style={'color':'green', 'text-decoration': 'none'}, id="title_alarm"),
        html.H4(className="app-header-date",
                children="Datenstand: {} 00:00 Uhr (wird täglich aktualisiert)".format(dataVersionDate),
                style={'color': colors['text']}),
        html.H4(className="app-header-date",
                children="Softwarestand: {} (UTC), Version {}".format(appDateStr, versionStr),
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
    html.P("Je nach Browser und Endgerät kann der erste Aufbau der Seite bis zu 30 sec. dauern.", className=bodyClass),
    html.Span(" Generell gilt: Die Zahlen sind mit Vorsicht zu genießen. Auf Landkreisebene sind die Zahlen niedrig, eine Handvoll neue Fälle kann viel ausmachen. Auch können hohe Zahlen Folge eines"
              " Ausbruch in einer Klinik, einem Heim oder einer Massenunterkunft sein und nicht repräsentativ für die"
              " Verteilung in der breiten Bevölkerung. Andererseits ist da noch die Dunkelziffer, die hier mit 3,5"
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
            " Anhand der Ampelfarbgebung läßt sich durch Scrollen schnell ein Überblick über Bundesländer und "
            " Landkreise und die Unterschiede zwischen ihnen verschaffen.",
            className=bodyClass),
])

h_News=html.P([
    html.Span("News:", className=introClass),
    html.P(
        " Version 1.0.0.0: Die Seite sieht zwar noch fast genauso aus wie vorher, aber die Datenpipeline und alle Berechnungen "
        " sind komplett von Grundauf neu geschrieben. Es sollten jetzt die neuen täglichen Fälle und"
        " die Gesamtzahlen identisch mit den offiziell vom RKI veröffentlichten Zahlen sein. Die neue Datenpipeline ist ganz frisch und kann noch Fehler enthalten, wobei die Fallzahlen "
        " alle ziemlich gut aussehen."
        "", className=bodyClass),
    html.P(
        "Ansonsten gibt es noch als erstes kleines neues, unübersehbares Feature: Condition Ultra beziehungsweise violett. Sie markiert im Prinzip"
        " Inzidenzen über 100. Neu ist zudem eine Spalte CFR unter Todesfälle. DIe CFR oder Fallsterblichkeit ist, wie viele von den positiv"
        " getesteten ingesamt verstorben sind."
        "", className=bodyClass),
    html.P(
        "Des weiteren gibt es zwei wesentliche Unterschiede in der Berechnung der Zahlen:"
        "", className=bodyClass),
    html.P(
        "1) Der Riskofaktor ist jetzt geringer, weil nur noch die Fälle der letzten Woche und nicht der letzten 2 Wochen als Grundlage für"
        " die Zahl der Infizierten herangezogen wird."
        "", className=bodyClass),

    html.P(
        "2) Die Trendberechnung und 7-Tage-Inzidenz berechnen nun alle Fälle mit ein. Die alte Version hatte als 'Kompromiss' "
        " alle Fälle ignoriert, die länger als 14 Tage zur Meldung bebraucht haben. Bei höherer Zahl an Nachmeldungen kann"
        " man einfach die in den Spalten unter 'Fälle strikt 7 Tage' heranziehen."
        "", className=bodyClass),
    html.P(
        "3) Die Meldeverzögerung bezieht sich nur noch auf die am aktuellen Tag gemeldeten Fälle und kann sich von Tag zu Tag stark ändern."
        "", className=bodyClass),

    # html.P(
    #     " Version 0.9.18: Prognosespalten hinzugefügt für Inzidenzen in 4 und 8 Wochen und Tage bis Inzidenz 3 und 7"
    #     "", className=bodyClass),
    # html.P(
    #     " Version 0.9.17: Fehler in der Datenaufbereitung gefixt, der zwischen dem 16.12. und 25.12.2020 fehlerhafte Werte verursacht hat. Sorry dafür."
    #     "", className=bodyClass),
    # html.P(
    #     " Version 0.9.16: R7-Berechnnung von sqrt(RwK) auf RwK^(4/7) geändert."
    #     "", className=bodyClass),
    # html.P(
    #     " Version 0.9.15: Weitere Spalten hinzugefügt, um die Anzahl von Fällen anzuzeigen, die bei der offiziellen Berechnung der"
    #     " 7-Tage-Inzidenz durch das RKI unter den Tisch fallen, weil sie später als 7 Tage nach der Meldung beim Gesundheitsamt ans RKI gemeldet wurden."
    #     " Neu ist auch die Spalte R7, die in ihrer Bedeutung und Dimension in etwa dem berühmten 7-Tage-R-Wert entspricht. Die .csv-Datei enthält jetzt Felder"
    #     " zum Datenstand und zur Software-Version, mit der die Tabelle generiert wurde. Der Dunkelzifferfaktor wurde zudem von 6,25 auf 3,5 reduziert."
    #     "", className=bodyClass),
    # html.P(
    #     " Version 0.9.14: Weiteren Fehler bei der Berechnung der Faelle/100k bei Bundesländern gefixt. Dank an Andreas für den Hinweis"
    #     "", className=bodyClass),
    # html.P(
    #     " Version 0.9.13: Fehler bei der Berechnung der Faelle/100k bei Bundesländern gefixt. Dank an Marcus für den Hinweis."
    #     "", className=bodyClass),
    # html.P(
    #     " Version 0.9.12: Schnelleres Laden der Tabelle durch Dash-Update."
    #     "", className=bodyClass),
    # html.P(
    #     " Version 0.9.11: Mittelwert, Median und Standardabweichung der Meldeverzögerung hinzufügt, Datendownload als .csv emöglicht."
    #     "", className=bodyClass),
    # html.P(
    #     " Version 0.9.10: Fehlerhafte Berechnung der Todesfälle korregiert. Vielen Dank an @Stanny96 und alle Anderen, die mich auf Fehler hingewiesen haben."
    #     "", className=bodyClass),
    # html.P(" Version 0.9.9: Bundesländer und Deutschland tauchen jetzt als eigene Region in der Liste auf. So lassen sich"
    #        " sehr einfach Bundesländer untereinander oder ein Kreis mit dem Landesdurchschitt oder ein Land mit dem Bund vergleichen."
    #        "", className=bodyClass),
    # html.P(" Version 0.9.8: Veränderung der Platzierung gegenüber gestern wird angezeigt"
    #        "", className=bodyClass),
    # html.P(" Version 0.9.7: Sie Spaltenheader bleiben stehen beim scrollen."
    #           "", className=bodyClass),
    # html.P(" Version 0.9.6: Bei den Berechnungen für einzelne Landkreise gab es in der Version eine bedeutende Änderung. Die Definition"
    #        " wurde geändert, was die Zählung der Fälle pro Woche betrifft. Als Fall in den letzten 7 Tagen gilt nun,"
    #        " wenn der Fall in den letzten 7 Tagen beim RKI eingegangen ist und in den letzten 14 Tagen beim Gesundheitsamt"
    #        " gemeldet wurde. Hintergrund ist, dass es bei einzelnen Landkreisen immer wieder zu Nachmeldungen von zig Fällen"
    #        " ans RKI kommt, die teilweise Wochen oder Monate zurückliegen. In Einzelfällen führte das zu einem zu hohen Risikoranking"
    #        " und einem verfälschten Bild der Entwicklung."
    #        "", className=bodyClass),
    # html.P(" Während das RKI nur als Fälle der letzten 7 Tage diejenigen ausweist, die sich auch in den letzten 7 Tagen"
    #         " beim beim Gesundheitsamt gemeldet haben, fallen in der RKI-Landkreis-7-Tage-Rechnung alle Fälle unter den Tisch"
    #         " die nicht auch in den letzten 7 Tagen eingegangen sind. Dadurch sind die RKI-Zahlen für die letzten 7 Tage"
    #         " meist zu niedrig, je nach Meldeverzögerung, während sie hier weniger zu niedrig oder zu hoch sind, weil hier"
    #         " verspätet eingegange Fälle der vorletzten 7 Tage den letzten Tagen zugeschlagen werden, wenn sie erst in den"
    #         " letzten 7 Tagen eingangen sind. Es wäre interessant zu wissen, warum Wochen zurückliegende Fälle in nennenswerter Zahl, manchmal"
    #         " in der Grössenordung von zig bis über hundert Fällen in einem Landkreis alle an einem Tag nachgemeldet werden."
    #         " Es gibt seit Version 0.9.15 Spalten, die die vom RKI bei der Inzidenzberechnung ignoeriert werden."
    #         " Die lokalen Behörden weisen auf ihren Webseiten ebenfalls oft höhere Inzidenzen als das RKI aus."
    #         "", className=bodyClass),
])

h_About=html.P([
    html.Span("Der Autor über sich:", className=introClass),
    html.Span(" Bin weder Webentwickler noch Virologe noch Statistiker, aber habe mich in den letzten Monaten sehr"
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

h_Downloads = html.P([
    html.H4(html.Span("Downloads", className=introClass)),
    html.P([html.A(html.Span("Der Tabelle zugrundeliegende Daten als .csv herunterladen", className=bodyClass),
                  href=fullDataURL),
            " (Es sind viel mehr Spalten und Zeilen enthalten, als angezeigt werden)"]),
    html.P(html.A(html.Span("Tabelle als .csv herunterladen", className=bodyClass), href=dataURL)),
])

h_Benutzung = html.P([
    html.P(html.H4(html.A(html.Span("An den Seitenanfang springen ⬆", className=introClass), href="#top",id="Benutzung"))),
    html.A(html.Span("Benutzung:", className=introClass)),
    html.P(
        "Durch Klicken auf die kleinen Pfeilsymbole im Kopf einer Spalte kann die Tabelle auf- oder absteigend sortiert werden.",
        className=bodyClass),

    html.P("Interessant ist es, nach unterschiedlichen Kriterien zu sortieren und dann zu sehen,"
           "wie sich betroffene Gebiete anhand des Kriteriums so auf die Liste verteilen.", className=bodyClass),

    html.P("Im leeren Feld über der Spalte kann ein Filter eingeben werden, z.B. Teil des Namens des Kreises. "
           "Mit Audrücken wie <10 oder >=50 können Datensätzte ausgefiltert werden, die bestimmte Werte in der Spalte "
           "über- oder unterschreiten. Einfach Wert eingeben und <Return> drücken. Eingabe löschen, um Filter zu entfernen."
           , className=bodyClass),
    html.P("Um nur die Bundesländer uns Deutschland zu sehen, 'B' im Feld Region/Art eingeben, '=B', um nur die Bundesländer"
            " zu sehen, 'R', um nur Deutschland zu sehen. Weitere Werte sind 'LK' für "
           " für Landkreis, SK für Stadtkreis und LSK für Land/Stadtregionen. Um nur Sachsen zu sehen, '=Sachsen' bei Region/Land nehmen,"
           , className=bodyClass),
     html.H4(html.A(html.Span("Ans Seitenende springen ⬇", className=introClass), href="#bottom")),
    html.P(html.H4(html.A(html.Span("Zu Absatz 'Benutzung' springen ⬆", className=introClass), href="#Benutzung")),
           style={'padding': '0px',
                  'backgroundColor': colors['background']}, id="tabletop"),
])

h_BedeutungSpaltenHead= html.Span("Bedeutung der Spalten:", className=introClass)

h_BedeutungSpaltenIntro=html.Span("Am aussagekräftigsten sind Werte je 100.000 Einwohner, und da sind derzeit praktisch"
" nur die letzten beiden Wochen von Interesse. Wie es sich davor zugetragen hat, lassen aber die Gesamtzahlen erahnen."
' Wo es null Infektionen gab, erlaubt ein Blick auf den Zeitpunkt der letzten Meldung, wie lange der Kreis "COVID-frei" ist.'
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
 Indektionsgeschen. Werden die Zahlen grösser, fällt die zusätzliche 5 immer weniger ins Gewicht, RwK nähert sich dem Rw.
 Dieser wöchentlicher Reproduktionsfaktor vermeidet auch wochentagsbedingte Schwankungen und kommt ohne Annahmen wie
 Dauer des seriellen Intervalls aus und ist leicht nachvollziehbar. Er modelliert aber nicht den Reproduktionswert eines
 konkreten Virus, sondern ist einfach nur ein Verhältnis, aber bei den kleinen Zahlen kann man schlichtweg keinen sinnvollen
 R-Wert mit seriellem Intervall berechnen, und der RwK liefert keine allzu absurden oder unbrauchbaren Ergebnisse.           
''')

h_R7=makeDefinition("R7",
'''
 entspricht in Dimension und Bedeutung in etwa dem dem bekannten R-Wert mit einem seriellen Intervall von 4 Tagen,
 gemittelt über 7 Tage. Die hier angezeigte Annäherung berechnet sich einfach aus RwK hoch 4/7.
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

h_Prognose=makeDefinition("Fälle je 100.000, in X Wochen",
"""
Zeigt die 7-Tage-Inzidenz in X Wochen an, falls sich der aktuelle Trend fortsetzt, also RwK gleich bleibt.
 Das ist in der Realität in der Regel nicht so, und bei kleinen Zahlen und und auf Landkreis-Ebene ergeben
 sich gelegentlich absurd hohe Zahlen, aber so ist nun mal die Mathematik, wenn man den aktuellen Trend fortschreibt. 
""")

h_TageBis=makeDefinition("Fälle je 100.000, Tage bis X",
"""
Zeigt an, wie viele Tage es dauert, bis eine Inzidenz von X erreicht ist, falls sich der aktuelle Trend fortsetzt, also RwK gleich bleibt.
 Ist die Zahl ist negativ, bedeutet dass erst mal, dass bei aktuellem Trend X niemals erreicht wird.
 Das kann ist in verschiedenen Situation passieren: 1) wenn die aktuelle Inzidenz bereits kleiner X ist und weiter fällt oder
 2) wenn der Trend > 1, also steigend ist, und die aktuelle Inzidenz höher als X ist. 
 In beiden Fällen liegt der Zeitpunkt quasi in der Vergangenheit und gibt an, wie lange es hypothetisch her ist,
 seit X über- oder unterschritten wurde, hätte die ganze Zeit über derselbe Trend geherrscht wie aktuell.  
""")

h_Strikt=makeDefinition("Fälle strikt 7 Tage",
'''
 enthält zum Vergleich die Berechnung, mit der das RKI die 7-Tage-Inzidenz ermittelt (Spalte "absolut"). Dabei fallen alle
 Fälle unter den Tisch, deren Meldedatum beim Gesundheitsamt älter als 7 Tage ist. "RKI ignoriert" enthält die Zahl der Fälle,
 die dabei wären, würde man alle in den letzten 7 gemeldeten Fälle zählen, so wie es hier allen anderen Berechnungen zugrundeliegt.
 "RKI ignoriert %" ist der Prozentsatz an ignorierten Fällen. Ein hohre Prozentsatz ist ein Indikator dafür, dass die
 Gesundheitsämter vor Ort überlastet sind. Bemerkenswert ist, dass einige Ämter as auch bei hohen Fallzahlen schaffen,
 sämtliche Fälle innerhalb von 7 Tagen zu testen und die Ergebnisse ans RKI zu übermitteln und 0 ignorierte Fälle zu produzieren.
 Die von lokalen Behörden ausgewiesene Inzidenz kann in der Nähe des "strikten" RKI-Werts ("absolut") liegen oder näher an meinem
 Wert ("Fälle letzte 7 Tage"), je nachdem, wie vor Ort gerechnet wird.
''')


h_LetzteMeldung=makeDefinition("Letzte Meldung",
"""
 gibt an, vor wie viel Tagen sich der letzte Erkrankte beim Gesundheitsamt gemeldet hat. 0 heisst, dass sich Personenen
 in der Region am (Vor-)Tag des Datenstands beim Amt gemeldet haben, getestet wurden und die Daten am selben Tag ans RKI
 übermittelt und dort gezählt wurden. -5 bedeutet, dass der neueste beim RKI bekannte Fall eine Person ist,
 die sich vor 5 Tagen beim Amt gemeldet hat.
""")

h_LetzteZaehlung=makeDefinition("Letzte Zählung",
"""
 gibt an, vor wie viel Tagen der letzte Fall aus der Region beim RKI eingegangen ist. 0 heisst, dass die Region am (Vor-)Tag des Datenstands Fälle 
gemeldet hat, -5 bedeutet, dass seit 5 Tagen keine Meldungen eingegangen sind.
""")

h_MeldeDelay=makeDefinition("Meldeverzögerung",
"""
 ist eine Auswertung der Anzahl der Tage von der Meldung beim Gesundheitsamt bis zum Eingang und Zählung
 beim RKI als Fall in der offiziellen Statistik auf Bundesebene. Hierbei wird angezeigt: Maximum und Mittelwert aller Verzögerungen 
 (Summe/Anzahl), der Median (ca. die Hälfte der Verzögerungen liegt unter dem Wert, Hälfte darüber),
  die Standardabweichung (durchschnittliche Abweichung vom Mittel) angezeigt sowie die Zahl der Fälle,
  die in die Berechnung eingegangen sind. Dabei werden alle heute gemeldeten Fälle sowie alle Negativmeldungen bzw."
  Stornomeldungen mitgezählt, so dass die Zahl höher sein kann als die Zahl der heute gemeldeten Fälle.
""")


h_RisikoList=html.Ul([
    html.Li(["N = Bevölkerung / Dunkelzifferfaktor / ([Anzahl der Fälle in den letzten 2 Wochen] *",h_RwK,")"]),
    html.Li("Als Faktor für die Dunkelziffer wurde 3,5 gewählt, also auf 1 gemeldeten Infizierten werden 2,5 weitere ungemeldete vermutet"),
    html.Li("Als grobe Annäherung an die Zahl der Ansteckenden wurde die Summe der Fälle der letzten 7 Tage gewählt"),
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
        html.Li([makeColorSpan("Ultra: ", conditionUltra),
                 "Viel schlimmer als Super-rot."
                ],
                style=LiStyle
        ),
        html.Li([makeColorSpan("Super-Rot: ", conditionDanger),
                 "Lasset alle Hoffnung fahren. Die Situation ist praktisch ausser Kontrolle."
                 "Wer kürzlich da war und ungeschützte Kontakte hatte, ist mit einer Wahrscheinlichkeit von (Anzahl der Kontakte)/N infiziert."
                 "Empfehlung: Möglichst zu Hause bleiben und außer Haus bestmögliche Schutzmaßnahmen ergreifen. Gegend weiträumig meiden."
                ],
                style=LiStyle
        ),
        html.Li([makeColorSpan("Super-Grün: ",conditionSafe),
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
        html.Tr([html.Td(h_R7)]),
        html.Tr([html.Td([h_Risiko, h_RisikoList]), html.Td(h_BgtFarben,style=cellStyleFarben)]),
        html.Tr([html.Td(h_Strikt)]),
        html.Tr([html.Td(h_LetzteMeldung)]),
        html.Tr([html.Td(h_LetzteZaehlung)]),
        html.Tr([html.Td(h_MeldeDelay)]),
        html.Tr([html.Td(h_Prognose)]),
        html.Tr([html.Td(h_TageBis)]),
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
    h_Downloads,
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

app.title = appTitle

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
debugFlag = False
if __name__ == '__main__':
    if socket.gethostname() == 'westphal.uberspace.de':
        app.run_server(host='0.0.0.0', port=1024,debug=debugFlag)
    else:
        app.run_server(host='::', port=1024,debug=debugFlag)
