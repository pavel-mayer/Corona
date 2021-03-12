#!/usr/bin/env python3

# Quick hack to browse RKI-NPGEO-Data, Pavel Mayer 2020,
# License: Use freely at your own risk.

# pip install Click==7.0 Flask==1.1.1 itsdangerous==1.1.0 Jinja2==2.10.3 MarkupSafe==1.1.1 uWSGI==2.0.18 Werkzeug==0.16.0 dash=1.11.0
# pip install Click Flask itsdangerous Jinja2 MarkupSafe uWSGI Werkzeug matplotlib dash pandas datatable feather-format dash=1.11.0
# pip install: matplotlib dash pandas datatable feather-format

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

versionStr="1.0.0.0"

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
    #sortColumns = ["Risk","LetzteMeldung","InzidenzFallNeu-7-Tage"]
    sortColumns = ["Kontaktrisiko","InzidenzFallNeu-7-Tage"]
    todayTable = getRankedTable(fullTable, day, sortColumns)
    yesterdayTable = getRankedTable(fullTable, day-1, sortColumns)
    print(todayTable)
    print(yesterdayTable)
    todayTableById = todayTable.sort("IdLandkreis")
    yesterdayTableById = yesterdayTable.sort("IdLandkreis")
    print(todayTableById.nrows)
    print(yesterdayTableById.nrows)

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
    clip(todayTableById, "InzidenzFallNeu-Tage-bis-50", 9999.9)
    clip(todayTableById, "InzidenzFallNeu-Tage-bis-100", 9999.9)
    clip(todayTableById, "InzidenzFallNeu-Prognose-4-Wochen", 9999.9)
    clip(todayTableById, "InzidenzFallNeu-Prognose-8-Wochen", 9999.9)

    return todayTableById


# def processData(fullCurrentTable, forDay):
#
#     fullTable = fullCurrentTable[(dt.f.newCaseOnDay <= forDay) | (dt.f.newCaseBeforeDay < forDay),:]
#
#     #lastDay = fullTable[:, 'MeldeDay'].max()[0, 0]
#     #lastnewCaseOnDay = fullTable[:, 'newCaseOnDay'].max()[0, 0]
#
#     alldays=fullTable[:,
#               [dt.sum(dt.f.AnzahlFall),
#                dt.sum(dt.f.FaellePro100k),
#                dt.sum(dt.f.AnzahlTodesfall),
#                dt.sum(dt.f.TodesfaellePro100k),
#                dt.first(dt.f.Bevoelkerung),
#                dt.max(dt.f.MeldeDay),
#                dt.max(dt.f.newCaseOnDay),
#                dt.first(dt.f.LandkreisTyp),
#                dt.first(dt.f.IdLandkreis),
#                dt.first(dt.f.IdBundesland),
#                dt.first(dt.f.Bundesland)],
#     dt.by(dt.f.Landkreis)]
#
#     #print(alldays)
#     # compute and add rows for Bundeslaender
#     bevoelkerung = alldays[:, [dt.sum(dt.f.Bevoelkerung), dt.sum(dt.f.AnzahlFall), dt.sum(dt.f.AnzahlTodesfall),], dt.by(dt.f.Bundesland)]
#     bevoelkerung=bevoelkerung[:,dt.f[:].extend({"FaellePro100k": dt.f.AnzahlFall * 100000 / dt.f.Bevoelkerung})]
#     bevoelkerung=bevoelkerung[:,dt.f[:].extend({"TodesfaellePro100k": dt.f.AnzahlTodesfall * 100000 / dt.f.Bevoelkerung})]
#
#     alldaysBundeslaender = fullTable[:,
#               [dt.sum(dt.f.AnzahlFall),
#                dt.first(dt.f.FaellePro100k), # just create the column, will be overwritten
#                dt.sum(dt.f.AnzahlTodesfall),
#                dt.first(dt.f.TodesfaellePro100k), # just create the column, will be overwritten
#                dt.first(dt.f.Bevoelkerung), # just create the column, will be overwritten
#                dt.max(dt.f.MeldeDay),
#                dt.max(dt.f.newCaseOnDay),
#                dt.first(dt.f.LandkreisTyp), # just create the column, will be overwritten
#                dt.first(dt.f.IdLandkreis),
#                dt.first(dt.f.IdBundesland),
#                dt.first(dt.f.Landkreis)],
#             dt.by(dt.f.Bundesland)]
#
#     alldaysBundeslaender[:, "Bevoelkerung"] = bevoelkerung[:, "Bevoelkerung"]
#     alldaysBundeslaender[:, "Landkreis"] = bevoelkerung[:, "Bundesland"]
#     alldaysBundeslaender[:, "FaellePro100k"] = bevoelkerung[:, "FaellePro100k"]
#     alldaysBundeslaender[:, "TodesfaellePro100k"] = bevoelkerung[:, "TodesfaellePro100k"]
#     alldaysBundeslaender[:, "LandkreisTyp"] = "B"
#     #print(alldaysBundeslaender[:, "IdLandkreis"])
#     alldaysBundeslaender[:, "IdLandkreis"] = alldaysBundeslaender[:, "IdBundesland"]
#     #print(alldaysBundeslaender[:, ["IdLandkreis","Bundesland"]])
#     alldays.rbind(alldaysBundeslaender, force=True)
#
#     # compute and add row for all Germany
#     bevoelkerungGermany = alldays[dt.f.Landkreis != dt.f.Bundesland, [dt.sum(dt.f.Bevoelkerung), dt.sum(dt.f.AnzahlFall), dt.sum(dt.f.AnzahlTodesfall)]]
#     bevoelkerungGermany = bevoelkerungGermany[:,
#                           dt.f[:].extend({"FaellePro100k": dt.f.AnzahlFall * 100000 / dt.f.Bevoelkerung})]
#     bevoelkerungGermany = bevoelkerungGermany[:,
#                           dt.f[:].extend({"TodesfaellePro100k": dt.f.AnzahlTodesfall * 100000 / dt.f.Bevoelkerung})]
#
#     alldaysGermany = fullTable[dt.f.Landkreis != dt.f.Bundesland,
#                      [dt.sum(dt.f.AnzahlFall),
#                       dt.first(dt.f.FaellePro100k),  # just create the column, will be overwritten
#                       dt.sum(dt.f.AnzahlTodesfall),
#                       dt.first(dt.f.TodesfaellePro100k),  # just create the column, will be overwritten
#                       dt.first(dt.f.Bevoelkerung),  # just create the column, will be overwritten
#                       dt.max(dt.f.MeldeDay),
#                       dt.max(dt.f.newCaseOnDay),
#                       dt.first(dt.f.LandkreisTyp),  # just create the column, will be overwritten
#                       dt.first(dt.f.Landkreis),
#                       dt.first(dt.f.IdLandkreis),
#                       dt.first(dt.f.IdBundesland),
#                       dt.first(dt.f.Bundesland)]]
#     alldaysGermany[:, "Bevoelkerung"] = bevoelkerungGermany[:, "Bevoelkerung"]
#     alldaysGermany[:, "Landkreis"] = "Deutschland"
#     alldaysGermany[:, "Bundesland"] = "Deutschland"
#     alldaysGermany[:, "FaellePro100k"] = bevoelkerungGermany[:, "FaellePro100k"]
#     alldaysGermany[:, "TodesfaellePro100k"] = bevoelkerungGermany[:, "TodesfaellePro100k"]
#     alldaysGermany[:, "LandkreisTyp"] = "BR"
#     alldaysGermany[:, "IdLandkreis"] = 0
#     alldaysGermany[:, "IdBundesland"] = 0
#     alldays.rbind(alldaysGermany, force=True)
#
#     ##############################################################################
#     # compute values for last 7 days for Landkreise
#     last7daysRecs = fullTable[((dt.f.newCaseOnDay > forDay - 7) & (dt.f.MeldeDay > forDay - 14)) | (dt.f.newDeathOnDay > forDay - 7), :]
#     strictLast7daysRecs = fullTable[((dt.f.newCaseOnDay > forDay - 7) & (dt.f.MeldeDay > forDay - 7)) | (dt.f.newDeathOnDay > forDay - 7), :]
#     #last7daysRecs.to_csv("last7daysRecs.csv")
#     last7days = last7daysRecs[:,
#                 [dt.sum(dt.f.AnzahlFall),
#                dt.sum(dt.f.FaellePro100k),
#                dt.sum(dt.f.AnzahlTodesfall),
#                dt.sum(dt.f.TodesfaellePro100k)],
#                 dt.by(dt.f.Landkreis)]
#     last7days.names=["Landkreis","AnzahlFallLetzte7Tage","FaellePro100kLetzte7Tage","AnzahlTodesfallLetzte7Tage",
#                      "TodesfaellePro100kLetzte7Tage"]
#
#     strictLast7days = strictLast7daysRecs[:,
#                 [dt.sum(dt.f.AnzahlFall),
#                  dt.sum(dt.f.FaellePro100k)],
#                 dt.by(dt.f.Landkreis)]
#     strictLast7days.names = ["Landkreis", "AnzahlFallLetzte7TageStrikt", "FaellePro100kLetzte7TageStrikt"]
#     #print(strictLast7days)
#     last7days = join(last7days,strictLast7days, "Landkreis" )
#     #print(last7days)
#
#     # compute values for last 7 days for Bundesländer
#     last7daysBL = last7daysRecs[:,
#                 [dt.sum(dt.f.AnzahlFall),
#                  dt.sum(dt.f.AnzahlTodesfall),
#                  dt.sum(dt.f.Bevoelkerung)],
#                   dt.by(dt.f.Bundesland)]
#     last7daysBL.names = ["Landkreis", "AnzahlFallLetzte7Tage", "AnzahlTodesfallLetzte7Tage","Bevoelkerung"]
#     strictLast7daysBL = strictLast7daysRecs[:,
#                 [dt.sum(dt.f.AnzahlFall),
#                  dt.sum(dt.f.Bevoelkerung)],
#                   dt.by(dt.f.Bundesland)]
#     strictLast7daysBL.names = ["Landkreis", "AnzahlFallLetzte7TageStrikt", "Bevoelkerung"]
#
#     bls = last7daysBL[:,"Landkreis"].to_list()[0]
#     blb = bevoelkerung[:,"Bundesland"].to_list()[0]
#     #print(bls)
#     #print(blb)
#     for i, bl in enumerate(bls):
#         for j, bb in enumerate(blb):
#             #print(i,j)
#             if last7daysBL[i, "Landkreis"] == bevoelkerung[j, "Bundesland"]:
#                 last7daysBL[i, "Bevoelkerung"] = bevoelkerung[j, "Bevoelkerung"]
#             if strictLast7daysBL[i, "Landkreis"] == bevoelkerung[j, "Bundesland"]:
#                 strictLast7daysBL[i, "Bevoelkerung"] = bevoelkerung[j, "Bevoelkerung"]
#
#     #last7daysBL[:, "Bevoelkerung"] = bevoelkerung[:, "Bevoelkerung"]
#     last7daysBL = last7daysBL[:, dt.f[:].extend({"FaellePro100kLetzte7Tage": dt.f.AnzahlFallLetzte7Tage * 100000 / dt.f.Bevoelkerung})]
#     last7daysBL = last7daysBL[:, dt.f[:].extend({"TodesfaellePro100kLetzte7Tage": dt.f.AnzahlTodesfallLetzte7Tage * 100000 / dt.f.Bevoelkerung})]
#     #print(last7daysBL)
#     last7days.rbind(last7daysBL, force=True)
#     #print(last7days)
#
#     strictLast7daysBL = strictLast7daysBL[:,
#                   dt.f[:].extend({"FaellePro100kLetzte7TageStrikt": dt.f.AnzahlFallLetzte7TageStrikt * 100000 / dt.f.Bevoelkerung})]
#     strictLast7daysBL = strictLast7daysBL[:, ["Landkreis","AnzahlFallLetzte7TageStrikt","FaellePro100kLetzte7TageStrikt"]]
#     #print(strictLast7daysBL)
#     last7days = merge(last7days,strictLast7daysBL, "Landkreis" )
#     #print(last7days)
#
#     # compute values for last 7 days for Germany
#     last7daysDE = last7daysRecs[:,
#                   [dt.first(dt.f.Landkreis),
#                    dt.sum(dt.f.AnzahlFall),
#                    dt.sum(dt.f.AnzahlTodesfall),
#                    dt.first(dt.f.Bevoelkerung),
#                    ]]
#     last7daysDE.names = ["Landkreis", "AnzahlFallLetzte7Tage", "AnzahlTodesfallLetzte7Tage", "Bevoelkerung"]
#     last7daysDE[:, "Landkreis"] = "Deutschland"
#     last7daysDE[:, "Bevoelkerung"] = bevoelkerungGermany[:, "Bevoelkerung"]
#     last7daysDE = last7daysDE[:,
#                   dt.f[:].extend({"FaellePro100kLetzte7Tage": dt.f.AnzahlFallLetzte7Tage * 100000 / dt.f.Bevoelkerung})]
#     last7daysDE = last7daysDE[:, dt.f[:].extend(
#         {"TodesfaellePro100kLetzte7Tage": dt.f.AnzahlTodesfallLetzte7Tage * 100000 / dt.f.Bevoelkerung})]
#
#     #print(last7daysDE)
#     last7days.rbind(last7daysDE, force=True)
#
#     strictLast7daysDE = strictLast7daysRecs[:,
#                   [dt.first(dt.f.Landkreis),
#                    dt.sum(dt.f.AnzahlFall),
#                    dt.first(dt.f.Bevoelkerung),
#                    ]]
#     strictLast7daysDE.names = ["Landkreis", "AnzahlFallLetzte7TageStrikt", "Bevoelkerung"]
#     strictLast7daysDE[:, "Landkreis"] = "Deutschland"
#     strictLast7daysDE[:, "Bevoelkerung"] = bevoelkerungGermany[:, "Bevoelkerung"]
#     strictLast7daysDE = strictLast7daysDE[:,
#                   dt.f[:].extend({"FaellePro100kLetzte7TageStrikt": dt.f.AnzahlFallLetzte7TageStrikt * 100000 / dt.f.Bevoelkerung})]
#     last7days = merge(last7days,strictLast7daysDE, "Landkreis" )
#
#     last7days = last7days[:, dt.f[:].extend({"FaelleLetzte7TageDropped": dt.f.AnzahlFallLetzte7Tage - dt.f.AnzahlFallLetzte7TageStrikt})]
#     last7days = last7days[:, dt.f[:].extend({"FaelleLetzte7TageDroppedPercent": dt.f.FaelleLetzte7TageDropped * 100/dt.f.AnzahlFallLetzte7Tage})]
#
#     # clip case count to zero
#     last7days[dt.f.AnzahlFallLetzte7Tage <0, "AnzahlFallLetzte7Tage"] = 0
#     last7days[dt.f.FaellePro100kLetzte7Tage <0, "FaellePro100kLetzte7Tage"] = 0
#     last7days[dt.f.AnzahlTodesfallLetzte7Tage <0, "AnzahlTodesfallLetzte7Tage"] = 0
#     last7days[dt.f.TodesfaellePro100kLetzte7Tage <0, "TodesfaellePro100kLetzte7Tage"] = 0
#
#     last7days[dt.f.AnzahlFallLetzte7TageStrikt < 0, "AnzahlFallLetzte7TageStrikt"] = 0
#     last7days[dt.f.FaellePro100kLetzte7TageStrikt < 0, "FaellePro100kLetzte7TageStrikt"] = 0
#     last7days[dt.f.FaelleLetzte7TageDropped < 0, "FaelleLetzte7TageDropped"] = 0
#     last7days[dt.f.FaelleLetzte7TageDroppedPercent < 0, "FaelleLetzte7TageDroppedPercent"] = 0
#
#     ##############################################################################
#     # compute values for last 7 days before 7 days
#
#     lastWeek7daysRecs = fullTable[((dt.f.newCaseOnDay > forDay - 14) & (dt.f.newCaseOnDay <= forDay - 7)
#                                    & (dt.f.MeldeDay > forDay - 21) & (dt.f.MeldeDay <= forDay - 7)) |
#                                   ((dt.f.newDeathOnDay > forDay - 14) & (dt.f.newDeathOnDay <= forDay - 7))
#                                   , :]
#     lastWeek7days=lastWeek7daysRecs[:,
#                     [dt.sum(dt.f.AnzahlFall),
#                dt.sum(dt.f.FaellePro100k),
#                dt.sum(dt.f.AnzahlTodesfall),
#                dt.sum(dt.f.TodesfaellePro100k)],
#        dt.by(dt.f.Landkreis)]
#     #lastWeek7days[dt.f[1:] < 0, dt.f[1:]] = 0
#     lastWeek7days.names=["Landkreis","AnzahlFallLetzte7TageDavor","FaellePro100kLetzte7TageDavor",
#                          "AnzahlTodesfallLetzte7TageDavor","TodesfaellePro100kLetzte7TageDavor"]
#
#     # compute values for last 7 days before 7 days for Bundesländer
#     lastWeek7daysBL = lastWeek7daysRecs[:,
#                   [dt.sum(dt.f.AnzahlFall),
#                    dt.sum(dt.f.AnzahlTodesfall),
#                    dt.sum(dt.f.Bevoelkerung)],
#                   dt.by(dt.f.Bundesland)]
#     lastWeek7daysBL.names = ["Landkreis", "AnzahlFallLetzte7TageDavor", "AnzahlTodesfallLetzte7TageDavor", "Bevoelkerung"]
#
#     bls = lastWeek7daysBL[:,"Landkreis"].to_list()[0]
#     blb = bevoelkerung[:, "Bundesland"].to_list()[0]
#     # print(bls)
#     # print(blb)
#     for i, bl in enumerate(bls):
#         for j, bb in enumerate(blb):
#             # print(i,j)
#             if lastWeek7daysBL[i, "Landkreis"] == bevoelkerung[j, "Bundesland"]:
#                 lastWeek7daysBL[i, "Bevoelkerung"] = bevoelkerung[j, "Bevoelkerung"]
#
#     #lastWeek7daysBL[:, "Bevoelkerung"] = bevoelkerung[:, "Bevoelkerung"]
#     lastWeek7daysBL = lastWeek7daysBL[:, dt.f[:].extend({"FaellePro100kLetzte7TageDavor": dt.f.AnzahlFallLetzte7TageDavor * 100000 / dt.f.Bevoelkerung})]
#     lastWeek7daysBL = lastWeek7daysBL[:, dt.f[:].extend({"TodesfaellePro100kLetzte7TageDavor": dt.f.AnzahlTodesfallLetzte7TageDavor * 100000 / dt.f.Bevoelkerung})]
#     lastWeek7days.rbind(lastWeek7daysBL, force=True)
#
#     # compute values for last 7 days for Germany
#     lastWeek7daysDE = lastWeek7daysRecs[:,
#                   [dt.first(dt.f.Landkreis),
#                    dt.sum(dt.f.AnzahlFall),
#                    dt.sum(dt.f.AnzahlTodesfall),
#                    dt.first(dt.f.Bevoelkerung),
#                    ]]
#     lastWeek7daysDE.names = ["Landkreis", "AnzahlFallLetzte7TageDavor", "AnzahlTodesfallLetzte7TageDavor", "Bevoelkerung"]
#     lastWeek7daysDE[:, "Landkreis"] = "Deutschland"
#     lastWeek7daysDE[:, "Bevoelkerung"] = bevoelkerungGermany[:, "Bevoelkerung"]
#     lastWeek7daysDE = lastWeek7daysDE[:,
#                   dt.f[:].extend({"FaellePro100kLetzte7TageDavor": dt.f.AnzahlFallLetzte7TageDavor * 100000 / dt.f.Bevoelkerung})]
#     lastWeek7daysDE = lastWeek7daysDE[:, dt.f[:].extend(
#         {"TodesfaellePro100kLetzte7TageDavor": dt.f.AnzahlTodesfallLetzte7TageDavor * 100000 / dt.f.Bevoelkerung})]
#     lastWeek7days.rbind(lastWeek7daysDE, force=True)
#
#     ##############################################################################
#     # compute delays
#     firstRecordTime = time.strptime("29.4.2020", "%d.%m.%Y")  # struct_time
#     firstRecordDay = cd.dayFromTime(firstRecordTime)
#     firstRecordDay = forDay - 21
#
#     delayRecs = fullTable[(dt.f.newCaseOnDay > firstRecordDay) | (dt.f.newDeathOnDay > firstRecordDay), :]
#     delayRecs.materialize()
#     #print(delayRecs)
#     #delayRecs.to_csv("delayRecs.csv")
#     delays = delayRecs[:, [dt.mean(dt.f.MeldeDelay), dt.median(dt.f.MeldeDelay), dt.sd(dt.f.MeldeDelay), dt.sum(dt.f.AnzahlFall)], dt.by(dt.f.Landkreis)]
#     delays.names = ["Landkreis", "DelayMean", "DelayMedian", "DelaySD", "DelayAnzahlFall"]
#
#     delaysBL = delayRecs[:, [dt.mean(dt.f.MeldeDelay), dt.median(dt.f.MeldeDelay), dt.sd(dt.f.MeldeDelay), dt.sum(dt.f.AnzahlFall)], dt.by(dt.f.Bundesland)]
#     delaysBL.names = ["Landkreis", "DelayMean", "DelayMedian", "DelaySD", "DelayAnzahlFall"]
#     delays.rbind(delaysBL)
#
#     delaysDE = delayRecs[:, [dt.first(dt.f.Landkreis), dt.mean(dt.f.MeldeDelay), dt.median(dt.f.MeldeDelay), dt.sd(dt.f.MeldeDelay), dt.sum(dt.f.AnzahlFall)]]
#     delaysDE.names = ["Landkreis", "DelayMean", "DelayMedian", "DelaySD", "DelayAnzahlFall"]
#     delaysDE[:, "Landkreis"] = "Deutschland"
#     delays.rbind(delaysDE)
#
#     #print(delaysBL)
#
#     alldays = join(alldays, delays, "Landkreis")
#     ##############################################################################
#
#     # clip case count to zero
#     lastWeek7days[dt.f.AnzahlFallLetzte7TageDavor <0, "AnzahlFallLetzte7TageDavor"] = 0
#     lastWeek7days[dt.f.FaellePro100kLetzte7TageDavor <0, "FaellePro100kLetzte7TageDavor"] = 0
#     lastWeek7days[dt.f.AnzahlTodesfallLetzte7TageDavor <0, "AnzahlTodesfallLetzte7TageDavor"] = 0
#     lastWeek7days[dt.f.TodesfaellePro100kLetzte7TageDavor <0, "TodesfaellePro100kLetzte7TageDavor"] = 0
#
#     allDaysExt0 = join(alldays, last7days, "Landkreis")
#     allDaysExt1 = join(allDaysExt0, lastWeek7days, "Landkreis")
#
#     Rw = (dt.f.AnzahlFallLetzte7Tage+5)/(dt.f.AnzahlFallLetzte7TageDavor + 5)
#
#     allDaysExt2=allDaysExt1[:,dt.f[:].extend({"AnzahlFallTrend":  Rw})]
#
#     #RwSqrt = (dt.math.sqrt(dt.f.AnzahlFallTrend))
#     RwSqrt = (dt.math.pow(dt.f.AnzahlFallTrend, 4/7))
#     allDaysExt2=allDaysExt2[:,dt.f[:].extend({"AnzahlFallTrendSqrt":  RwSqrt})]
#     allDaysExt3=allDaysExt2[:,dt.f[:].extend({"FaellePro100kTrend": dt.f.FaellePro100kLetzte7Tage-dt.f.FaellePro100kLetzte7TageDavor})]
#     allDaysExt3=allDaysExt3[:,dt.f[:].extend({"FaellePro100kPrognose": dt.f.FaellePro100kLetzte7Tage*dt.math.pow(dt.f.AnzahlFallTrend, 4)})]
#     allDaysExt3=allDaysExt3[:,dt.f[:].extend({"FaellePro100kPrognose2": dt.f.FaellePro100kLetzte7Tage*dt.math.pow(dt.f.AnzahlFallTrend, 8)})]
#
#     # res = cur * trend ^ t
#     # trend ^ t = res/cur
#     # t = log(trend, res/cur)
#     # t = ln(res/cur)/ln(trend)
#
#     allDaysExt3=allDaysExt3[:,dt.f[:].extend({"FaellePro100kTageBisSicher": dt.math.log(3/dt.f.FaellePro100kLetzte7Tage) / dt.math.log(dt.f.AnzahlFallTrend) * 7})]
#     allDaysExt3=allDaysExt3[:,dt.f[:].extend({"FaellePro100kTageBisSicher2": dt.math.log(7/dt.f.FaellePro100kLetzte7Tage) / dt.math.log(dt.f.AnzahlFallTrend) * 7})]
#
#     allDaysExt4=allDaysExt3[:,dt.f[:].extend({"TodesfaellePro100kTrend": dt.f.TodesfaellePro100kLetzte7Tage-dt.f.TodesfaellePro100kLetzte7TageDavor})]
#
#     allDaysExt5=allDaysExt4[:,dt.f[:].extend({"Kontaktrisiko": dt.f.Bevoelkerung/3.5/((dt.f.AnzahlFallLetzte7Tage+dt.f.AnzahlFallLetzte7TageDavor)*Rw)})]
#     allDaysExt6 = allDaysExt5[:, dt.f[:].extend({"LetzteMeldung": forDay - dt.f.MeldeDay})]
#     allDaysExt6b = allDaysExt6[:, dt.f[:].extend({"LetzteMeldungNeg": dt.f.MeldeDay - forDay})]
#     allDaysExt6c = allDaysExt6b[:, dt.f[:].extend({"LetzteZaehlungNeg": dt.f.newCaseOnDay - forDay})]
#     datenStand = cd.dateStrYMDFromDay(forDay+1)
#     allDaysExt6c = allDaysExt6c[:, dt.f[:].extend({"Datenstand": datenStand})]
#     allDaysExt6c = allDaysExt6c[:, dt.f[:].extend({"Sofwareversion": versionStr})]
#
#     allDaysExt6c[dt.f.Kontaktrisiko * 2 == dt.f.Kontaktrisiko, "Kontaktrisiko"] = 99999
#     allDaysExt6c[dt.f.FaellePro100kPrognose >= 10000, "FaellePro100kPrognose"] = 9999.9
#     allDaysExt6c[dt.f.FaellePro100kPrognose2 >= 10000, "FaellePro100kPrognose2"] = 9999.9
#
#     sortedByRisk = allDaysExt6c.sort(["Kontaktrisiko","LetzteMeldung","FaellePro100k"])
#     #print(sortedByRisk)
#     allDaysExt=sortedByRisk[:,dt.f[:].extend({"Rang": 0})]
#     allDaysExt[:,"Rang"]=np.arange(1,allDaysExt.nrows+1)
#     #print(allDaysExt)
#     #allDaysExt.materialize()
#     return allDaysExt
#
# def loadAndProcessData(fileName):
#     currentFullTable, lastDay = loadData(fileName)
#     todayTable = processData(currentFullTable, lastDay).sort("Landkreis")
#     yesterdayTable = processData(currentFullTable, lastDay-1).sort("Landkreis")
#
#     #print(currentFullTable)
#     #print(todayTable)
#     #print(yesterdayTable)
#
#     resultTable = todayTable[:, dt.f[:].extend({"NewCasesToday": 0})]
#     change = np.subtract(todayTable[:, "AnzahlFall"],yesterdayTable[:, "AnzahlFall"])
#     resultTable[:, "NewCasesToday"] = change
#
#     for i in range(yesterdayTable.nrows):
#         l_name = todayTable[i, dt.f.Landkreis].to_list()[0][0]
#         #l_id = todayTable[i, dt.f.IdLandkreis].to_list()[0][0]
#         l_new_name = yesterdayTable[i, dt.f.Landkreis].to_list()[0][0]
#         if l_name != l_new_name:
#             print("missing {} in today, was {} yesterday".format(l_name, l_new_name))
#             exit(1)
#         #else:
#         #    print("{}: ok: {}".format(i, l_name))
#
#     resultTable=resultTable[:,dt.f[:].extend({"RangChange": 0})]
#     rangChange = np.subtract(yesterdayTable[:,"Rang"],todayTable[:,"Rang"])
#     resultTable[:,"RangChange"] = rangChange
#
#     resultTable=resultTable[:,dt.f[:].extend({"RangYesterday": 0})]
#     resultTable[:,"RangYesterday"] = yesterdayTable[:,"Rang"]
#
#     resultTable=resultTable[:,dt.f[:].extend({"RangChangeStr": "-"})]
#
#     rangChangeStrs = np.full(len(rangChange), "*")
#     for i, rc in enumerate(rangChange):
#         #print(i, rc)
#
#         rangChangeStr = ""
#         if rc > 0:
#             rangChangeStr = "▲"
#         if rc < 0:
#             rangChangeStr = "▼"
#
#         resultTable[i, "RangChangeStr"] = rangChangeStr
#         rangChangeStrs[i] = rangChangeStr
#
#     #print(rangChangeStrs)
#
#     #print("Column names frame order:", list(enumerate(resultTable.names)))
#     resultTable2 = resultTable.sort("Rang")
#     #print(resultTable2)
#     data = resultTable2.to_pandas()
#     #print(data)
#
#     return data

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
        ('InzidenzFallNeu-7-Tage-Trend-Spezial', ['Fälle', 'RwK'], 'numeric', FormatFixed2, colWidth(70)),
        ('InzidenzFallNeu-7-Tage-R', ['Fälle', 'R7'], 'numeric', FormatFixed2, colWidth(70)),
        ('AnzahlFallNeu-7-Tage', ['Fälle', 'letzte 7 Tage'], 'numeric', FormatInt, colWidth(defaultColWidth)),
        ('AnzahlFallNeu-7-Tage-7-Tage-davor', ['Fälle', 'vorl. 7 Tage'], 'numeric', FormatInt, colWidth(defaultColWidth)),
        ('AnzahlFall', ['Fälle', 'total'], 'numeric', FormatInt, colWidth(90)),
        ('AnzahlFallNeu', ['Fälle', 'neu'], 'numeric', FormatInt, colWidth(defaultColWidth)),

        ('InzidenzFallNeu-7-Tage', ['Fälle je 100.000', 'letzte 7 Tage'], 'numeric', FormatFixed1, colWidth(defaultColWidth)),
        ('InzidenzFallNeu-7-Tage-7-Tage-davor', ['Fälle je 100.000', 'vorl. 7 Tage'], 'numeric', FormatFixed1, colWidth(defaultColWidth)),
        #('FaellePro100kTrend', ['Fälle je 100.000', 'Diff.'], 'numeric', FormatFixed1, colWidth(defaultColWidth)),
        ('InzidenzFall', ['Fälle je 100.000', 'total'], 'numeric', FormatFixed1, colWidth(60)),
        ('InzidenzFallNeu-Prognose-4-Wochen', ['Fälle je 100.000', 'in 4 Wochen'], 'numeric', FormatFixed1, colWidth(60)),
        ('InzidenzFallNeu-Prognose-8-Wochen', ['Fälle je 100.000', 'in 8 Wochen'], 'numeric', FormatFixed1, colWidth(60)),
        ('InzidenzFallNeu-Tage-bis-50', ['Fälle je 100.000', 'Tage bis 50'], 'numeric', FormatInt, colWidth(60)),
        ('InzidenzFallNeu-Tage-bis-100', ['Fälle je 100.000', 'Tage bis 100'], 'numeric', FormatInt, colWidth(60)),

        ('AnzahlFallNeu-Meldung-letze-7-Tage-7-Tage', ['Fälle strikt 7 Tage', 'absolut'], 'numeric', FormatInt, colWidth(defaultColWidth)),
        ('InzidenzFallNeu-Meldung-letze-7-Tage-7-Tage', ['Fälle strikt 7 Tage', 'je 100.000'], 'numeric', FormatFixed1, colWidth(defaultColWidth)),
        ('AnzahlFallNeu-7-Tage-Dropped', ['Fälle strikt 7 Tage', 'RKI ignoriert'], 'numeric', FormatInt, colWidth(defaultColWidth)),
        ('ProzentFallNeu-7-Tage-Dropped', ['Fälle strikt 7 Tage', 'RKI ignoriert %'], 'numeric', FormatFixed1, colWidth(defaultColWidth)),

        ('DatenstandTag-Diff', ['Meldung', 'Letzte Zählung'], 'numeric', FormatInt, colWidth(70)),
        ('MeldeDauerFallNeu-Min-Neg', ['Meldung', 'Letzte Meldung'], 'numeric', FormatInt, colWidth(70)),
        ('MeldeDauerFallNeu-Max', ['Meldeverzögerung (Tage)', 'Max.'], 'numeric', FormatFixed1, colWidth(62)),
        ('MeldeDauerFallNeu-Schnitt', ['Meldeverzögerung (Tage)', 'Mittel x̅'], 'numeric', FormatFixed1, colWidth(62)),
        ('MeldeDauerFallNeu-Median', ['Meldeverzögerung (Tage)', 'Median x̃'], 'numeric', FormatInt, colWidth(62)),
        ('MeldeDauerFallNeu-StdAbw', ['Meldeverzögerung (Tage)', 'Stdabw. σx'], 'numeric', FormatFixed1, colWidth(62)),
        ('MeldeDauerFallNeu-Fallbasis', ['Meldeverzögerung (Tage)', 'Anzahl Fälle'], 'numeric', FormatInt, colWidth(62)),
        ('AnzahlTodesfallNeu-7-Tage', ['Todesfälle', 'letzte 7 Tage'], 'numeric', FormatInt, colWidth(defaultColWidth)),
        ('AnzahlTodesfallNeu-7-Tage-7-Tage-davor', ['Todesfälle', 'vorl. 7 Tage'], 'numeric', FormatInt, colWidth(defaultColWidth)),
        ('AnzahlTodesfall', ['Todesfälle', 'total'], 'numeric', FormatInt, colWidth(defaultColWidth)),
        ('InzidenzTodesfallNeu-7-Tage', ['Todesfälle je 100.000', 'letzte 7 Tage'], 'numeric', FormatFixed2, colWidth(defaultColWidth)),
        ('InzidenzTodesfallNeu-7-Tage-7-Tage-davor', ['Todesfälle je 100.000', 'vorl. 7 Tage'], 'numeric', FormatFixed2, colWidth(defaultColWidth)),
        ('InzidenzTodesfallNeu-7-Tage-Trend-Spezial', ['Todesfälle je 100.000', 'Trend'], 'numeric', FormatFixed2, colWidth(defaultColWidth)),
        ('InzidenzTodesfall', ['Todesfälle je 100.000', 'total'], 'numeric', FormatFixed2, colWidth(60)),
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
# dataFilename = "data.csv"
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
# csvData = open(dataFilename,"rb").read().decode('utf-8')
# csvFullData = open(fullTableFilename,"rb").read().decode('utf-8')

dataURL = '/covid/risks/data.csv'

def csvResponse(data):
    r = Response(response=data, status=200, mimetype="text/csv")
    r.headers["Content-Type"] = "text/csv; charset=utf-8"
    return r

@server.route(dataURL)
def csv_data():
    return csvResponse(csvData)

fullDataURL = '/covid/risks/full-data.csv'
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
fullTable = loadData("all-series.csv")

#maxDay = float(dframe["MeldeDay"].max())
maxDay = fullTable[:,"DatenstandTag"].max().to_list()[0][0]
print(maxDay)
dataVersionDate = cd.dateStrWDMYFromDay(maxDay+1)
print("Loading done, max Day {} date {}".format(maxDay, dataVersionDate))

print("Creating Datatable")

table = getTableForDay(fullTable, maxDay)
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
    print(c)
    return c


FaellePro100kLetzte7TageClass = makeConditionClass("InzidenzFallNeu-7-Tage",100,50,20,5,1)
FaellePro100kPrognoseClass = makeConditionClass("InzidenzFallNeu-Prognose-4-Wochen",100,50,20,5,1)
FaellePro100kPrognose2Class = makeConditionClass("InzidenzFallNeu-Prognose-8-Wochen",100,50,20,5,1)


# FaellePro100kLetzte7TageClass = {
#     conditionUltra : '{InzidenzFallNeu-7-Tage} > 50',
#     conditionDanger : '{InzidenzFallNeu-7-Tage} > 50 && {InzidenzFallNeu-7-Tage} <= 50',
#     conditionTooHigh: '{InzidenzFallNeu-7-Tage} > 20 && {InzidenzFallNeu-7-Tage} <= 50',
#     conditionSerious: '{InzidenzFallNeu-7-Tage} > 5 && {InzidenzFallNeu-7-Tage} <= 20',
#     conditionGood: '{InzidenzFallNeu-7-Tage} >= 1 && {InzidenzFallNeu-7-Tage} <= 5',
#     conditionSafe: '{InzidenzFallNeu-7-Tage} < 1'
# }
#
# FaellePro100kPrognoseClass = {
#     conditionDanger : '{InzidenzFallNeu-Prognose-4-Wochen} > 50',
#     conditionTooHigh: '{InzidenzFallNeu-Prognose-4-Wochen} > 20 && {InzidenzFallNeu-Prognose-4-Wochen} <= 50',
#     conditionSerious: '{InzidenzFallNeu-Prognose-4-Wochen} > 5 && {InzidenzFallNeu-Prognose-4-Wochen} <= 20',
#     conditionGood: '{InzidenzFallNeu-Prognose-4-Wochen} >= 1 && {InzidenzFallNeu-Prognose-4-Wochen} <= 5',
#     conditionSafe: '{InzidenzFallNeu-Prognose-4-Wochen} < 1'
# }
#
# FaellePro100kPrognose2Class = {
#     conditionDanger : '{InzidenzFallNeu-Prognose-8-Wochen} > 50',
#     conditionTooHigh: '{InzidenzFallNeu-Prognose-8-Wochen} > 20 && {InzidenzFallNeu-Prognose-8-Wochen} <= 50',
#     conditionSerious: '{InzidenzFallNeu-Prognose-8-Wochen} > 5 && {InzidenzFallNeu-Prognose-8-Wochen} <= 20',
#     conditionGood: '{InzidenzFallNeu-Prognose-8-Wochen} >= 1 && {InzidenzFallNeu-Prognose-8-Wochen} <= 5',
#     conditionSafe: '{InzidenzFallNeu-Prognose-8-Wochen} < 1'
# }

# AnzahlFallTrendClass = {
#     conditionDanger : '{InzidenzFallNeu-7-Tage-Trend-Spezial} > 3',
#     conditionTooHigh: '{InzidenzFallNeu-7-Tage-Trend-Spezial} > 1 && {InzidenzFallNeu-7-Tage-Trend-Spezial} <= 3',
#     conditionSerious: '{InzidenzFallNeu-7-Tage-Trend-Spezial} > 0.9 && {InzidenzFallNeu-7-Tage-Trend-Spezial} <= 1',
#     conditionGood: '{InzidenzFallNeu-7-Tage-Trend-Spezial} > 0.3 && {InzidenzFallNeu-7-Tage-Trend-Spezial} <= 0.9',
#     conditionSafe: '{InzidenzFallNeu-7-Tage-Trend-Spezial} < 0.3'
# }
AnzahlFallTrendClass = makeConditionClass("InzidenzFallNeu-7-Tage-Trend-Spezial",3,2,1,0.9,0.3)

LandkreisClass = {
    conditionUltra : KontaktrisikoClass[conditionUltra] +" || "+ braced(FaellePro100kLetzte7TageClass[conditionUltra])+" || "+braced(AnzahlFallTrendClass[conditionUltra]),
    conditionDanger : "("+KontaktrisikoClass[conditionDanger]+" || "+ braced(FaellePro100kLetzte7TageClass[conditionDanger])+ ") && "+ Not(FaellePro100kLetzte7TageClass[conditionUltra])+" && "+ Not(AnzahlFallTrendClass[conditionUltra]),
    conditionTooHigh: KontaktrisikoClass[conditionTooHigh]+" && "+ Not(FaellePro100kLetzte7TageClass[conditionUltra])+" && "+ Not(AnzahlFallTrendClass[conditionUltra])+" && "+ Not(FaellePro100kLetzte7TageClass[conditionDanger])+" && "+ Not(AnzahlFallTrendClass[conditionDanger]),
    conditionSerious: KontaktrisikoClass[conditionSerious]+" && "+ Not(FaellePro100kLetzte7TageClass[conditionUltra])+" && "+ Not(AnzahlFallTrendClass[conditionUltra])+" && "+ Not(FaellePro100kLetzte7TageClass[conditionDanger])+" && "+ Not(AnzahlFallTrendClass[conditionDanger]),
    conditionGood: KontaktrisikoClass[conditionGood]+" && "+ Not(FaellePro100kLetzte7TageClass[conditionUltra])+" && "+ Not(AnzahlFallTrendClass[conditionUltra])+" && "+ Not(FaellePro100kLetzte7TageClass[conditionDanger])+" && "+ Not(AnzahlFallTrendClass[conditionDanger]),
    conditionSafe: KontaktrisikoClass[conditionSafe]+" && "+ Not(FaellePro100kLetzte7TageClass[conditionUltra])+" && "+ Not(AnzahlFallTrendClass[conditionUltra])+" && "+ Not(FaellePro100kLetzte7TageClass[conditionDanger])+" && "+ Not(AnzahlFallTrendClass[conditionDanger]),
}

print(LandkreisClass)

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
    # result = result + conditionalStyles(FaellePro100kLetzte7TageClass, ['FaellePro100kLetzte7Tage'])
    # result = result + conditionalStyles(AnzahlFallTrendClass, ['AnzahlFallTrend'])
    # result = result + conditionalStyles(KontaktrisikoClass, ['Kontaktrisiko'])
    # result = result + conditionalStyles(LandkreisClass, ['Landkreis'])
    result = result + conditionalStyles(FaellePro100kLetzte7TageClass, 'InzidenzFallNeu-7-Tage')
    result = result + conditionalStyles(AnzahlFallTrendClass, 'InzidenzFallNeu-7-Tage-Trend-Spezial')
    result = result + conditionalStyles(KontaktrisikoClass, 'Kontaktrisiko')
    result = result + conditionalStyles(LandkreisClass, 'Landkreis')
    result = result + conditionalStyles(FaellePro100kPrognoseClass, 'InzidenzFallNeu-Prognose-4-Wochen')
    result = result + conditionalStyles(FaellePro100kPrognose2Class, 'InzidenzFallNeu-Prognose-8-Wochen')

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
        " die Gesamtzahlen identisch mit den offiziell vom RKI veröffentlichten Zahlen sein. Die neue Datenpipeline ist ganz frisch und kann noch Fehler enthalten, wobei die Fallzahlen eingentlich "
        " alle ziemlich gut aussehen."
        "", className=bodyClass),
    html.P(
        "Ansonsten gibt es noch als erstes kleines neues, unübersehbares Feature: Condition Ultra beziehungsweise violett. Sie markiert im Prinzip"
        " Inzidenzen über 100. Des weiteren gibt es zwei wesentliche Unterschiede in der Berechnung der Zahlen:"
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
    html.P([html.A(html.Span("Angereicherte Ursprungsdaten als .csv herunterladen", className=bodyClass),
                  href=fullDataURL),
            " (Kombination der RKI/NPGEO-Daten seit 29.4.2020 mit Eingangszeitstempeln und anderen zusätzlichen Feldern versehen)"]),
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
 "RKI ignoriert %" ist der Prozentsatz an ignorierten Fällen. Ein hohe Prozentsatz ist ein Indikator dafür, dass die
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
 beim RKI als Fall in der offiziellen Statistik auf Bundesebene. Hierbei wird angezeigt: Der Mittelwert aller Verzögerungen 
 (Summe/Anzahl), der Median (ca. die Hälfte der Verzögerungen liegt unter dem Wert, Hälfte darüber),
  die Standardabweichung (durchschnittliche Abweichung vom Mittel) angezeigt sowie die Zahl der Fälle,
  die in die Berechnung eingegangen sind.
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
