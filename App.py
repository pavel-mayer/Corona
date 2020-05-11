#!/usr/bin/env python3.6

# Quick hack to browse RKI-NPGEO-Data, Pavel Mayer 2020,
# License: Use freely at your own risk.

# pip install Click==7.0 Flask==1.1.1 itsdangerous==1.1.0 Jinja2==2.10.3 MarkupSafe==1.1.1 uWSGI==2.0.18 Werkzeug==0.16.0
# pip install: dash pandas datatable feather-format


import os
import flask
from flask import render_template

import dash
import dash_table
import pandas as pd
import numpy as np
import datatable as dt
import json
import dash_table.FormatTemplate as FormatTemplate
from dash_table.Format import Format, Scheme, Sign, Symbol

def pretty(jsonmap):
    print(json.dumps(jsonmap, sort_keys=False, indent=4, separators=(',', ': ')))

def merge(largerTable, smallerTable, keyFieldName):
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

FormatFixed1 = Format(
                precision=1,
                scheme=Scheme.fixed,
                symbol=Symbol.no,
            )
FormatFixed2 = Format(
                precision=2,
                scheme=Scheme.fixed,
                symbol=Symbol.no,
            )

FormatInt = Format(
                precision=0,
                scheme=Scheme.fixed,
                symbol=Symbol.no,
#                symbol_suffix=u'˚F'
            )

def loadAndProcessData(dataFilename):
    print("Loading "+dataFilename)

    fullTable = dt.fread(dataFilename)
    print("Loading done loading table from ‘"+dataFilename+"‘, keys:")
    print(fullTable.keys())
    cases = fullTable[:,'AnzahlFall'].sum()[0,0]
    dead = fullTable[:,'AnzahlTodesfall'].sum()[0,0]

    lastDay=fullTable[:,'MeldeDay'].max()[0,0]
    print("File stats: lastDay {} cases {} dead {}".format(lastDay, cases, dead))

    newTable=fullTable[:,dt.f[:].extend({"erkMeldeDelay": dt.f.MeldeDay-dt.f.RefDay})]
    #print(newTable.keys())

    #dt.by(dt.f.Bundesland)]
    alldays=fullTable[:,
              [dt.sum(dt.f.AnzahlFallPos),
               dt.sum(dt.f.FaellePro100k),
               dt.sum(dt.f.AnzahlTodesfallPos),
               dt.sum(dt.f.TodesfaellePro100k),
               dt.mean(dt.f.Bevoelkerung)],
       dt.by(dt.f.LandkreisName)]

    last7days=fullTable[dt.f.newCaseOnDay>lastDay-7,:][:,
              [dt.sum(dt.f.AnzahlFallPos),
               dt.sum(dt.f.FaellePro100k),
               dt.sum(dt.f.AnzahlTodesfallPos),
               dt.sum(dt.f.TodesfaellePro100k)],
       dt.by(dt.f.LandkreisName)]
    last7days.names=["LandkreisName","AnzahlFallLetzte7Tage","FaellePro100kLetzte7Tage","AnzahlTodesfallLetzte7Tage","TodesfaellePro100kLetzte7Tage"]

    ## todo: increase when more data here
    lastWeek7days=fullTable[(dt.f.newCaseOnDay > lastDay-13) & (dt.f.newCaseOnDay<=lastDay-6),:][:,
              [dt.sum(dt.f.AnzahlFallPos),
               dt.sum(dt.f.FaellePro100k),
               dt.sum(dt.f.AnzahlTodesfallPos),
               dt.sum(dt.f.TodesfaellePro100k)],
       dt.by(dt.f.LandkreisName)]
    lastWeek7days.names=["LandkreisName","AnzahlFallLetzte7TageDavor","FaellePro100kLetzte7TageDavor","AnzahlTodesfallLetzte7TageDavor","TodesfaellePro100kLetzte7TageDavor"]

    allDaysExt0 = merge(alldays, last7days, "LandkreisName")
    allDaysExt1 = merge(allDaysExt0, lastWeek7days, "LandkreisName")

    Rw = dt.f.AnzahlFallLetzte7Tage/dt.f.AnzahlFallLetzte7TageDavor

    allDaysExt2=allDaysExt1[:,dt.f[:].extend({"AnzahlFallTrend":  Rw})]
    allDaysExt3=allDaysExt2[:,dt.f[:].extend({"FaellePro100kTrend": dt.f.FaellePro100kLetzte7Tage-dt.f.FaellePro100kLetzte7TageDavor})]
    allDaysExt4=allDaysExt3[:,dt.f[:].extend({"TodesfaellePro100kTrend": dt.f.TodesfaellePro100kLetzte7Tage-dt.f.TodesfaellePro100kLetzte7TageDavor})]

    allDaysExt=allDaysExt4[:,dt.f[:].extend({"Kontaktrisiko": dt.f.Bevoelkerung/6.25/((dt.f.AnzahlFallLetzte7Tage+dt.f.AnzahlFallLetzte7TageDavor)*Rw)})]

    print("Column names frame order:",list(enumerate(allDaysExt.names)))

    data=allDaysExt.to_pandas()
    return data

# [(0, 'Landkreis'), (1, 'AnzahlFall'), (2, 'FaellePro100k'), (3, 'AnzahlTodesfall'), (4, 'TodesfaellePro100k'),
# (5, 'Bevoelkerung'), (6, 'AnzahlFallLetzte7Tage'), (7, 'FaellePro100kLetzte7Tage'), (8, 'AnzahlTodesfallLetzte7Tage'),
# (9, 'TodesfaellePro100kLetzte7Tage'), (10, 'AnzahlFallLetzte7TageDavor'), (11, 'FaellePro100kLetzte7TageDavor'),
# (12, 'AnzahlTodesfallLetzte7TageDavor'),(13, 'TodesfaellePro100kLetzte7TageDavor'), (14, 'AnzahlFallTrend'),
# (15, 'FaellePro100kTrend'), (16, 'TodesfaellePro100kTrend'), (17, 'Kontaktrisiko')]

def makeColumns():
    desiredOrder = [(0, 'LandkreisName', ['Kreis','Name'],'text',Format()),
                    (5, 'Bevoelkerung', ['Kreis','Einwohner'],'numeric',FormatInt),
                    (17, 'Kontaktrisiko', ['Kreis','Risiko 1:N'],'numeric',FormatInt),
                    (1, 'AnzahlFallPos', ['Fälle','total'],'numeric',FormatInt),
                    (6, 'AnzahlFallLetzte7Tage', ['Fälle','letzte Woche'] ,'numeric',FormatInt),
                    (14, 'AnzahlFallTrend', ['Fälle','R'] ,'numeric',FormatFixed2),
                    (10, 'AnzahlFallLetzte7TageDavor',['Fälle','vorletzte Woche'],'numeric',FormatInt),
                    (2, 'FaellePro100k',['Fälle je 100000','total'],'numeric',FormatFixed1),
                    (15, 'FaellePro100kTrend',['Fälle je 100000','Differenz'] ,'numeric',FormatFixed1),
                    (7, 'FaellePro100kLetzte7Tage',['Fälle je 100000','letzte Woche'] ,'numeric',FormatFixed1),
                    (11, 'FaellePro100kLetzte7TageDavor', ['Fälle je 100000','vorletzte Woche'],'numeric',FormatFixed1),
                    (3, 'AnzahlTodesfallPos', ['Todesfälle','total'],'numeric',FormatInt),
                    (8, 'AnzahlTodesfallLetzte7Tage', ['Todesfälle','letzte Woche'],'numeric',FormatInt),
                    (12, 'AnzahlTodesfallLetzte7TageDavor', ['Todesfälle','vorletzte Woche'],'numeric',FormatInt),
                    (4, 'TodesfaellePro100k', ['Todesfälle je 100000','total'],'numeric',FormatFixed2),
                    (9, 'TodesfaellePro100kLetzte7Tage', ['Todesfälle je 100000','letzte Woche'],'numeric',FormatFixed2),
                    (16, 'TodesfaellePro100kTrend', ['Todesfälle je 100000','Differenz'],'numeric',FormatFixed2),
                    (13, 'TodesfaellePro100kLetzte7TageDavor', ['Todesfälle je 100000','vorletzte Woche'],'numeric',FormatFixed2)]

    orderedIndices, orderedCols, orderedNames, orderedTypes, orderFormats = zip(*desiredOrder)
    orderedIndices = np.array(orderedIndices)+1
    #print(orderedIndices)

    columns = [{'name': L1, 'id': L2, 'type':L3, 'format':L4} for (L1,L2,L3,L4) in zip(orderedNames,orderedCols,orderedTypes,orderFormats)]
    print("columns=",columns)
    return columns


server = flask.Flask(__name__)

@server.route('/')
def index():
    return 'Hello Covid Flask app'

app = dash.Dash(
    __name__,
    server=server,
    routes_pathname_prefix='/covid/'
)

fullTableFilename = "full-latest.csv"
cacheFilename = "data-cached.feather"

FORCE_REFRESH_CACHE = True

if FORCE_REFRESH_CACHE or not os.path.isfile(cacheFilename) or os.path.getmtime(fullTableFilename) > os.path.getmtime(cacheFilename) :
    dframe = loadAndProcessData(fullTableFilename)
    dframe.to_feather(cacheFilename)
else:
    print("Loading data cache from ‘"+cacheFilename+"‘")
    dframe = pd.read_feather(cacheFilename)

data = dframe.to_dict("records")
columns = makeColumns()
print("Loading done, creating Datatable")

app.layout = dash_table.DataTable(
    id='table',
    columns=columns,
    data=data,
    sort_action='native',
    page_size= 500,
    sort_by = [{"column_id": "Kontaktrisiko", "direction": "asc"}],
#    fixed_rows={ 'headers': True, 'data': 0 },
    style_cell={'textAlign': 'right',
                'padding': '5px',
                'backgroundColor': 'rgb(50, 50, 50)',
                'color': 'white'
    },
    style_cell_conditional=[
        {
            'if': {'column_id': 'LandkreisName'},
            'textAlign': 'left'
        }
    ],
    style_data_conditional=[
        {
            'if': {'row_index': 'odd'},
            'backgroundColor': 'rgb(70, 70, 70)'
        },
        ############################################################################
        {
            'if': {
                'filter_query': '{FaellePro100kLetzte7Tage} < 1',
                'column_id': ['FaellePro100kLetzte7Tage', 'LandkreisName']
            },
            'backgroundColor': 'green',
            'fontWeight': 'bold',
            'color': 'white'
        },
        {
            'if': {
                'filter_query': '{FaellePro100kLetzte7Tage} >= 1 && {FaellePro100kLetzte7Tage} < 5',
                'column_id': ['FaellePro100kLetzte7Tage', 'LandkreisName']
            },
            #            'backgroundColor': 'tomato',
            'fontWeight': 'bold',
            'color': 'lightgreen'
        },
        {
            'if': {
                'filter_query': '{FaellePro100kLetzte7Tage} > 10 && {FaellePro100kLetzte7Tage} < 20',
                'column_id': ['FaellePro100kLetzte7Tage','LandkreisName']
            },
            #            'backgroundColor': 'tomato',
            'fontWeight': 'bold',
            'color': 'yellow'
        },
        {
            'if': {
                'filter_query': '{FaellePro100kLetzte7Tage} > 20 && {FaellePro100kLetzte7Tage} < 50',
                'column_id': ['FaellePro100kLetzte7Tage','LandkreisName']
            },
            #            'backgroundColor': 'tomato',
            'fontWeight': 'bold',
            'color': 'tomato'
        },
        {
            'if': {
                'filter_query': '{FaellePro100kLetzte7Tage} > 50',
                'column_id': ['FaellePro100kLetzte7Tage','LandkreisName']
            },
            'fontWeight': 'bold',
            'backgroundColor': 'firebrick',
            'color': 'white'
        },

        ############################################################################

        {
            'if': {
                'filter_query': '{FaellePro100kTrend} > 0',
                'column_id': 'FaellePro100kTrend'
            },
            'fontWeight': 'bold',
            #'backgroundColor': 'tomato',
            'color': 'tomato'
        },
        ############################################################################
        {
            'if': {
                'filter_query': '{AnzahlFallTrend} > 1',
                'column_id': 'AnzahlFallTrend'
            },
            'fontWeight': 'bold',
            #'backgroundColor': 'tomato',
            'color': 'tomato'
        },
        {
            'if': {
                'filter_query': '{AnzahlFallTrend} > 0.9 && {AnzahlFallTrend} <= 1',
                'column_id': 'AnzahlFallTrend'
            },
            'fontWeight': 'bold',
            # 'backgroundColor': 'tomato',
            'color': 'yellow'
        },
        {
            'if': {
                'filter_query': '{AnzahlFallTrend} < 0.7',
                'column_id': 'AnzahlFallTrend'
            },
            'fontWeight': 'bold',
            # 'backgroundColor': 'tomato',
            'color': 'lightgreen'
        },
        ############################################################################
        {
            'if': {
                'filter_query': '{Kontaktrisiko} > 0 && {Kontaktrisiko} < 100',
                'column_id': ['Kontaktrisiko','LandkreisName']
            },
            'fontWeight': 'bold',
            'backgroundColor': 'firebrick',
            'color': 'white'
        },
        {
            'if': {
                'filter_query': '{Kontaktrisiko} >= 100 && {Kontaktrisiko} < 1000',
                'column_id': ['Kontaktrisiko','LandkreisName']
            },
            'fontWeight': 'bold',
            # 'backgroundColor': 'tomato',
            'color': 'tomato'
        },
        {
            'if': {
                'filter_query': '{Kontaktrisiko} >= 1000 && {Kontaktrisiko} < 2500',
                'column_id': ['Kontaktrisiko','LandkreisName']
            },
            'fontWeight': 'bold',
            # 'backgroundColor': 'tomato',
            'color': 'yellow'
        },
        {
            'if': {
                'filter_query': '{Kontaktrisiko} >= 5000 && {Kontaktrisiko} < 10000',
                'column_id': ['Kontaktrisiko','LandkreisName']
            },
            'fontWeight': 'bold',
            # 'backgroundColor': 'tomato',
            'color': 'lightgreen'
        },
        {
            'if': {
                'filter_query': '{Kontaktrisiko} > 10000',
                'column_id': ['Kontaktrisiko','LandkreisName']
            },
            'fontWeight': 'bold',
            'backgroundColor': 'green',
            'color': 'white'
        },

    ],
#    style_as_list_view = True,
    style_header={
        'backgroundColor': 'rgb(30, 30, 30)',
        'color': 'white',
        'fontWeight': 'bold',
#        'height': '100px',
        'whiteSpace': 'normal',
        'height': 'auto',
        'overflow-wrap': 'normal',
        'textAlign': 'center'
    },
    merge_duplicate_headers=True,
)

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=1024,debug=True)
