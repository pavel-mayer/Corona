#!/usr/bin/env python3.6

# Quick hack to browse RKI-NPGEO-Data, Pavel Mayer 2020,
# License: Use freely at your own risk.

# pip install Click==7.0 Flask==1.1.1 itsdangerous==1.1.0 Jinja2==2.10.3 MarkupSafe==1.1.1 uWSGI==2.0.18 Werkzeug==0.16.0
# pip install: dash pandas datatable feather-format

import locale

print("Locale:"+str(locale.getlocale()))
locale.setlocale(locale.LC_ALL, 'de_DE')
print("Locale set to:"+str(locale.getlocale()))

import cov_dates as cd

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
               dt.mean(dt.f.Bevoelkerung),
               dt.max(dt.f.MeldeDay),
               dt.first(dt.f.LandkreisTyp),
               dt.first(dt.f.Bundesland)],
    dt.by(dt.f.LandkreisName)]

    last7days=fullTable[dt.f.newCaseOnDay>lastDay-7,:][:,
              [dt.sum(dt.f.AnzahlFallPos),
               dt.sum(dt.f.FaellePro100k),
               dt.sum(dt.f.AnzahlTodesfallPos),
               dt.sum(dt.f.TodesfaellePro100k)],
    dt.by(dt.f.LandkreisName)]
    last7days.names=["LandkreisName","AnzahlFallLetzte7Tage","FaellePro100kLetzte7Tage","AnzahlTodesfallLetzte7Tage",
                     "TodesfaellePro100kLetzte7Tage"]

    lastWeek7days=fullTable[(dt.f.newCaseOnDay > lastDay-14) & (dt.f.newCaseOnDay<=lastDay-7),:][:,
              [dt.sum(dt.f.AnzahlFallPos),
               dt.sum(dt.f.FaellePro100k),
               dt.sum(dt.f.AnzahlTodesfallPos),
               dt.sum(dt.f.TodesfaellePro100k)],
       dt.by(dt.f.LandkreisName)]
    lastWeek7days.names=["LandkreisName","AnzahlFallLetzte7TageDavor","FaellePro100kLetzte7TageDavor",
                         "AnzahlTodesfallLetzte7TageDavor","TodesfaellePro100kLetzte7TageDavor"]

    allDaysExt0 = merge(alldays, last7days, "LandkreisName")
    allDaysExt1 = merge(allDaysExt0, lastWeek7days, "LandkreisName")

    Rw = dt.f.AnzahlFallLetzte7Tage/dt.f.AnzahlFallLetzte7TageDavor

    allDaysExt2=allDaysExt1[:,dt.f[:].extend({"AnzahlFallTrend":  Rw})]
    allDaysExt3=allDaysExt2[:,dt.f[:].extend({"FaellePro100kTrend": dt.f.FaellePro100kLetzte7Tage-dt.f.FaellePro100kLetzte7TageDavor})]
    allDaysExt4=allDaysExt3[:,dt.f[:].extend({"TodesfaellePro100kTrend": dt.f.TodesfaellePro100kLetzte7Tage-dt.f.TodesfaellePro100kLetzte7TageDavor})]

    allDaysExt5=allDaysExt4[:,dt.f[:].extend({"Kontaktrisiko": dt.f.Bevoelkerung/6.25/((dt.f.AnzahlFallLetzte7Tage+dt.f.AnzahlFallLetzte7TageDavor)*Rw)})]
    allDaysExt=allDaysExt5[:,dt.f[:].extend({"LetzteMeldung": lastDay - dt.f.MeldeDay})]

    print("Column names frame order:",list(enumerate(allDaysExt.names)))

    data=allDaysExt.to_pandas()
    return data

# [(0, 'Landkreis'), (1, 'AnzahlFall'), (2, 'FaellePro100k'), (3, 'AnzahlTodesfall'), (4, 'TodesfaellePro100k'),
# (5, 'Bevoelkerung'), (6, 'AnzahlFallLetzte7Tage'), (7, 'FaellePro100kLetzte7Tage'), (8, 'AnzahlTodesfallLetzte7Tage'),
# (9, 'TodesfaellePro100kLetzte7Tage'), (10, 'AnzahlFallLetzte7TageDavor'), (11, 'FaellePro100kLetzte7TageDavor'),
# (12, 'AnzahlTodesfallLetzte7TageDavor'),(13, 'TodesfaellePro100kLetzte7TageDavor'), (14, 'AnzahlFallTrend'),
# (15, 'FaellePro100kTrend'), (16, 'TodesfaellePro100kTrend'), (17, 'Kontaktrisiko')]
# Column names frame order: [(0, 'LandkreisName'), (1, 'AnzahlFallPos'), (2, 'FaellePro100k'), (3, 'AnzahlTodesfallPos'), (4, 'TodesfaellePro100k'), (5, 'Bevoelkerung'), (6, 'AnzahlFallLetzte7Tage'), (7, 'FaellePro100kLetzte7Tage'), (8, 'AnzahlTodesfallLetzte7Tage'), (9, 'TodesfaellePro100kLetzte7Tage'), (10, 'AnzahlFallLetzte7TageDavor'), (11, 'FaellePro100kLetzte7TageDavor'), (12, 'AnzahlTodesfallLetzte7TageDavor'), (13, 'TodesfaellePro100kLetzte7TageDavor'), (14, 'AnzahlFallTrend'), (15, 'FaellePro100kTrend'), (16, 'TodesfaellePro100kTrend'), (17, 'Kontaktrisiko')]
def makeColumns():
    desiredOrder = [('LandkreisName', ['Kreis','Name'],'text',Format()),
                    ('Bundesland', ['Kreis', 'Bundesland'], 'text', Format()),
                    ('LandkreisTyp', ['Kreis', 'Art'], 'text', Format()),
                    ('Bevoelkerung', ['Kreis','Einwohner'],'numeric',FormatInt),
                    ('LetzteMeldung', ['Kreis','Letze Meldung'],'numeric',FormatInt),
                    ('Kontaktrisiko', ['Kreis','Risiko 1:N'],'numeric',FormatInt),
                    ('AnzahlFallTrend', ['Fälle','Rw'] ,'numeric',FormatFixed2),
                    ('AnzahlFallLetzte7Tage', ['Fälle','letzte Woche'] ,'numeric',FormatInt),
                    ('AnzahlFallLetzte7TageDavor',['Fälle','vorletzte Woche'],'numeric',FormatInt),
                    ('AnzahlFallPos', ['Fälle','total'],'numeric',FormatInt),
                    ('FaellePro100kLetzte7Tage',['Fälle je 100000','letzte Woche'] ,'numeric',FormatFixed1),
                    ('FaellePro100kLetzte7TageDavor', ['Fälle je 100000','vorletzte Woche'],'numeric',FormatFixed1),
                    ('FaellePro100kTrend',['Fälle je 100000','Differenz'] ,'numeric',FormatFixed1),
                    ('FaellePro100k',['Fälle je 100000','total'],'numeric',FormatFixed1),
                    ('AnzahlTodesfallLetzte7Tage', ['Todesfälle','letzte Woche'],'numeric',FormatInt),
                    ('AnzahlTodesfallLetzte7TageDavor', ['Todesfälle','vorletzte Woche'],'numeric',FormatInt),
                    ('AnzahlTodesfallPos', ['Todesfälle','total'],'numeric',FormatInt),
                    ('TodesfaellePro100kLetzte7Tage', ['Todesfälle je 100000','letzte Woche'],'numeric',FormatFixed2),
                    ('TodesfaellePro100kLetzte7TageDavor', ['Todesfälle je 100000','vorletzte Woche'],'numeric',FormatFixed2),
                    ('TodesfaellePro100kTrend', ['Todesfälle je 100000','Differenz'],'numeric',FormatFixed2),
                    ('TodesfaellePro100k', ['Todesfälle je 100000', 'total'], 'numeric', FormatFixed2),
                    ]

    orderedCols, orderedNames, orderedTypes, orderFormats = zip(*desiredOrder)
    #orderedIndices = np.array(orderedIndices)+1
    #print(orderedIndices)

    columns = [{'name': L1, 'id': L2, 'type':L3, 'format':L4} for (L1,L2,L3,L4) in zip(orderedNames,orderedCols,orderedTypes,orderFormats)]
    print("columns=",columns)
    return columns


server = flask.Flask(__name__)

@server.route('/covid/Landkreise/about')
def index():
    return 'Hello Covid Flask app'

app = dash.Dash(
    __name__,
    server=server,
    routes_pathname_prefix='/covid/Landkreise/',
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
columns = makeColumns()

colors = {
    'background': 'rgb(50, 50, 50)',
    'text': 'white'
}

h_table = dash_table.DataTable(
    id='table',
    columns=columns,
    data=data,
    sort_action='native',
    filter_action='native',
    page_size= 500,
    sort_by = [{"column_id": "Kontaktrisiko", "direction": "asc"}],
#    fixed_rows={ 'headers': True, 'data': 0 },
    style_cell={'textAlign': 'right',
                'padding': '5px',
                'backgroundColor': colors['background'],
                'color': 'white',
    },
    style_data={
        'border-bottom': '1px solid '+colors['background'],
        'border-left': '1px solid rgb(128, 128, 128)',
        'border-right': '1px solid rgb(128, 128, 128)'
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
        'textAlign': 'center'
    },
    merge_duplicate_headers=True,
    style_cell_conditional=[
        {
            'if': {'column_id': 'LandkreisName'},
            'textAlign': 'left'
        },

    ],
    style_data_conditional=[
        # {
        #     'if': {'column_id': 'Kontaktrisiko'},
        #     'border-left': '3px solid blue',
        #     'border-right': '3px solid blue',
        # },
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
            #'fontWeight': 'bold',
            'color': 'white'
        },
        {
            'if': {
                'filter_query': '{FaellePro100kLetzte7Tage} >= 1 && {FaellePro100kLetzte7Tage} < 5',
                'column_id': ['FaellePro100kLetzte7Tage', 'LandkreisName']
            },
            #            'backgroundColor': 'tomato',
            #'fontWeight': 'bold',
            'color': 'lightgreen'
        },
        {
            'if': {
                'filter_query': '{FaellePro100kLetzte7Tage} > 10 && {FaellePro100kLetzte7Tage} < 20',
                'column_id': ['FaellePro100kLetzte7Tage','LandkreisName']
            },
            #            'backgroundColor': 'tomato',
            #'fontWeight': 'bold',
            'color': 'yellow'
        },
        {
            'if': {
                'filter_query': '{FaellePro100kLetzte7Tage} > 20 && {FaellePro100kLetzte7Tage} < 50',
                'column_id': ['FaellePro100kLetzte7Tage','LandkreisName']
            },
            #            'backgroundColor': 'tomato',
            #'fontWeight': 'bold',
            'color': 'tomato'
        },
        {
            'if': {
                'filter_query': '{FaellePro100kLetzte7Tage} > 50',
                'column_id': ['FaellePro100kLetzte7Tage','LandkreisName']
            },
            #'fontWeight': 'bold',
            'backgroundColor': 'firebrick',
            'color': 'white'
        },

        ############################################################################

        {
            'if': {
                'filter_query': '{FaellePro100kTrend} > 0',
                'column_id': 'FaellePro100kTrend'
            },
            #'fontWeight': 'bold',
            #'backgroundColor': 'tomato',
            'color': 'tomato'
        },
        ############################################################################
        {
            'if': {
                'filter_query': '{AnzahlFallTrend} > 1',
                'column_id': 'AnzahlFallTrend'
            },
            #'fontWeight': 'bold',
            #'backgroundColor': 'tomato',
            'color': 'tomato'
        },
        {
            'if': {
                'filter_query': '{AnzahlFallTrend} > 0.9 && {AnzahlFallTrend} <= 1',
                'column_id': 'AnzahlFallTrend'
            },
            #'fontWeight': 'bold',
            # 'backgroundColor': 'tomato',
            'color': 'yellow'
        },
        {
            'if': {
                'filter_query': '{AnzahlFallTrend} < 0.7',
                'column_id': 'AnzahlFallTrend'
            },
            #'fontWeight': 'bold',
            # 'backgroundColor': 'tomato',
            'color': 'lightgreen'
        },
        ############################################################################
        {
            'if': {
                'filter_query': '{Kontaktrisiko} > 0 && {Kontaktrisiko} < 100',
                'column_id': ['Kontaktrisiko','LandkreisName']
            },
            #'fontWeight': 'bold',
            'backgroundColor': 'firebrick',
            'color': 'white'
        },
        {
            'if': {
                'filter_query': '{Kontaktrisiko} >= 100 && {Kontaktrisiko} < 1000',
                'column_id': ['Kontaktrisiko','LandkreisName']
            },
            #'fontWeight': 'bold',
            # 'backgroundColor': 'tomato',
            'color': 'tomato'
        },
        {
            'if': {
                'filter_query': '{Kontaktrisiko} >= 1000 && {Kontaktrisiko} < 2500',
                'column_id': ['Kontaktrisiko','LandkreisName']
            },
            #'fontWeight': 'bold',
            # 'backgroundColor': 'tomato',
            'color': 'yellow'
        },
        {
            'if': {
                'filter_query': '{Kontaktrisiko} >= 5000 && {Kontaktrisiko} < 10000',
                'column_id': ['Kontaktrisiko','LandkreisName']
            },
            #'fontWeight': 'bold',
            # 'backgroundColor': 'tomato',
            'color': 'lightgreen'
        },
        {
            'if': {
                'filter_query': '{Kontaktrisiko} > 10000',
                'column_id': ['Kontaktrisiko','LandkreisName']
            },
            #'fontWeight': 'bold',
            'backgroundColor': 'green',
            'color': 'white'
        },

    ],

)

def readExplanation():
    with open('explainer.md', 'r') as file:
        data = file.read()
    return data

h_header = html.Header(
    style={
        'backgroundColor': colors['background'],
    },
    children=[
        html.H1(className="app-header", children="COVID Risiko Deutschland nach Landkreisen", style={'color': colors['text']}),
        html.H3(className="app-header-date", children="Datenstand: {} 00:00 Uhr".format(dataVersionDate), style={'color': colors['text']})
    ]
)

h_explanation = dcc.Markdown(
    readExplanation(),
    style={
        'backgroundColor': colors['background'],
        'color': 'white',
        'line-height:': '1.8',
    },
    dangerously_allow_html=True,
)

introClass="intro"
bodyClass="bodyText"
bodyLink="bodyLink"

h_Hinweis=html.P([
    html.Span("Hinweis:", className=introClass),
    html.Span("Dies ist eine privat betriebene Seite, für die Richtigkeit der Berechnung und der Ergebnisse "
              "übernehme ich keine Gewähr. Im Zweifel mit den ", className=bodyClass),
    html.A("offiziellen Zahlen des RKI abgleichen.",
           href="https://experience.arcgis.com/experience/478220a4c454480e823b17327b2bf1d4/page/page_1/",
           className=bodyLink
           ),
    html.Span(" Generell gilt: Die Zahlen sind mit Vorsicht zu genießen, inbssondere können hohe Zahlen auch Folge eines "
              "Ausbruch in einer Klinik, einem Heim oder einer Massenunterkunft sein und nicht repräsentativ für die"
              "Verteilung in der breiten Bevölkerung. Andererseits ist da noch die Dunkelziffer. ", className=bodyClass),
])

h_Erlauterung=html.P([
    html.Span(children="Erläuterung:", className=introClass),
    html.Span(children="Diese Seite bereitet", className=bodyClass),
    html.A(children = "die RKI COVID19 Daten aus dem NPGEO-Corona-Hub",
              href="https://npgeo-corona-npgeo-de.hub.arcgis.com/datasets/dd4580c810204019a7b8eb3e0b329dd6_0",
              className=bodyLink),
    html.Span(children="in tabellarischer Form auf. Sie können einfach nach jeder Spalte sortiert werden,"
                            " indem das Symbol im Kopf der Spalte ggf. mehrfach angewählt wird.", className=bodyClass),
])

h_Benutzung = html.P([
    html.Span("Benutzung:", className=introClass),
    html.P(
        "Durch Klicken auf die kleinen Pfeilsymbole im Kopf einer Spalte kann die Tabelle auf- oder absteigend sortiert werden.",
        className=bodyClass),

    html.P("Interessant ist es, nach Gesamtanzahl von Fällen  zu sortieren und dann zu sehen,"
           "wo sich ehemals stark betroffene Gebiete so auf die Liste verteilen.", className=bodyClass),

    html.P("Im leeren Feld über der Spalte kann ein Filter eingeben werden, z.B. Teil des Namens des Kreises. "
           "Mit Audrücken wie <10 oder >50 können Datensätzte ausgefiltert werden, die bestimmte Werte in der Spalte "
           "über- oder unterschreiten."
           , className=bodyClass),
])

h_BedeutungSpaltenHead= html.Span("Bedeutung der Spalten:", className=introClass)

h_BedeutungSpaltenIntro=html.Span("Die Bedeutung der meisten Spalten ist aus ihrem Titel zu entnehmen, mit folgenden Ausnahmen:",
                   className=bodyClass)

h_BedeutungSpalten = html.P([h_BedeutungSpaltenHead,h_BedeutungSpaltenIntro])

def makeDefinition(value, definition):
    return html.Div([
        html.Span(children=value, className=introClass),
        html.Span(children=definition, className=bodyClass),
    ])

h_Rw = html.Span(["R", html.Sub("w")])

h_RwDef = makeDefinition(h_Rw,
'''
Dies ist eine Berechnung der Vermehrung oder Abnahme der Fälle einer Woche in der Folgewoche. Sie entspricht nicht genau
der normalen Corona-Reproduktionszahl und beträgt etwa das doppelte, setzt aber keine Annahme über das serielle Intervall
voraus und vermeidet wochentagsbedingte Schwankungen. EIn Wert von 2 bedeutet, dass in der letzten Woche doppelt so viele
Fälle gemeldet wurden wie in der Vorwoche, ein Wert von 0,5 dedeutet nur halb so viele neue Fälle.                
''')

h_Risiko=makeDefinition("Risiko 1:N",
"""
Die Zahl kann so interpretiert werden, dass jeweils eine von der Zahl dieser Personen ansteckend sein kann. 
Ist sie etwa 100, kann sich im Durchschnitt in jedem Bus oder Waggon ein Ansteckender befinden.
Sie berechnet sich wie folgt:  
""")

h_LetzteMeldung=makeDefinition("Letzte Meldung",
"""
 gibt an, vor wie viel Tagen die letzte Meldung erfolgt. 0 heisst, dass der Kreis am Tag des Datenstands Fälle 
gemeldet hat, 5 bedeutet, dass seit 5 Tagen keine Meldungen eingegangen sind.
""")


h_RisikoList=html.Ul([
    html.Li(["Bevölkerung / Dunkelzifferfaktor / ((Anzahl der Fälle in den letzten 2 Wochen) *",h_Rw,")"]),
    html.Li("Als Faktor für die Dunkelziffer wurde 6,25 gewählt, also auf 1 gemeldeten Infizierten werden 5,25 weitere vermutet"),
    html.Li("Als grobe Annäherung an die Zahl der Ansteckenden wurde die Summe der Fälle der letzten zwei Wochen gewählt"),
    html.Li("Die Zahl der aktuell Ansteckenden wird zudem für die Risikoberechnung hochgerechnet,"
            "indem die Entwicklung von der vorletzen Woche zur letzen Woche prozentual unverändert fortgeschrieben wird "
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

conditionDanger="conditionDanger"
conditionTooHigh="conditionTooHigh"
conditionSerious="conditionSerious"
conditionGood="conditionGood"
conditionSafe="conditionSafe"

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


h_BgFarbenList=html.Ul([
    html.Li([makeColorSpan("Rot: ",conditionDanger), "Lasset alle Hoffnung fahren. Die Situation ist praktisch ausser Kontrolle."
             "Möglichst zu Hause bleiben und außer Haus bestmögliche Schutzmassnahmen ergreifen. Gegend weiträumig meiden."
             "Wer kürzlich da war könnte infiziert sein."], style=LiStyle),
    html.Li([makeColorSpan("Grün: ",conditionSafe), "Einheimische da könnten völlig entspannt sein, wenn sie keinen "
             "ungeschützten Kontakt mit Fremden aus anderen Kreisen hätten. Würde da bleiben und niemanden rein lassen."], style=LiStyle),
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

h_Bedeutungen = html.Table([
        html.Tr([html.Th(h_BedeutungSpaltenHead), html.Td(h_BedeutungFarbenHead,style=cellStyle)]),
        html.Tr([html.Td(h_BedeutungSpaltenIntro), html.Td(h_BedeutungFarbenIntro,style=cellStyle)]),
        html.Tr([html.Td(h_RwDef), html.Td(h_TextFarben,style=cellStyle)]),
        html.Tr([html.Td([h_Risiko, h_RisikoList]), html.Td(h_BgtFarben,style=cellStyle)]),
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
    h_table
])

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=1024,debug=True)
