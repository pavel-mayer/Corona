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
import socket

print("Running on host '{}'".format(socket.gethostname()))

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
    lastnewCaseOnDay=fullTable[:,'newCaseOnDay'].max()[0,0]
    print("File stats: lastDay {} lastnewCaseOnDay {} cases {} dead {}".format(lastDay, lastnewCaseOnDay, cases, dead))

    newTable=fullTable[:,dt.f[:].extend({"erkMeldeDelay": dt.f.MeldeDay-dt.f.RefDay})]
    #print(newTable.keys())

    #dt.by(dt.f.Bundesland)]
    alldays=fullTable[:,
              [dt.sum(dt.f.AnzahlFall),
               dt.sum(dt.f.FaellePro100k),
               dt.sum(dt.f.AnzahlTodesfall),
               dt.sum(dt.f.TodesfaellePro100k),
               dt.mean(dt.f.Bevoelkerung),
               dt.max(dt.f.MeldeDay),
               dt.first(dt.f.LandkreisTyp),
               dt.first(dt.f.Bundesland)],
    dt.by(dt.f.Landkreis)]

    last7days=fullTable[dt.f.newCaseOnDay>lastDay-7,:][:,
              [dt.sum(dt.f.AnzahlFall),
               dt.sum(dt.f.FaellePro100k),
               dt.sum(dt.f.AnzahlTodesfall),
               dt.sum(dt.f.TodesfaellePro100k)],
    dt.by(dt.f.Landkreis)]
    last7days.names=["Landkreis","AnzahlFallLetzte7Tage","FaellePro100kLetzte7Tage","AnzahlTodesfallLetzte7Tage",
                     "TodesfaellePro100kLetzte7Tage"]
    last7days[dt.f.AnzahlFallLetzte7Tage <0, "AnzahlFallLetzte7Tage"] = 0
    last7days[dt.f.FaellePro100kLetzte7Tage <0, "FaellePro100kLetzte7Tage"] = 0
    last7days[dt.f.AnzahlTodesfallLetzte7Tage <0, "AnzahlTodesfallLetzte7Tage"] = 0
    last7days[dt.f.TodesfaellePro100kLetzte7Tage <0, "TodesfaellePro100kLetzte7Tage"] = 0

    lastWeek7days=fullTable[(dt.f.newCaseOnDay > lastDay-14) & (dt.f.newCaseOnDay<=lastDay-7),:][:,
              [dt.sum(dt.f.AnzahlFall),
               dt.sum(dt.f.FaellePro100k),
               dt.sum(dt.f.AnzahlTodesfall),
               dt.sum(dt.f.TodesfaellePro100k)],
       dt.by(dt.f.Landkreis)]
    #lastWeek7days[dt.f[1:] < 0, dt.f[1:]] = 0
    lastWeek7days.names=["Landkreis","AnzahlFallLetzte7TageDavor","FaellePro100kLetzte7TageDavor",
                         "AnzahlTodesfallLetzte7TageDavor","TodesfaellePro100kLetzte7TageDavor"]
    lastWeek7days[dt.f.AnzahlFallLetzte7TageDavor <0, "AnzahlFallLetzte7TageDavor"] = 0
    lastWeek7days[dt.f.FaellePro100kLetzte7TageDavor <0, "FaellePro100kLetzte7TageDavor"] = 0
    lastWeek7days[dt.f.AnzahlTodesfallLetzte7TageDavor <0, "AnzahlTodesfallLetzte7TageDavor"] = 0
    lastWeek7days[dt.f.TodesfaellePro100kLetzte7TageDavor <0, "TodesfaellePro100kLetzte7TageDavor"] = 0

    allDaysExt0 = merge(alldays, last7days, "Landkreis")
    allDaysExt1 = merge(allDaysExt0, lastWeek7days, "Landkreis")

    Rw = dt.f.AnzahlFallLetzte7Tage/dt.f.AnzahlFallLetzte7TageDavor

    allDaysExt2=allDaysExt1[:,dt.f[:].extend({"AnzahlFallTrend":  Rw})]
    allDaysExt3=allDaysExt2[:,dt.f[:].extend({"FaellePro100kTrend": dt.f.FaellePro100kLetzte7Tage-dt.f.FaellePro100kLetzte7TageDavor})]
    allDaysExt4=allDaysExt3[:,dt.f[:].extend({"TodesfaellePro100kTrend": dt.f.TodesfaellePro100kLetzte7Tage-dt.f.TodesfaellePro100kLetzte7TageDavor})]

    allDaysExt5=allDaysExt4[:,dt.f[:].extend({"Kontaktrisiko": dt.f.Bevoelkerung/6.25/((dt.f.AnzahlFallLetzte7Tage+dt.f.AnzahlFallLetzte7TageDavor)*Rw)})]
    allDaysExt6 = allDaysExt5[:, dt.f[:].extend({"LetzteMeldung": lastDay - dt.f.MeldeDay})]
    allDaysExt6b = allDaysExt6[:, dt.f[:].extend({"LetzteMeldungNeg": dt.f.MeldeDay - lastDay})]

    allDaysExt6b[dt.f.Kontaktrisiko * 2 == dt.f.Kontaktrisiko, "Kontaktrisiko"] = 999999

    sortedByRisk = allDaysExt6b.sort(["Kontaktrisiko","LetzteMeldung","FaellePro100k"])
    #print(sortedByRisk)
    allDaysExt=sortedByRisk[:,dt.f[:].extend({"Rang": 0})]
    allDaysExt[:,"Rang"]=np.arange(1,allDaysExt.nrows+1)
    #print(allDaysExt)


    print("Column names frame order:",list(enumerate(allDaysExt.names)))

    data=allDaysExt.to_pandas()
    return data

def makeColumns():
    desiredOrder = [
        ('Rang', ['Risiko', 'Rang'], 'numeric', FormatInt),
        ('Kontaktrisiko', ['Risiko', 'Risiko 1:N'], 'numeric', FormatInt),
        ('Landkreis', ['Kreis', 'Name'], 'text', Format()),
        ('Bundesland', ['Kreis', 'Bundesland'], 'text', Format()),
        ('LandkreisTyp', ['Kreis', 'Art'], 'text', Format()),
        ('Bevoelkerung', ['Kreis', 'Einwohner'], 'numeric', FormatInt),
        ('LetzteMeldungNeg', ['Kreis', 'Letze Meldung'], 'numeric', FormatInt),
        ('AnzahlFallTrend', ['Fälle', 'Rw'], 'numeric', FormatFixed2),
        ('AnzahlFallLetzte7Tage', ['Fälle', 'letzte Woche'], 'numeric', FormatInt),
        ('AnzahlFallLetzte7TageDavor', ['Fälle', 'vorletzte Woche'], 'numeric', FormatInt),
        ('AnzahlFall', ['Fälle', 'total'], 'numeric', FormatInt),
        ('FaellePro100kLetzte7Tage', ['Fälle je 100000', 'letzte Woche'], 'numeric', FormatFixed1),
        ('FaellePro100kLetzte7TageDavor', ['Fälle je 100000', 'vorletzte Woche'], 'numeric', FormatFixed1),
        ('FaellePro100kTrend', ['Fälle je 100000', 'Differenz'], 'numeric', FormatFixed1),
        ('FaellePro100k', ['Fälle je 100000', 'total'], 'numeric', FormatFixed1),
        ('AnzahlTodesfallLetzte7Tage', ['Todesfälle', 'letzte Woche'], 'numeric', FormatInt),
        ('AnzahlTodesfallLetzte7TageDavor', ['Todesfälle', 'vorletzte Woche'], 'numeric', FormatInt),
        ('AnzahlTodesfall', ['Todesfälle', 'total'], 'numeric', FormatInt),
        ('TodesfaellePro100kLetzte7Tage', ['Todesfälle je 100000', 'letzte Woche'], 'numeric', FormatFixed2),
        ('TodesfaellePro100kLetzte7TageDavor', ['Todesfälle je 100000', 'vorletzte Woche'], 'numeric', FormatFixed2),
        ('TodesfaellePro100kTrend', ['Todesfälle je 100000', 'Differenz'], 'numeric', FormatFixed2),
        ('TodesfaellePro100k', ['Todesfälle je 100000', 'total'], 'numeric', FormatFixed2),
    ]

    orderedCols, orderedNames, orderedTypes, orderFormats = zip(*desiredOrder)
    #orderedIndices = np.array(orderedIndices)+1
    #print(orderedIndices)

    columns = [{'name': L1, 'id': L2, 'type':L3, 'format':L4} for (L1,L2,L3,L4) in zip(orderedNames,orderedCols,orderedTypes,orderFormats)]
    #print("columns=",columns)
    return columns


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
columns = makeColumns()

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
    ]
    result = result + conditionalStyles(FaellePro100kLetzte7TageClass, ['FaellePro100kLetzte7Tage'])
    result = result + conditionalStyles(AnzahlFallTrendClass, ['AnzahlFallTrend'])
    result = result + conditionalStyles(KontaktrisikoClass, ['Kontaktrisiko'])
    result = result + conditionalStyles(LandkreisClass, ['Landkreis'])

    return result

cs_data = make_style_data_conditional()
#print("cs_data", cs_data)
#pretty(cs_data)

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
            'if': {'column_id': 'Landkreis'},
            'textAlign': 'left'
        },

    ],
    style_data_conditional = make_style_data_conditional()

)

def readExplanation():
    with open('explainer.md', 'r') as file:
        data = file.read()
    return data

appDate = os.path.getmtime("app.py")
print(appDate)
appDateStr=cd.dateTimeStrFromTime(appDate)
print(appDateStr)

h_header = html.Header(
    style={
        'backgroundColor': colors['background'],
    },
    children=[
        html.H1(className="app-header", children="COVID Risiko Deutschland nach Landkreisen", style={'color': colors['text']}),
        html.H4(className="app-header-date",
                children="Datenstand: {} 00:00 Uhr (wird täglich gegen Mittag aktualisiert)".format(dataVersionDate),
                style={'color': colors['text']}),
        html.H4(className="app-header-date",
                children="Softwarestand: {} (UTC), Version 0.9.4".format(appDateStr),
                style={'color': colors['text']})
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

introClass="intro"
bodyClass="bodyText"
bodyLink="bodyLink"

h_Hinweis=html.P([
    html.Span("Hinweis:", className=introClass),
    html.Span(" Dies ist eine privat betriebene Seite im Testbetrieb, für die Richtigkeit der Berechnung und der Ergebnisse "
              "gibt es keine Gewähr. Im Zweifel mit den ", className=bodyClass),
    html.A("offiziellen Zahlen des RKI abgleichen.",
           href="https://experience.arcgis.com/experience/478220a4c454480e823b17327b2bf1d4/page/page_1/",
           className=bodyLink
           ),
    html.P("Das System ist brandneu, kann unentdeckte, subtile Fehler machen, und auch die Berechnung des Rangs kann sich durch Updates der Software derzeit noch verändern.", className=bodyClass),
    html.Span(" Generell gilt: Die Zahlen sind mit Vorsicht zu genießen. Auf Landkreisebene sind die Zahlen niederig, eine Handvoll neue Fälle kann viel ausmachen. Auch können hohe Zahlen Folge eines"
              " Ausbruch in einer Klinik, einem Heim oder einer Massenunterkunft sein und nicht repräsentativ für die"
              " Verteilung in der breiten Bevölkerung. Andererseits ist da noch die Dunkelziffer, die hier mit 6,25"
              " angenommen wird. (siehe Risiko 1:N) Es laufen also viel mehr meist symptomlose Infizierte umher als Fälle registriert"
              " sind. Und fast immer gilt: Steigen die Zahlen, ist es nicht unter Kontrolle.", className=bodyClass),
    html.P(
        " Und ja, Ranglisten sind unfair, aber nachdem die Zahlen kleiner werden, ist der Blick in die Regionen umso"
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
            " Die Daten können aber nach jeder Spalte sortiert und gefilter werden, siehe 'Benutzung'."
            " Anhand der Ampelfarbgebung läßt sich durch Scrollen schnell ein Überblick über viele"
            " Landkreise und die Unterschiede zwischen ihnen verschaffen.",
            className=bodyClass),
])

h_About=html.P([
    html.Span("Der Autor über sich:", className=introClass),
    html.Span(" Bin weder Webentwickler noch Virologe noch Statisker, aber habe mich in den letzten Wochen sehr"
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
    html.Span("Benutzung:", className=introClass),
    html.P(
        "Durch Klicken auf die kleinen Pfeilsymbole im Kopf einer Spalte kann die Tabelle auf- oder absteigend sortiert werden.",
        className=bodyClass),

    html.P("Interessant ist es, nach Gesamtanzahl von Fällen  zu sortieren und dann zu sehen,"
           "wo sich ehemals stark betroffene Gebiete so auf die Liste verteilen.", className=bodyClass),

    html.P("Im leeren Feld über der Spalte kann ein Filter eingeben werden, z.B. Teil des Namens des Kreises. "
           "Mit Audrücken wie <10 oder >50 können Datensätzte ausgefiltert werden, die bestimmte Werte in der Spalte "
           "über- oder unterschreiten. Einfach Wert eingeben und <Return> drücken. Eingabe löschen, um Filter zu entfernen."
           , className=bodyClass),
])

h_BedeutungSpaltenHead= html.Span("Bedeutung der Spalten:", className=introClass)

h_BedeutungSpaltenIntro=html.Span("Am aussagekröftigsten sind Werte je 100.000 Einwohner, und da sind derzeit praktisch"
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

h_RwDef = makeDefinition(h_Rw,
'''
 ist ein wöchentlicher Reproduktionsfaktor. Er ist das Verhältnis aller Fälle der letzten 7 Tage gegenüber den 7
 Tagen davor. Diese Zahl "schlägt" etwa doppelt so stark aus wie der "normale" Reproduktionsfaktor, aber über 1.0 heißt
 auch hier Ausbreitung und unter 1.0 Rückgang. Ein Wert von 2 bedeutet, dass in der letzten Woche doppelt so viele
 Fälle gemeldet wurden wie in der vorletzten Woche, ein Wert von 0,5 bedeutet nur halb so viele neue Fälle. 
 Dieser wöchentlicher Reproduktionsfaktor vermeidet auch wochentagsbedingte Schwankungen und kommt ohne Annahmen wie
 Dauer des seriellen Intervalls aus und ist leicht nachvollziehbar.             
''')

h_Risiko=makeDefinition("Risiko 1:N/Rang",
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
    html.Li(["N = Bevölkerung / Dunkelzifferfaktor / ([Anzahl der Fälle in den letzten 2 Wochen] *",h_Rw,")"]),
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
                 "Wer kürzlich da war und ungeschützte Kontakte hatte, ist mit einer Wahrscheinlichkeit von 1:N infiziert."
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
    h_table
])

if __name__ == '__main__':
    debugFlag = socket.gethostname() == 'pavlator.local'
    app.run_server(host='0.0.0.0', port=1024,debug=debugFlag)
