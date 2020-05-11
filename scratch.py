import geopandas
import geoplot
import geoplot.crs as gcrs
import matplotlib.pyplot as plt
import numpy as np
import datatable as dt
import plotly.graph_objects as go
import json

def pretty(jsonmap):
    print(json.dumps(jsonmap, sort_keys=False, indent=4, separators=(',', ': ')))

fullTable = dt.fread("full-latest.csv")
print(fullTable.keys())
cases = fullTable[:,'AnzahlFall'].sum()[0,0]
dead = fullTable[:,'AnzahlTodesfall'].sum()[0,0]

lastDay=fullTable[:,'MeldeDay'].max()[0,0]
print("lastDay {} cases {} dead{}".format(lastDay, cases, dead))

newTable=fullTable[:,dt.f[:].extend({"erkMeldeDelay": dt.f.MeldeDay-dt.f.RefDay})]
#print(newTable.keys())


#dt.by(dt.f.Bundesland)]
alldays=fullTable[:,
          [dt.sum(dt.f.AnzahlFall),
           dt.sum(dt.f.FaellePro100k),
           dt.sum(dt.f.AnzahlTodesfall),
           dt.sum(dt.f.TodesfaellePro100k)],
   dt.by(dt.f.Landkreis)]

last7days=fullTable[dt.f.newCaseOnDay>lastDay-7,:][:,
          [dt.sum(dt.f.AnzahlFall),
           dt.sum(dt.f.FaellePro100k),
           dt.sum(dt.f.AnzahlTodesfall),
           dt.sum(dt.f.TodesfaellePro100k)],
   dt.by(dt.f.Landkreis)]
last7days.names=["Landkreis","AnzahlFallLetzte7Tage","FaellePro100kLetzte7Tage","AnzahlTodesfallLetzte7Tage","TodesfaellePro100kLetzte7Tage"]

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

def reorder(desiredOrder):
    result = 0
    return result

lastWeek7days=fullTable[(dt.f.newCaseOnDay > lastDay-14) & (dt.f.newCaseOnDay<=lastDay-7),:][:,
          [dt.sum(dt.f.AnzahlFall),
           dt.sum(dt.f.FaellePro100k),
           dt.sum(dt.f.AnzahlTodesfall),
           dt.sum(dt.f.TodesfaellePro100k)],
   dt.by(dt.f.Landkreis)]
lastWeek7days.names=["Landkreis","AnzahlFallLetzte7TageDavor","FaellePro100kLetzte7TageDavor","AnzahlTodesfallLetzte7TageDavor","TodesfaellePro100kLetzte7TageDavor"]

allDaysExt0 = merge(alldays, last7days, "Landkreis")
allDaysExt = merge(allDaysExt0, lastWeek7days, "Landkreis")

print(list(enumerate(allDaysExt.names)))
desiredOrder = [(0, 'Landkreis', 'Kreis'),
                (1, 'AnzahlFall', 'Fälle'),
                (5, 'AnzahlFallLetzte7Tage', 'Fälle letzte Woche'),
                (9, 'AnzahlFallLetzte7TageDavor','Fälle vorletze Woche'),
                (2, 'FaellePro100k','Fälle je 100000'),
                (6, 'FaellePro100kLetzte7Tage','Fälle je 100000 letzte Woche'),
                (10, 'FaellePro100kLetzte7TageDavor', 'Fälle je 100000 vorletzte Woche'),
                (3, 'AnzahlTodesfall', 'Todesfälle'),
                (7, 'AnzahlTodesfallLetzte7Tage', 'Todesfälle letzte Woche'),
                (11, 'AnzahlTodesfallLetzte7TageDavor', 'Todesfälle vorletze Woche'),
                (4, 'TodesfaellePro100k', 'Todesfälle je 100000'),
                (8, 'TodesfaellePro100kLetzte7Tage', 'Todesfälle je 100000 letzte Woche'),
                (12, 'TodesfaellePro100kLetzte7TageDavor', 'Todesfälle je 100000 vorletzte Woche')]

orderedIndices, orderedCols, orderedNames = zip(*desiredOrder)
orderedIndices = np.array(orderedIndices)+1
print(orderedIndices)
fig = go.Figure(data=[go.Table(
    #columnorder=[0,1,5],
    #columnwidth=[80, 400],

    header=dict(values=orderedNames,
                line_color='darkslategray',
                fill_color='lightskyblue',
                align='left'),
    cells=dict(values=allDaysExt.to_list(),
               line_color='darkslategray',
               fill_color='lightcyan',
               align='left'))
])

#fig.update_layout(width=500, height=300)
fig.show()
#pframe=last7days.to_pandas()
#pframe.plot()
exit(0)

slices = fullTable[:,['AnzahlFall','AnzahlTodesfall']].sum()
print(slices)
slices2 = fullTable[dt.f.MeldeDay>=0,:][:,[dt.sum(dt.f.AnzahlFall),dt.sum(dt.f.AnzahlTodesfall)],dt.by(dt.f.MeldeDay)]
print(slices2)
print(slices2.to_dict())


Bundeslaender = geopandas.read_file('Landkreise/landkreise-in-germany.geojson')

print(Bundeslaender.keys())
print(Bundeslaender.head())
print(Bundeslaender.total_bounds)

#new_bounds = np.multiply(Bundeslaender.total_bounds,[zoom,1/zoom,zoom,1/zoom])
#print(new_bounds)

ax = geoplot.polyplot(Bundeslaender, figsize=(8, 8), linewidth=0.3,edgecolor='white', facecolor='lightgray',
                 projection=gcrs.WebMercator())

plt.show()

exit(0)

fig = go.Figure(data=[go.Table(
    header=dict(values=['A Scores', 'B Scores'],
                line_color='darkslategray',
                fill_color='lightskyblue',
                align='left'),
    cells=dict(values=[[100, 90, 80, 90], # 1st column
                       [95, 85, 75, 95]], # 2nd column
               line_color='darkslategray',
               fill_color='lightcyan',
               align='left'))
])

fig.update_layout(width=500, height=300)
fig.show()