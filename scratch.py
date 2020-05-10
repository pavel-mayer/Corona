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

last7Landkreise = last7days[:, 'Landkreis'].to_list()[0]
last7Faelle = last7days[:, 'AnzahlFallLetzte7Tage'].to_list()[0]
last7daysDict=dict(zip(last7Landkreise, last7Faelle))

pretty(last7daysDict)

lastWeek7days=fullTable[(dt.f.newCaseOnDay > lastDay-14) & (dt.f.newCaseOnDay<=lastDay-7),:][:,
          [dt.sum(dt.f.AnzahlFall),
           dt.sum(dt.f.FaellePro100k),
           dt.sum(dt.f.AnzahlTodesfall),
           dt.sum(dt.f.TodesfaellePro100k)],
   dt.by(dt.f.Landkreis)]
lastWeek7days.names=["Landkreis","AnzahlFallLetzte7TageDavor","FaellePro100kLetzte7TageDavor","AnzahlTodesfallLetzte7TageDavor","TodesfaellePro100kLetzte7TageDavor"]


#allDaysExt = alldays[:,dt.f[:].extend({"AnzahlFallLetzte7Tage": last7daysDict[dt.f.Landkreis] if dt.f.Landkreis in last7daysDict.keys() else 0})]
allDaysExt = alldays[:,dt.f[:].extend({"AnzahlFallLetzte7Tage": 0})]

for i, lk in enumerate(alldays[:,'Landkreis'].to_list()[0]):
    if lk in last7daysDict:
        allDaysExt[i,"AnzahlFallLetzte7Tage"] = last7daysDict[lk]
    print("lk",lk)

print(last7days)
print(alldays)

fig = go.Figure(data=[go.Table(
    header=dict(values=allDaysExt.keys(),
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