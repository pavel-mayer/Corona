#!/usr/bin/env python3.6

# Quick hack to browse RKI-NPGEO-Data, Pavel Mayer 2020,
# License: Use freely at your own risk.

import time
from datetime import timedelta
from datetime import timezone
from datetime import datetime, date
import pytz
import calendar

day0 = time.strptime("22.2.2020", "%d.%m.%Y") # struct_time
day0t = time.mktime(day0) # float timestamp since epoch
day0d = datetime.fromtimestamp(day0t) # datetime.datetime
#day0d = pytz.utc.localize(datetime.fromtimestamp(day0t)) # datetime.datetime
#day0d.replace(tzinfo=timezone.utc)

weekDay = ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"]

def datetimeFromStampStr(s) -> str:
    dt = datetime.fromtimestamp(int(s) / 1000)
    return dt

def dateStrFromStampStr(s) -> str:
    t = time.gmtime(int(s)/1000) #time.struct_time
    return "{}.{}.{}".format(t.tm_mday, t.tm_mon,t.tm_year)

def dateTimeStrFromStampStr(s) -> str:
    t = time.gmtime(int(s)/1000) #time.struct_time
    return "{0}, {1:02d}.{2:02d}.{3} {4:02d}:{5:02d}".format(weekDay[t.tm_wday], t.tm_mday, t.tm_mon,t.tm_year,t.tm_hour, t.tm_min)

def dateTimeStrFromAnyStampStr(s) -> str:
    try:
        return dateTimeStrFromStampStr(s)
    except ValueError:
        t = time.strptime(s, "%Y/%m/%d %H:%M:%S")
        return "{0}, {1:02d}.{2:02d}.{3} {4:02d}:{5:02d}".format(weekDay[t.tm_wday], t.tm_mday, t.tm_mon,t.tm_year,t.tm_hour, t.tm_min)

def stampFromDateStr(s):
    t = time.strptime(s, "%Y/%m/%d %H:%M:%S")
    return calendar.timegm(t)

def dateTimeStrFromTime(tt) -> str:
    t = time.gmtime(tt) #time.struct_time
    return "{0}, {1:02d}.{2:02d}.{3} {4:02d}:{5:02d}".format(weekDay[t.tm_wday], t.tm_mday, t.tm_mon,t.tm_year,t.tm_hour, t.tm_min)

def dateFromDay(day) -> datetime:
    return day0d + timedelta(day)

## 1965-02-23
def dateStrFromDay(day) -> str:
    result = dateFromDay(day)
    return "{}.{}.{}".format(result.day, result.month, result.year)

## 1965-02-23
def dateStrYMDFromDay(day) -> str:
    result = dateFromDay(day)
    return "{0:04d}-{1:02d}-{2:02d}".format(result.year, result.month, result.day)

## 23.3.
def dateStrDMFromDay(day) -> str:
    result = dateFromDay(day)
    return "{}.{}.".format(result.day,result.month)

## Di, 23.3.
def dateStrWDMFromDay(day) -> str:
    result = dateFromDay(day)
    return "{}, {}.{}".format(weekDay[result.weekday()],result.day, result.month)

## Di, 23.3.1965
def dateStrWDMYFromDay(day) -> str:
    result = dateFromDay(day)
    return "{}, {}.{}.{}".format(weekDay[result.weekday()],result.day, result.month, result.year)

def dayFromDate(date) -> int:
#    print("date",date)
#    print("day0d",day0d)
    delta = date - day0d
    return int(delta.days)

def dayFromTime(t) -> int:
    return dayFromDate(datetime.fromtimestamp(time.mktime(t)))

def dayFromStampStr(s) -> int:
    d = datetime.fromtimestamp(int(s) / 1000)
    delta = d - day0d
    return delta.days

# Parse date string like "2020/11/25 00:00:00"
def datetimeFromDateStr(ds):
    st = time.strptime(ds, "%Y/%m/%d %H:%M:%S")
    stf = time.mktime(st)
    sdt = datetime.fromtimestamp(stf)
    return sdt

def dayFromAnyStampStr(s) -> int:
    try:
        return dayFromStampStr(s)
    except ValueError:
        d = datetimeFromDateStr(s)
        delta = d - day0d
        return delta.days

# Parse date string like "19.05.2020, 00:00 Uhr"
def datetimeFromDatenstand(ds):
    st = time.strptime(ds, "%d.%m.%Y, %H:%M Uhr")
    stf = time.mktime(st)
    sdt = datetime.fromtimestamp(stf)
    return sdt

# Parse date string like "27.03.2020 00:00"
def datetimeFromDatenstandAny(ds):
    st = None
    try:
        st = time.strptime(ds, "%Y-%m-%d")
    except ValueError:
        ds = ds.replace(",","")
        ds = ds.replace(" Uhr","")
        st = time.strptime(ds, "%d.%m.%Y %H:%M")
    stf = time.mktime(st)
    sdt = datetime.fromtimestamp(stf)
    return sdt

def dayFromDatenstand(ds):
    return dayFromDate(datetimeFromDatenstand(ds))

def todayDay():
    return dayFromTime(time.localtime())

#firstDumpDay = time.strptime("27.3.2020", "%d.%m.%Y")
#firstDumpDay = dayFromTime(time.strptime("22.2.2020", "%d.%m.%Y"))

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