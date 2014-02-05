'''
Created on Feb 4, 2014

@author: Gopanand S
'''

import csv
import urllib
import datetime

URL = "https://data.openjerseycity.org/storage/f/2013-12-26T19%3A36%3A22.594Z/jcpd-calls-for-service-november-2013.csv"

def retrieve_csv():
    fileobj = urllib.urlopen(URL)
    listOfDicts = list(csv.DictReader(fileobj))
    return listOfDicts

def map_time(hr):
    if hr>=3 and hr<6:
        return 'Wee Hours'
    elif hr>=6 and hr<12:
        return 'Morning'
    elif hr>=12 and hr<17:
        return 'Afternoon'
    elif hr>=17 and hr<21:
        return 'Evening'
    elif hr>=21 or hr<3:
        return 'OverNight'
    
goog_map_url = 'http://maps.googleapis.com/maps/api/geocode/json?address={street}+{city}+{state}&sensor=false'

def geo_locate(street, city='Jersey City', state='NJ'):
    print goog_map_url.format(street=street, city=city, state=state)

day_map = {1:'Sunday', 2:'Monday', 3:'Tuesday', 4:'Wednesday', 5:'Thursday', 6:'Friday', 7:'Saturday'}

def transform_data(data):
    for row in data:
        row['Day'] = day_map[int(row['TR WEEKDAY'])]
        ts = datetime.datetime.strptime(row['TR'], "%m/%d/%Y %H:%M:%S")
        row['TimeStamp'] = ts
        row['TimeOfDay'] = map_time(ts.time().hour)
        geo_locate(row['Street'])
    return data

if __name__ == '__main__':
    data = retrieve_csv()
    transformed_data = transform_data(data)
    print transformed_data[-1]
    
    