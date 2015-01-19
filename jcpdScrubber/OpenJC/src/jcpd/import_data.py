'''
Created on Feb 4, 2014

@author: Gopanand S
'''
import os
import csv
import json
import urllib
import pickle
import logging
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
    try:
        street = urllib.quote(street)
        city = urllib.quote(city)
        resp_obj = urllib.urlopen(goog_map_url.format(street=street, city=city, state=state))
        res = json.load(resp_obj)
        info=[]
        if res.get('status') == 'OK':
            for loc in res.get('results'):
                info.append([loc.get('geometry').get('location'), loc.get('formatted_address')])
            if len(info) > 1:
                logging.warn("Got more than one results for " + street + ", " + city + ", " + state)
            return info
        else:
            logging.warn("Got a not-OK (" + res.get('status') + ") result for " + " $$$ " + street + ", " + city + ", " + state)
    except Exception as ex:
        print ex
        logging.error("Unable to locate for " + street + ", " + city + ", " + state)
        

day_map = {1:'Sunday', 2:'Monday', 3:'Tuesday', 4:'Wednesday', 5:'Thursday', 6:'Friday', 7:'Saturday'}

def transform_data(data):
    for row in data:
        row['Day'] = day_map[int(row['TR WEEKDAY'])]
        ts = datetime.datetime.strptime(row['TR'], "%m/%d/%Y %H:%M:%S")
        row['TimeStamp'] = ts
        row['TimeOfDay'] = map_time(ts.time().hour)
        row['geolocation'] = geo_locate(row['Street'])
    return data

if __name__ == '__main__':
    data = retrieve_csv()
    #data = transform_data(data)
    
    fname = os.getcwd() + '/total.txt'
    
    with open(fname, mode='w') as op_file:
        pickle.dump(data, op_file)
    logging.info("Result set stored in " + fname)
    
    
    