# -*- coding: utf-8 -*-
"""
Created on Wed Nov  7 10:21:31 2018

@author: Burhanuddin
"""

import json as js
import requests
import time
from config import api_key
import pandas as pd
from init import types, lat, lng, radius



def getData(lat, lng, radius, type):
    pagetoken = None
    frames = []
    numberResults= 0
    while True:
        url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={lat},{lng}&radius={radius}&type={type}&key={api_key}{pagetoken}".format(lat = str(lat), lng = str(lng), radius = radius, type = type, api_key = api_key, pagetoken = "&pagetoken="+pagetoken if pagetoken else "")
        response = requests.get(url)
        res = js.loads(response.text)
        results = res["results"]
        frame = pd.DataFrame(results)
        frames.append(frame)
        numberResults += len(res["results"])
        pagetoken = res.get("next_page_token",None)
        time.sleep(5)
        if not pagetoken:
            dataframe = frames[0]
            print("We have {n} results".format(n = numberResults))
            if len(frames) > 1:
                for i in range(1, len(frames),1):
                    dataframe = pd.concat([dataframe, frames[i]], axis=0, ignore_index = True)
            return dataframe
        
def clearData(dataFrame):
    try:
        cleanFrame = dataFrame.drop(['geometry', 'icon', 'id', 'opening_hours', 'photos', 'plus_code', 'reference', 'scope'],axis=1)
        columnLen = len(cleanFrame.columns)
        cleanFrame.insert(columnLen, 'Phone Number', 0)
        return cleanFrame
    except ValueError:
        return dataFrame
    
    
def saveCsv(dataFrame, lat, lng):
    title = "{lat}_{lng}_.csv".format(lat=str(lat), lng=str(lng))
    dataFrame.to_csv(title, encoding='utf-8', index=False)


def detailSearch(placeID):
    site = "https://maps.googleapis.com/maps/api/place/details/json?key={api_key}&placeid={placeID}&fields=formatted_phone_number".format(api_key = api_key, placeID = placeID)
    response = requests.get(site)
    res = js.loads(response.text)
    phoneNumber = res["result"]["formatted_phone_number"]
    print(phoneNumber)
    return(phoneNumber)

def addNumbers(dataFrame):
    for index, row in dataFrame.iterrows():
        try:
            dataFrame['Phone Number'][index] = detailSearch(row['place_id'])
        except KeyError:
            dataFrame['Phone Number'][index] = 0
#    try:
#        dataFrame['Phone Number'][index] = dataFrame.apply(lambda row: detailSearch(row['place_id']), axis = 1)
#    except KeyError:
#        dataFrame['Phone Number'][index] = 0
    return dataFrame
        

def fullProcess(lat, lng, radius, type):
    fullData = getData(lat, lng, radius, type)
    cleanData = clearData(fullData)
    contactData = addNumbers(cleanData)
    return contactData

def leadSearch(lat, lng, radius):
    type_frames = []
    for type in types:
        frame = fullProcess(lat, lng, radius, type)
        type_frames.append(frame)
    data_frame = type_frames[0]
    for i in range(1, len(type_frames),1):
        data_frame = pd.concat([data_frame, type_frames[i]], axis=0, ignore_index=True)
    return data_frame
    
def completeSearch(lat, lng, radius):
    leads = leadSearch(lat, lng, radius)
    saveCsv(leads, lat, lng)
    return

if __name__ == "__main__":
    completeSearch(lat, lng, radius)