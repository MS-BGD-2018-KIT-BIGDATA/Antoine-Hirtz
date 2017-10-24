#!/usr/bin/python

import requests
import json
import numpy as np
import pandas as pd

def getDistanceBetween(origins, destinations):
    url_api = "https://maps.googleapis.com/maps/api/distancematrix/json?"
    url_googleapis= ("".join([url_api,
                              "&origins=", origins.strip(),
                              "&destinations=", destinations.strip(),
                              "&key=", "AIzaSyAChvRXVRStyty2Qrm2_RYvxtoNlm7VO3s"]))
    res = requests.get(url_googleapis)

    if res.ok:
        json_res = json.loads(res.text)
        return json_res['rows'][0]['elements'][0]['distance']['text']
    else:
        return None

def getListOfCities():
    

list_cities =

dist1 = getDistanceBetween(origins = "Paris,    France", destinations = "Marseille,France")
dist2 = getDistanceBetween(origins = "Paris,France", destinations = "Rennes,France")
