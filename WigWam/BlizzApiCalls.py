import os
import time
from warnings import simplefilter
simplefilter("ignore")
import glob
import codecs
import pandas as pd
import numpy as np
import pprint
import json
import requests
from sklearn.externals import joblib
import infoRefinement as ir


def getApiToken():
    data = {"grant_type":"client_credentials"}
    print(data)
    apiclient = os.environ.get("WOWAPICLIENT")
    apisecret = os.environ.get("WOWAPISECRET")
    #body = str.encode(json.dumps(data))

    BlizzardTokenUrl = "https://eu.battle.net/oauth/token"
    resp = requests.post(BlizzardTokenUrl, data=data, auth=(apiclient, apisecret))

    rdata = resp.json()
    print(resp.text)

    if "error" in rdata.keys(): return str(rdata["error"])
    else:
        tokDict = {"Token":rdata["access_token"],"Type":rdata["token_type"], "Expires":rdata["expires_in"]}

    joblib.dump(tokDict, "token.pkl")

    return "token requested"

def testpickle():
    tokDict = {"Token": "EUKNVle4Mc8LqjFEvRAqn186fZIdp2S094", "Type": "bla", "Expires": "bla"}
    joblib.dump(tokDict, "token.pkl")

def getStatistic(char,realm,region):
    BlizzApiUrl = "https://eu.api.blizzard.com/wow/character/" + realm + "/" + char
    tokDict = joblib.load("token.pkl")
    apiKey = tokDict["Token"]

    data = {"fields": "statistics"}
    headers = {"Content-Type":"application/json", "Authorization": "Bearer "+ apiKey}
    resp = requests.get(BlizzApiUrl, params=data, headers=headers)

    if resp.status_code == 401:
        print(getApiToken())
        return getStatistic(char, realm, region)
    
    else:    
        return resp.json()

def getRatedPvP(char,realm,region):
    BlizzApiUrl = "https://eu.api.blizzard.com/wow/character/" + realm + "/" + char
    tokDict = joblib.load("token.pkl")
    apiKey = tokDict["Token"]

    data = {"fields": "pvp"}
    headers = {"Content-Type":"application/json", "Authorization": "Bearer "+ apiKey}
    resp = requests.get(BlizzApiUrl, params=data, headers=headers)

    if resp.status_code == 401:
        print(getApiToken())
        return getRatedPvP(char, realm, region)
    
    else:    
        return resp.json()

def getAudit(char,realm,region):
    BlizzApiUrl = "https://eu.api.blizzard.com/wow/character/" + realm + "/" + char
    tokDict = joblib.load("token.pkl")
    apiKey = tokDict["Token"]

    data = {"fields": "audit"}
    headers = {"Content-Type":"application/json", "Authorization": "Bearer "+ apiKey}
    resp = requests.get(BlizzApiUrl, params=data, headers=headers)

    if resp.status_code == 401:
        print(getApiToken())
        return getAudit(char, realm, region)
    
    else:    
        return resp.json()

def getQuests(char,realm,region):
    BlizzApiUrl = "https://eu.api.blizzard.com/wow/character/" + realm + "/" + char
    tokDict = joblib.load("token.pkl")
    apiKey = tokDict["Token"]

    data = {"fields": "quests"}
    headers = {"Content-Type":"application/json", "Authorization": "Bearer "+ apiKey}
    resp = requests.get(BlizzApiUrl, params=data, headers=headers)

    if resp.status_code == 401:
        print(getApiToken())
        return getQuests(char, realm, region)
    
    else:    
        return resp.json()

def getMounts(char,realm,region):
    BlizzApiUrl = "https://eu.api.blizzard.com/wow/character/" + realm + "/" + char
    tokDict = joblib.load("token.pkl")
    apiKey = tokDict["Token"]

    data = {"fields": "mounts"}
    headers = {"Content-Type":"application/json", "Authorization": "Bearer "+ apiKey}
    resp = requests.get(BlizzApiUrl, params=data, headers=headers)

    if resp.status_code == 401:
        print(getApiToken())
        return getMounts(char, realm, region)
    
    else:    
        return resp.json()

def getHonorableKills(char, realm, region):
    BlizzApiUrl = "https://eu.api.blizzard.com/wow/character/" + realm + "/" + char
    tokDict = joblib.load("token.pkl")
    apiKey = tokDict["Token"]

    data = {"fields": "statistics"}
    headers = {"Content-Type":"application/json", "Authorization": "Bearer "+ apiKey}
    resp = requests.get(BlizzApiUrl, params=data, headers=headers)

    if resp.status_code == 401:
        print(getApiToken())
        return getHonorableKills(char, realm, region)
    
    else:
        rdict = resp.json()
        return rdict["statistics"]["subCategories"][2]["subCategories"][1]

def getKillingBlows(char, realm, region):
    BlizzApiUrl = "https://eu.api.blizzard.com/wow/character/" + realm + "/" + char
    tokDict = joblib.load("token.pkl")
    apiKey = tokDict["Token"]

    data = {"fields": "statistics"}
    headers = {"Content-Type":"application/json", "Authorization": "Bearer "+ apiKey}
    resp = requests.get(BlizzApiUrl, params=data, headers=headers)

    if resp.status_code == 401:
        print(getApiToken())
        return getKillingBlows(char, realm, region)
    
    else:
        rdict = resp.json()
        return rdict["statistics"]["subCategories"][2]["subCategories"][2]


def getDeaths(char, realm, region):
    BlizzApiUrl = "https://eu.api.blizzard.com/wow/character/" + realm + "/" + char
    tokDict = joblib.load("token.pkl")
    apiKey = tokDict["Token"]

    data = {"fields": "statistics"}
    headers = {"Content-Type":"application/json", "Authorization": "Bearer "+ apiKey}
    resp = requests.get(BlizzApiUrl, params=data, headers=headers)

    if resp.status_code == 401:
        print(getApiToken())
        return getDeaths(char, realm, region)
    
    else:
        rdict = resp.json()
        return rdict["statistics"]["subCategories"][3]


def getPvP(char, realm, region):
    BlizzApiUrl = "https://eu.api.blizzard.com/wow/character/" + realm + "/" + char
    tokDict = joblib.load("token.pkl")
    apiKey = tokDict["Token"]

    data = {"fields": "statistics"}
    headers = {"Content-Type":"application/json", "Authorization": "Bearer "+ apiKey}
    resp = requests.get(BlizzApiUrl, params=data, headers=headers)

    if resp.status_code == 401:
        print(getApiToken())
        return getPvP(char, realm, region)
    
    else:
        rdict = resp.json()
        return rdict["statistics"]["subCategories"][9]

def getBGs(char, realm, region):
    BlizzApiUrl = "https://eu.api.blizzard.com/wow/character/" + realm + "/" + char
    tokDict = joblib.load("token.pkl")
    apiKey = tokDict["Token"]

    data = {"fields": "statistics"}
    headers = {"Content-Type":"application/json", "Authorization": "Bearer "+ apiKey}
    resp = requests.get(BlizzApiUrl, params=data, headers=headers)

    if resp.status_code == 401:
        print(getApiToken())
        return getBGs(char, realm, region)
    
    else:
        rdict = resp.json()
        rdict2 = rdict["statistics"]["subCategories"][9]["subCategories"][1]
        del rdict["statistics"]
        return (rdict, rdict2)

def checkBGOrder(dict):
    checkList = []
    baseList = joblib.load("bgOrder.pkl")
    for d in dict["statistics"]:
        checkList.append(d["name"])
    match = True

    for index, entry in enumerate(checkList):
        if entry == baseList[index]:
            match = True
        else:
            match = False
            break

    if match == True: return True
    else: return False



def getRaces():
    BlizzApiUrl = "https://eu.api.blizzard.com/wow/data/character/races"
    tokDict = joblib.load("token.pkl")
    apiKey = tokDict["Token"]

    headers = {"Content-Type":"application/json", "Authorization": "Bearer "+ apiKey}
    resp = requests.get(BlizzApiUrl, headers=headers)

    if resp.status_code == 401:
        print(getApiToken())
        return getRaces()
    
    else:
        rdict = resp.json()
        return rdict


def getClasses():
    BlizzApiUrl = "https://eu.api.blizzard.com/wow/data/character/classes"
    tokDict = joblib.load("token.pkl")
    apiKey = tokDict["Token"]

    headers = {"Content-Type":"application/json", "Authorization": "Bearer "+ apiKey}
    resp = requests.get(BlizzApiUrl, headers=headers)

    if resp.status_code == 401:
        print(getApiToken())
        return getClasses()
    
    else:
        rdict = resp.json()
        return rdict



if __name__ == "__main__":
    #print(getApiToken())
    #testpickle()
    #print(json.dumps(getBGs("christhina","azshara","eu"),sort_keys=True, indent=4))
    testp = getBGs("christhina","azshara","eu")
    cardsdict = ir.refineBGInfo(testp[1])
    ir.createMoreBGCharts("christhina", "azshara", cardsdict)
    #check = checkBGOrder(testp)
    #print(check)
    #testdict = ir.refineBGInfo(testp)
    #print(testdict)
    #print(json.dumps(getClasses(), sort_keys=True, indent=4))